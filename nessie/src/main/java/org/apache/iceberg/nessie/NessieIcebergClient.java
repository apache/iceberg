/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.nessie;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.relocated.com.google.common.base.Suppliers;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.view.ViewMetadata;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.GetContentBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.OnReferenceBuilder;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergContent;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableIcebergView;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NessieIcebergClient implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(NessieIcebergClient.class);

  private final NessieApiV1 api;
  private final Supplier<UpdateableReference> reference;
  private final Map<String, String> catalogOptions;

  public NessieIcebergClient(
      NessieApiV1 api,
      String requestedRef,
      String requestedHash,
      Map<String, String> catalogOptions) {
    this.api = api;
    this.catalogOptions = catalogOptions;
    this.reference = Suppliers.memoize(() -> loadReference(requestedRef, requestedHash));
  }

  public NessieApiV1 getApi() {
    return api;
  }

  UpdateableReference getRef() {
    return reference.get();
  }

  public Reference getReference() {
    return reference.get().getReference();
  }

  public void refresh() throws NessieNotFoundException {
    getRef().refresh(api);
  }

  public NessieIcebergClient withReference(String requestedRef, String hash) {
    if (null == requestedRef
        || (getRef().getReference().getName().equals(requestedRef)
            && getRef().getHash().equals(hash))) {
      return this;
    }
    return new NessieIcebergClient(getApi(), requestedRef, hash, catalogOptions);
  }

  private UpdateableReference loadReference(String requestedRef, String hash) {
    try {
      Reference ref =
          requestedRef == null
              ? api.getDefaultBranch()
              : api.getReference().refName(requestedRef).get();
      if (hash != null) {
        if (ref instanceof Branch) {
          ref = Branch.of(ref.getName(), hash);
        } else {
          ref = Tag.of(ref.getName(), hash);
        }
      }
      return new UpdateableReference(ref, hash != null);
    } catch (NessieNotFoundException ex) {
      if (requestedRef != null) {
        throw new IllegalArgumentException(
            String.format("Nessie ref '%s' does not exist", requestedRef), ex);
      }

      throw new IllegalArgumentException(
          String.format(
              "Nessie does not have an existing default branch. "
                  + "Either configure an alternative ref via '%s' or create the default branch on the server.",
              NessieConfigConstants.CONF_NESSIE_REF),
          ex);
    }
  }

  public List<TableIdentifier> listTables(Namespace namespace) {
    return listContents(namespace, Content.Type.ICEBERG_TABLE);
  }

  public List<TableIdentifier> listViews(Namespace namespace) {
    return listContents(namespace, Content.Type.ICEBERG_VIEW);
  }

  /** Lists Iceberg table or view from the given namespace */
  private List<TableIdentifier> listContents(Namespace namespace, Content.Type type) {
    try {
      return withReference(api.getEntries()).get().getEntries().stream()
          .filter(namespacePredicate(namespace))
          .filter(e -> type.equals(e.getType()))
          .map(this::toIdentifier)
          .collect(Collectors.toList());
    } catch (NessieNotFoundException ex) {
      throw new NoSuchNamespaceException(
          ex,
          "Unable to list %ss due to missing ref '%s'",
          NessieUtil.contentTypeString(type).toLowerCase(),
          getRef().getName());
    }
  }

  private Predicate<EntriesResponse.Entry> namespacePredicate(Namespace ns) {
    if (ns == null) {
      return e -> true;
    }

    final List<String> namespace = Arrays.asList(ns.levels());
    return e -> {
      List<String> names = e.getName().getElements();

      if (names.size() <= namespace.size()) {
        return false;
      }

      return namespace.equals(names.subList(0, namespace.size()));
    };
  }

  private TableIdentifier toIdentifier(EntriesResponse.Entry entry) {
    List<String> elements = entry.getName().getElements();
    return TableIdentifier.of(elements.toArray(new String[elements.size()]));
  }

  public IcebergContent fetchContent(TableIdentifier tableIdentifier) {
    try {
      ContentKey key = NessieUtil.toKey(tableIdentifier);
      Content content = withReference(api.getContent().key(key)).get().get(key);
      return content != null ? content.unwrap(IcebergContent.class).orElse(null) : null;
    } catch (NessieNotFoundException e) {
      return null;
    }
  }

  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    getRef().checkMutable();
    if (namespace.isEmpty()) {
      throw new IllegalArgumentException("Creating empty namespaces is not supported");
    }
    ContentKey key = ContentKey.of(namespace.levels());
    org.projectnessie.model.Namespace content =
        org.projectnessie.model.Namespace.of(key.getElements(), metadata);
    try {
      Content existing = api.getContent().reference(getReference()).key(key).get().get(key);
      if (existing != null) {
        throw namespaceAlreadyExists(key, existing, null);
      }
      try {
        commitRetry("create namespace " + key, Operation.Put.of(key, content));
      } catch (NessieReferenceConflictException e) {
        Optional<Conflict> conflict =
            NessieUtil.extractSingleConflict(
                e,
                EnumSet.of(
                    Conflict.ConflictType.KEY_EXISTS, Conflict.ConflictType.NAMESPACE_ABSENT));
        if (conflict.isPresent()) {
          switch (conflict.get().conflictType()) {
            case KEY_EXISTS:
              Content conflicting = withReference(api.getContent()).key(key).get().get(key);
              throw namespaceAlreadyExists(key, conflicting, e);
            case NAMESPACE_ABSENT:
              throw new NoSuchNamespaceException(
                  e,
                  "Cannot create namespace '%s': parent namespace '%s' does not exist",
                  namespace,
                  conflict.get().key());
          }
        }
        throw new RuntimeException(
            String.format("Cannot create namespace '%s': %s", namespace, e.getMessage()));
      }
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(
          String.format(
              "Cannot create namespace '%s': ref '%s' is no longer valid.",
              namespace, getRef().getName()),
          e);
    } catch (BaseNessieClientServerException e) {
      throw new RuntimeException(
          String.format("Cannot create namespace '%s': %s", namespace, e.getMessage()), e);
    }
  }

  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    try {
      String filter = "entry.contentType == 'NAMESPACE' && ";
      if (namespace.isEmpty()) {
        filter += "size(entry.keyElements) == 1";
      } else {
        org.projectnessie.model.Namespace root =
            org.projectnessie.model.Namespace.of(namespace.levels());
        filter +=
            String.format(
                "size(entry.keyElements) == %d && entry.encodedKey.startsWith('%s.')",
                root.getElementCount() + 1, root.name());
      }
      List<ContentKey> entries =
          withReference(api.getEntries()).filter(filter).stream()
              .map(EntriesResponse.Entry::getName)
              .collect(Collectors.toList());
      if (entries.isEmpty()) {
        return Collections.emptyList();
      }
      GetContentBuilder getContent = withReference(api.getContent());
      entries.forEach(getContent::key);
      return getContent.get().values().stream()
          .map(v -> v.unwrap(org.projectnessie.model.Namespace.class))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .map(v -> Namespace.of(v.getElements().toArray(new String[0])))
          .collect(Collectors.toList());
    } catch (NessieNotFoundException e) {
      if (namespace.isEmpty()) {
        throw new NoSuchNamespaceException(
            e,
            "Cannot list top-level namespaces: ref '%s' is no longer valid.",
            getRef().getName());
      }
      throw new NoSuchNamespaceException(
          e,
          "Cannot list child namespaces from '%s': ref '%s' is no longer valid.",
          namespace,
          getRef().getName());
    }
  }

  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    getRef().checkMutable();
    ContentKey key = ContentKey.of(namespace.levels());
    try {
      Map<ContentKey, Content> contentMap =
          api.getContent().reference(getReference()).key(key).get();
      Content existing = contentMap.get(key);
      if (existing != null && !existing.getType().equals(Content.Type.NAMESPACE)) {
        throw new NoSuchNamespaceException(
            "Content object with name '%s' is not a namespace.", namespace);
      }
      try {
        commitRetry("drop namespace " + key, Operation.Delete.of(key));
        return true;
      } catch (NessieReferenceConflictException e) {
        Optional<Conflict> conflict =
            NessieUtil.extractSingleConflict(
                e,
                EnumSet.of(
                    Conflict.ConflictType.KEY_DOES_NOT_EXIST,
                    Conflict.ConflictType.NAMESPACE_NOT_EMPTY));
        if (conflict.isPresent()) {
          Conflict.ConflictType conflictType = conflict.get().conflictType();
          switch (conflictType) {
            case KEY_DOES_NOT_EXIST:
              return false;
            case NAMESPACE_NOT_EMPTY:
              throw new NamespaceNotEmptyException(e, "Namespace '%s' is not empty.", namespace);
          }
        }
        throw new RuntimeException(
            String.format("Cannot drop namespace '%s': %s", namespace, e.getMessage()));
      }
    } catch (NessieNotFoundException e) {
      LOG.error(
          "Cannot drop namespace '{}': ref '{}' is no longer valid.",
          namespace,
          getRef().getName(),
          e);
    } catch (BaseNessieClientServerException e) {
      throw new RuntimeException(
          String.format("Cannot drop namespace '%s': %s", namespace, e.getMessage()), e);
    }
    return false;
  }

  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    ContentKey key = ContentKey.of(namespace.levels());
    try {
      Map<ContentKey, Content> contentMap = withReference(api.getContent()).key(key).get();
      return unwrapNamespace(contentMap.get(key))
          .orElseThrow(
              () -> new NoSuchNamespaceException("Namespace does not exist: %s", namespace))
          .getProperties();
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(
          String.format(
              "Cannot load namespace '%s': ref '%s' is no longer valid.",
              namespace, getRef().getName()),
          e);
    }
  }

  public boolean setProperties(Namespace namespace, Map<String, String> properties) {
    return updateProperties(namespace, props -> props.putAll(properties));
  }

  public boolean removeProperties(Namespace namespace, Set<String> properties) {
    return updateProperties(namespace, props -> props.keySet().removeAll(properties));
  }

  private boolean updateProperties(Namespace namespace, Consumer<Map<String, String>> action) {
    getRef().checkMutable();
    ContentKey key = ContentKey.of(namespace.levels());
    try {
      commitRetry(
          "update namespace " + key,
          true,
          commitBuilder -> {
            org.projectnessie.model.Namespace oldNamespace =
                unwrapNamespace(api.getContent().reference(getReference()).key(key).get().get(key))
                    .orElseThrow(
                        () -> new NessieContentNotFoundException(key, getReference().getName()));
            Map<String, String> newProperties = Maps.newHashMap(oldNamespace.getProperties());
            action.accept(newProperties);
            org.projectnessie.model.Namespace updatedNamespace =
                org.projectnessie.model.Namespace.builder()
                    .from(oldNamespace)
                    .properties(newProperties)
                    .build();
            commitBuilder.operation(Operation.Put.of(key, updatedNamespace));
            return commitBuilder;
          });
      // always successful, otherwise an exception is thrown
      return true;
    } catch (NessieReferenceConflictException e) {
      Optional<Conflict> conflict =
          NessieUtil.extractSingleConflict(e, EnumSet.of(Conflict.ConflictType.KEY_DOES_NOT_EXIST));
      if (conflict.isPresent()
          && conflict.get().conflictType() == Conflict.ConflictType.KEY_DOES_NOT_EXIST) {
        throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);
      }
      throw new RuntimeException(
          String.format(
              "Cannot update properties on namespace '%s': %s", namespace, e.getMessage()));
    } catch (NessieContentNotFoundException e) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    } catch (NessieReferenceNotFoundException e) {
      throw new RuntimeException(
          String.format(
              "Cannot update properties on namespace '%s': ref '%s' is no longer valid.",
              namespace, getRef().getName()),
          e);
    } catch (BaseNessieClientServerException e) {
      throw new RuntimeException(
          String.format("Cannot update namespace '%s': %s", namespace, e.getMessage()), e);
    }
  }

  public void renameTable(TableIdentifier from, TableIdentifier to) {
    renameContent(from, to, Content.Type.ICEBERG_TABLE);
  }

  public void renameView(TableIdentifier from, TableIdentifier to) {
    renameContent(from, to, Content.Type.ICEBERG_VIEW);
  }

  private void renameContent(TableIdentifier from, TableIdentifier to, Content.Type type) {
    getRef().checkMutable();

    IcebergContent existingFromContent = fetchContent(from);
    validateFromContentForRename(from, type, existingFromContent);

    IcebergContent existingToContent = fetchContent(to);
    validateToContentForRename(from, to, existingToContent);

    String contentType = NessieUtil.contentTypeString(type).toLowerCase();
    try {
      commitRetry(
          String.format("Iceberg rename %s from '%s' to '%s'", contentType, from, to),
          Operation.Delete.of(NessieUtil.toKey(from)),
          Operation.Put.of(NessieUtil.toKey(to), existingFromContent));
    } catch (NessieNotFoundException e) {
      // important note: the NotFoundException refers to the ref only. If a table was not found it
      // would imply that the
      // another commit has deleted the table from underneath us. This would arise as a Conflict
      // exception as opposed to
      // a not found exception. This is analogous to a merge conflict in git when a table has been
      // changed by one user
      // and removed by another.
      throw new RuntimeException(
          String.format(
              "Cannot rename %s '%s' to '%s': ref '%s' no longer exists.",
              contentType, from, to, getRef().getName()),
          e);
    } catch (BaseNessieClientServerException e) {
      CommitFailedException commitFailedException =
          new CommitFailedException(
              e,
              "Cannot rename %s '%s' to '%s': the current reference is not up to date.",
              contentType,
              from,
              to);
      Optional<RuntimeException> exception = Optional.empty();
      if (e instanceof NessieConflictException) {
        exception = NessieUtil.handleExceptionsForCommits(e, getRef().getName(), type);
      }
      throw exception.orElse(commitFailedException);
    } catch (HttpClientException ex) {
      // Intentionally catch all nessie-client-exceptions here and not just the "timeout" variant
      // to catch all kinds of network errors (e.g. connection reset). Network code implementation
      // details and all kinds of network devices can induce unexpected behavior. So better be
      // safe than sorry.
      throw new CommitStateUnknownException(ex);
    }
    // Intentionally just "throw through" Nessie's HttpClientException here and do not "special
    // case"
    // just the "timeout" variant to propagate all kinds of network errors (e.g. connection reset).
    // Network code implementation details and all kinds of network devices can induce unexpected
    // behavior. So better be safe than sorry.
  }

  private static void validateToContentForRename(
      TableIdentifier from, TableIdentifier to, IcebergContent existingToContent) {
    if (existingToContent != null) {
      if (existingToContent.getType() == Content.Type.ICEBERG_VIEW) {
        throw new AlreadyExistsException("Cannot rename %s to %s. View already exists", from, to);
      } else if (existingToContent.getType() == Content.Type.ICEBERG_TABLE) {
        throw new AlreadyExistsException("Cannot rename %s to %s. Table already exists", from, to);
      } else {
        throw new AlreadyExistsException(
            "Cannot rename %s to %s. Another content of type %s with same name already exists",
            from, to, existingToContent.getType());
      }
    }
  }

  private static void validateFromContentForRename(
      TableIdentifier from, Content.Type type, IcebergContent existingFromContent) {
    if (existingFromContent == null) {
      if (type == Content.Type.ICEBERG_VIEW) {
        throw new NoSuchViewException("View does not exist: %s", from);
      } else if (type == Content.Type.ICEBERG_TABLE) {
        throw new NoSuchTableException("Table does not exist: %s", from);
      } else {
        throw new RuntimeException("Cannot perform rename for content type: " + type);
      }
    } else if (existingFromContent.getType() != type) {
      throw new RuntimeException(
          String.format("content type of from identifier %s should be of %s", from, type));
    }
  }

  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    return dropContent(identifier, purge, Content.Type.ICEBERG_TABLE);
  }

  public boolean dropView(TableIdentifier identifier, boolean purge) {
    return dropContent(identifier, purge, Content.Type.ICEBERG_VIEW);
  }

  private boolean dropContent(TableIdentifier identifier, boolean purge, Content.Type type) {
    getRef().checkMutable();

    IcebergContent existingContent = fetchContent(identifier);
    if (existingContent == null) {
      return false;
    }

    if (existingContent.getType() != type) {
      throw new RuntimeException(
          String.format(
              "Cannot drop %s: not matching with the type `%s`",
              identifier, NessieUtil.contentTypeString(type)));
    }

    String contentType = NessieUtil.contentTypeString(type).toLowerCase();

    if (purge) {
      LOG.info(
          "Purging data for {} {} was set to true but is ignored",
          contentType,
          identifier.toString());
    }

    // We try to drop the content. Simple retry after ref update.
    try {
      commitRetry(
          String.format("Iceberg delete table %s", identifier),
          Operation.Delete.of(NessieUtil.toKey(identifier)));
      return true;
    } catch (NessieConflictException e) {
      LOG.error(
          "Cannot drop {}: failed after retry (update ref '{}' and retry)",
          contentType,
          getRef().getName(),
          e);
    } catch (NessieNotFoundException e) {
      LOG.error("Cannot drop {}: ref '{}' is no longer valid.", contentType, getRef().getName(), e);
    } catch (BaseNessieClientServerException e) {
      LOG.error("Cannot drop {}: unknown error", contentType, e);
    }
    return false;
  }

  /** @deprecated will be removed after 1.5.0 */
  @Deprecated
  public void commitTable(
      TableMetadata base,
      TableMetadata metadata,
      String newMetadataLocation,
      IcebergTable expectedContent,
      ContentKey key)
      throws NessieConflictException, NessieNotFoundException {
    String contentId = expectedContent == null ? null : expectedContent.getId();
    commitTable(base, metadata, newMetadataLocation, contentId, key);
  }

  public void commitTable(
      TableMetadata base,
      TableMetadata metadata,
      String newMetadataLocation,
      String contentId,
      ContentKey key)
      throws NessieConflictException, NessieNotFoundException {

    Snapshot snapshot = metadata.currentSnapshot();
    long snapshotId = snapshot != null ? snapshot.snapshotId() : -1L;

    ImmutableCommitMeta.Builder builder = ImmutableCommitMeta.builder();
    builder.message(buildCommitMsg(base, metadata, key.toString()));
    if (isSnapshotOperation(base, metadata)) {
      builder.putProperties("iceberg.operation", snapshot.operation());
    }
    CommitMeta commitMeta = NessieUtil.catalogOptions(builder, catalogOptions).build();

    ImmutableIcebergTable.Builder newTableBuilder = ImmutableIcebergTable.builder();
    IcebergTable newTable =
        newTableBuilder
            .id(contentId)
            .snapshotId(snapshotId)
            .schemaId(metadata.currentSchemaId())
            .specId(metadata.defaultSpecId())
            .sortOrderId(metadata.defaultSortOrderId())
            .metadataLocation(newMetadataLocation)
            .build();

    Map<String, String> properties = base != null ? base.properties() : null;
    commitContent(key, newTable, properties, commitMeta);
  }

  public void commitView(
      ViewMetadata base,
      ViewMetadata metadata,
      String newMetadataLocation,
      String contentId,
      ContentKey key)
      throws NessieConflictException, NessieNotFoundException {

    long versionId = metadata.currentVersion().versionId();
    ImmutableIcebergView.Builder newViewBuilder = ImmutableIcebergView.builder();
    IcebergView newView =
        newViewBuilder
            .id(contentId)
            .versionId(versionId)
            .schemaId(metadata.currentSchemaId())
            .metadataLocation(newMetadataLocation)
            // Only view metadata location need to be tracked from Nessie.
            // Other information can be extracted by parsing the view metadata file.
            .sqlText("-")
            .dialect("-")
            .build();

    ImmutableCommitMeta.Builder builder = ImmutableCommitMeta.builder();
    builder.message(buildCommitMsg(base, metadata, key.toString()));
    builder.putProperties("iceberg.operation", metadata.currentVersion().operation());
    CommitMeta commitMeta = NessieUtil.catalogOptions(builder, catalogOptions).build();

    Map<String, String> properties = base != null ? base.properties() : null;
    commitContent(key, newView, properties, commitMeta);
  }

  private void commitContent(
      ContentKey key,
      IcebergContent newContent,
      Map<String, String> properties,
      CommitMeta commitMeta)
      throws NessieNotFoundException, NessieConflictException {
    UpdateableReference updateableReference = getRef();

    updateableReference.checkMutable();

    Branch current = (Branch) updateableReference.getReference();
    Branch expectedHead = current;
    if (properties != null) {
      String metadataCommitId =
          properties.getOrDefault(
              NessieTableOperations.NESSIE_COMMIT_ID_PROPERTY, expectedHead.getHash());
      if (metadataCommitId != null) {
        expectedHead = Branch.of(expectedHead.getName(), metadataCommitId);
      }
    }

    LOG.debug(
        "Committing '{}' against '{}', current is '{}': {}",
        key,
        expectedHead,
        current.getHash(),
        newContent);

    Branch branch =
        getApi()
            .commitMultipleOperations()
            .operation(Operation.Put.of(key, newContent))
            .commitMeta(commitMeta)
            .branch(expectedHead)
            .commit();
    LOG.info(
        "Committed '{}' against '{}', expected commit-id was '{}'",
        key,
        branch,
        expectedHead.getHash());
    updateableReference.updateReference(branch);
  }

  private boolean isSnapshotOperation(TableMetadata base, TableMetadata metadata) {
    Snapshot snapshot = metadata.currentSnapshot();
    return snapshot != null
        && (base == null
            || base.currentSnapshot() == null
            || snapshot.snapshotId() != base.currentSnapshot().snapshotId());
  }

  private <T extends OnReferenceBuilder<?>> T withReference(T builder) {
    UpdateableReference ref = getRef();
    if (!ref.isMutable()) {
      builder.reference(ref.getReference());
    } else {
      builder.refName(ref.getName());
    }
    return builder;
  }

  private String buildCommitMsg(TableMetadata base, TableMetadata metadata, String tableName) {
    if (isSnapshotOperation(base, metadata)) {
      return String.format(
          "Iceberg %s against %s", metadata.currentSnapshot().operation(), tableName);
    } else if (base != null && metadata.currentSchemaId() != base.currentSchemaId()) {
      return String.format("Iceberg schema change against table %s", tableName);
    } else if (base == null) {
      return String.format("Iceberg table created/registered with name %s", tableName);
    }
    return String.format("Iceberg commit against table %s", tableName);
  }

  private String buildCommitMsg(ViewMetadata base, ViewMetadata metadata, String viewName) {
    String operation = metadata.currentVersion().operation();
    if (base != null && !metadata.currentSchemaId().equals(base.currentSchemaId())) {
      return String.format(
          "Iceberg schema change against view %s for the operation %s", viewName, operation);
    }
    return String.format("Iceberg view %sd with name %s", operation, viewName);
  }

  public String refName() {
    return getRef().getName();
  }

  @Override
  public void close() {
    if (null != api) {
      api.close();
    }
  }

  private void commitRetry(String message, Operation... ops)
      throws BaseNessieClientServerException {
    commitRetry(message, false, builder -> builder.operations(Arrays.asList(ops)));
  }

  private void commitRetry(String message, boolean retryConflicts, CommitEnhancer commitEnhancer)
      throws BaseNessieClientServerException {
    // Retry all errors except for NessieNotFoundException and also NessieConflictException, unless
    // retryConflicts is set to true.
    Predicate<Exception> shouldRetry =
        e ->
            !(e instanceof NessieNotFoundException)
                && (!(e instanceof NessieConflictException) || retryConflicts);
    Tasks.range(1)
        .retry(5)
        .shouldRetryTest(shouldRetry)
        .throwFailureWhenFinished()
        .onFailure((o, exception) -> refresh())
        .run(
            i -> {
              try {
                Branch branch =
                    commitEnhancer
                        .enhance(api.commitMultipleOperations())
                        .commitMeta(NessieUtil.buildCommitMetadata(message, catalogOptions))
                        .branch((Branch) getReference())
                        .commit();
                getRef().updateReference(branch);
              } catch (NessieConflictException e) {
                if (retryConflicts) {
                  refresh(); // otherwise retrying a conflict doesn't make sense
                }
                throw e;
              }
            },
            BaseNessieClientServerException.class);
  }

  private static AlreadyExistsException namespaceAlreadyExists(
      ContentKey key, @Nullable Content existing, @Nullable Exception ex) {
    if (existing instanceof org.projectnessie.model.Namespace) {
      return new AlreadyExistsException(ex, "Namespace already exists: %s", key);
    } else {
      return new AlreadyExistsException(
          ex, "Another content object with name '%s' already exists", key);
    }
  }

  private static Optional<org.projectnessie.model.Namespace> unwrapNamespace(Content content) {
    return content == null
        ? Optional.empty()
        : content.unwrap(org.projectnessie.model.Namespace.class);
  }

  private interface CommitEnhancer {

    CommitMultipleOperationsBuilder enhance(CommitMultipleOperationsBuilder builder)
        throws BaseNessieClientServerException;
  }
}
