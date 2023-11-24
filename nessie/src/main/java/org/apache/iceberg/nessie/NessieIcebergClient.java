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
import org.apache.iceberg.relocated.com.google.common.base.Suppliers;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;
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
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableIcebergTable;
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
    try {
      return withReference(api.getEntries()).get().getEntries().stream()
          .filter(namespacePredicate(namespace))
          .filter(e -> Content.Type.ICEBERG_TABLE == e.getType())
          .map(this::toIdentifier)
          .collect(Collectors.toList());
    } catch (NessieNotFoundException ex) {
      throw new NoSuchNamespaceException(
          ex, "Unable to list tables due to missing ref '%s'", getRef().getName());
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

  public IcebergTable table(TableIdentifier tableIdentifier) {
    try {
      ContentKey key = NessieUtil.toKey(tableIdentifier);
      Content table = withReference(api.getContent().key(key)).get().get(key);
      return table != null ? table.unwrap(IcebergTable.class).orElse(null) : null;
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
                  "Cannot create Namespace '%s': parent namespace '%s' does not exist",
                  namespace,
                  conflict.get().key());
          }
        }
        throw new RuntimeException(
            String.format("Cannot create Namespace '%s': %s", namespace, e.getMessage()));
      }
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(
          String.format(
              "Cannot create Namespace '%s': ref '%s' is no longer valid.",
              namespace, getRef().getName()),
          e);
    } catch (BaseNessieClientServerException e) {
      throw new RuntimeException(
          String.format("Cannot create Namespace '%s': %s", namespace, e.getMessage()), e);
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
            "Cannot list top-level Namespaces: ref '%s' is no longer valid.",
            getRef().getName());
      }
      throw new NoSuchNamespaceException(
          e,
          "Cannot list child Namespaces from '%s': ref '%s' is no longer valid.",
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
            "Content object with name '%s' is not a Namespace.", namespace);
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
            String.format("Cannot drop Namespace '%s': %s", namespace, e.getMessage()));
      }
    } catch (NessieNotFoundException e) {
      LOG.error(
          "Cannot drop Namespace '{}': ref '{}' is no longer valid.",
          namespace,
          getRef().getName(),
          e);
    } catch (BaseNessieClientServerException e) {
      throw new RuntimeException(
          String.format("Cannot drop Namespace '%s': %s", namespace, e.getMessage()), e);
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
              "Cannot load Namespace '%s': ref '%s' is no longer valid.",
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
              "Cannot update properties on Namespace '%s': %s", namespace, e.getMessage()));
    } catch (NessieContentNotFoundException e) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    } catch (NessieReferenceNotFoundException e) {
      throw new RuntimeException(
          String.format(
              "Cannot update properties on Namespace '%s': ref '%s' is no longer valid.",
              namespace, getRef().getName()),
          e);
    } catch (BaseNessieClientServerException e) {
      throw new RuntimeException(
          String.format("Cannot update Namespace '%s': %s", namespace, e.getMessage()), e);
    }
  }

  public void renameTable(TableIdentifier from, TableIdentifier to) {
    getRef().checkMutable();

    IcebergTable existingFromTable = table(from);
    if (existingFromTable == null) {
      throw new NoSuchTableException("Table does not exist: %s", from.name());
    }
    IcebergTable existingToTable = table(to);
    if (existingToTable != null) {
      throw new AlreadyExistsException("Table already exists: %s", to.name());
    }

    try {
      commitRetry(
          String.format("Iceberg rename table from '%s' to '%s'", from, to),
          Operation.Delete.of(NessieUtil.toKey(from)),
          Operation.Put.of(NessieUtil.toKey(to), existingFromTable));
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
              "Cannot rename table '%s' to '%s': ref '%s' no longer exists.",
              from.name(), to.name(), getRef().getName()),
          e);
    } catch (BaseNessieClientServerException e) {
      throw new CommitFailedException(
          e,
          "Cannot rename table '%s' to '%s': the current reference is not up to date.",
          from.name(),
          to.name());
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

  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    getRef().checkMutable();

    IcebergTable existingTable = table(identifier);
    if (existingTable == null) {
      return false;
    }

    if (purge) {
      LOG.info("Purging data for table {} was set to true but is ignored", identifier.toString());
    }

    // We try to drop the table. Simple retry after ref update.
    try {
      commitRetry(
          String.format("Iceberg delete table %s", identifier),
          Operation.Delete.of(NessieUtil.toKey(identifier)));
      return true;
    } catch (NessieConflictException e) {
      LOG.error(
          "Cannot drop table: failed after retry (update ref '{}' and retry)",
          getRef().getName(),
          e);
    } catch (NessieNotFoundException e) {
      LOG.error("Cannot drop table: ref '{}' is no longer valid.", getRef().getName(), e);
    } catch (BaseNessieClientServerException e) {
      LOG.error("Cannot drop table: unknown error", e);
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
    UpdateableReference updateableReference = getRef();

    updateableReference.checkMutable();

    Branch current = (Branch) updateableReference.getReference();
    Branch expectedHead = current;
    if (base != null) {
      String metadataCommitId =
          base.property(NessieTableOperations.NESSIE_COMMIT_ID_PROPERTY, expectedHead.getHash());
      if (metadataCommitId != null) {
        expectedHead = Branch.of(expectedHead.getName(), metadataCommitId);
      }
    }

    Snapshot snapshot = metadata.currentSnapshot();
    long snapshotId = snapshot != null ? snapshot.snapshotId() : -1L;

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

    LOG.debug(
        "Committing '{}' against '{}', current is '{}': {}",
        key,
        expectedHead,
        current.getHash(),
        newTable);
    ImmutableCommitMeta.Builder builder = ImmutableCommitMeta.builder();
    builder.message(buildCommitMsg(base, metadata, key.toString()));
    if (isSnapshotOperation(base, metadata)) {
      builder.putProperties("iceberg.operation", snapshot.operation());
    }
    Branch branch =
        getApi()
            .commitMultipleOperations()
            .operation(Operation.Put.of(key, newTable))
            .commitMeta(NessieUtil.catalogOptions(builder, catalogOptions).build())
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
      return String.format("Iceberg schema change against %s", tableName);
    } else if (base == null) {
      return String.format("Iceberg table created/registered with name %s", tableName);
    }
    return String.format("Iceberg commit against %s", tableName);
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
      return new AlreadyExistsException(ex, "Namespace already exists: '%s'", key);
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
