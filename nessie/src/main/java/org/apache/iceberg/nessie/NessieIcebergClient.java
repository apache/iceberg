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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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
import org.apache.iceberg.util.Tasks;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.OnReferenceBuilder;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.GetNamespacesResponse;
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

      // First, check that the namespace doesn't exist at the current hash;
      // this will avoid a NessieBadRequestException when committing the namespace creation
      Reference ref = getReference();
      Map<ContentKey, Content> contentMap = api.getContent().reference(ref).key(key).get();
      Content existing = contentMap.get(key);
      if (existing != null) {
        if (existing instanceof org.projectnessie.model.Namespace) {
          throw new AlreadyExistsException("Namespace already exists: '%s'", namespace);
        } else {
          throw new AlreadyExistsException(
              "Another content object with name '%s' already exists", key.toPathString());
        }
      }

      try {

        CommitMultipleOperationsBuilder commitBuilder =
            api.commitMultipleOperations()
                .commitMeta(
                    NessieUtil.buildCommitMetadata("create namespace " + key, catalogOptions))
                .operation(Operation.Put.of(key, content));

        Tasks.foreach(commitBuilder)
            .retry(5)
            .stopRetryOn(
                NessieReferenceNotFoundException.class, NessieReferenceConflictException.class)
            .throwFailureWhenFinished()
            .onFailure((o, exception) -> refresh())
            .run(
                b -> {
                  Branch branch = b.branch((Branch) getReference()).commit();
                  getRef().updateReference(branch);
                },
                BaseNessieClientServerException.class);

      } catch (NessieReferenceConflictException e) {
        List<Conflict> conflicts = e.getErrorDetails().conflicts();
        if (conflicts.size() == 1) {
          Conflict conflict = conflicts.get(0);
          if (conflict.conflictType() == Conflict.ConflictType.KEY_EXISTS) {
            // could be a namespace or something else, we can't tell here
            throw new AlreadyExistsException("Namespace already exists: '%s'", namespace);
          } else if (conflict.conflictType() == Conflict.ConflictType.NAMESPACE_ABSENT) {
            throw new NoSuchNamespaceException(
                "Cannot create Namespace '%s': parent namespace '%s' does not exist",
                namespace, conflict.key());
          }
        }
        throw new RuntimeException(
            String.format("Cannot create Namespace '%s': %s", namespace, e.getMessage()));
      } catch (BaseNessieClientServerException e) {
        throw new RuntimeException(
            String.format("Cannot create Namespace '%s': %s", namespace, e.getMessage()));
      }
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(
          String.format(
              "Cannot create Namespace '%s': ref '%s' is no longer valid.",
              namespace, getRef().getName()),
          e);
    }
  }

  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    try {
      GetNamespacesResponse response =
          withReference(
                  getApi()
                      .getMultipleNamespaces()
                      .namespace(org.projectnessie.model.Namespace.of(namespace.levels())))
              .get();
      return response.getNamespaces().stream()
          .map(ns -> Namespace.of(ns.getElements().toArray(new String[0])))
          .filter(ns -> ns.length() == namespace.length() + 1)
          .collect(Collectors.toList());
    } catch (NessieReferenceNotFoundException e) {
      throw new RuntimeException(
          String.format(
              "Cannot list Namespaces starting from '%s': " + "ref '%s' is no longer valid.",
              namespace, getRef().getName()),
          e);
    }
  }

  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {

    getRef().checkMutable();
    ContentKey key = ContentKey.of(namespace.levels());

    try {

      CommitMultipleOperationsBuilder commitBuilder =
          api.commitMultipleOperations()
              .commitMeta(NessieUtil.buildCommitMetadata("drop namespace " + key, catalogOptions))
              .operation(Operation.Delete.of(key));

      Tasks.foreach(commitBuilder)
          .retry(5)
          .stopRetryOn(
              NessieReferenceNotFoundException.class, NessieReferenceConflictException.class)
          .throwFailureWhenFinished()
          .onFailure((o, exception) -> refresh())
          .run(
              b -> {
                Branch branch = b.branch((Branch) getRef().getReference()).commit();
                getRef().updateReference(branch);
              },
              BaseNessieClientServerException.class);

      return true;
    } catch (NessieReferenceNotFoundException e) {
      LOG.error(
          "Cannot drop Namespace '{}': ref '{}' is no longer valid.",
          namespace,
          getRef().getName(),
          e);
      return false;
    } catch (NessieReferenceConflictException e) {
      List<Conflict> conflicts = e.getErrorDetails().conflicts();
      if (conflicts.size() == 1) {
        Conflict conflict = conflicts.get(0);
        if (Objects.equals(conflict.key(), key)) {
          if (conflict.conflictType() == Conflict.ConflictType.KEY_DOES_NOT_EXIST) {
            return false;
          }
          if (conflict.conflictType() == Conflict.ConflictType.NAMESPACE_NOT_EMPTY) {
            throw new NamespaceNotEmptyException(
                e, "Namespace '%s' is not empty. One or more tables exist.", namespace);
          }
        }
      }
      throw new RuntimeException(
          String.format("Cannot drop Namespace '%s': %s", namespace, e.getMessage()));
    } catch (BaseNessieClientServerException e) {
      throw new RuntimeException(
          String.format("Cannot drop Namespace '%s': %s", namespace, e.getMessage()));
    }
  }

  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    try {
      return withReference(
              getApi()
                  .getNamespace()
                  .namespace(org.projectnessie.model.Namespace.of(namespace.levels())))
          .get()
          .getProperties();
    } catch (NessieNamespaceNotFoundException e) {
      throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);
    } catch (NessieReferenceNotFoundException e) {
      throw new RuntimeException(
          String.format(
              "Cannot load Namespace '%s': " + "ref '%s' is no longer valid.",
              namespace, getRef().getName()),
          e);
    }
  }

  public boolean setProperties(Namespace namespace, Map<String, String> properties) {
    try {
      withReference(
              getApi()
                  .updateProperties()
                  .namespace(org.projectnessie.model.Namespace.of(namespace.levels()))
                  .updateProperties(properties))
          .update();
      refresh();
      // always successful, otherwise an exception is thrown
      return true;
    } catch (NessieNamespaceNotFoundException e) {
      throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(
          String.format(
              "Cannot update properties on Namespace '%s': ref '%s' is no longer valid.",
              namespace, getRef().getName()),
          e);
    }
  }

  public boolean removeProperties(Namespace namespace, Set<String> properties) {
    try {
      withReference(
              getApi()
                  .updateProperties()
                  .namespace(org.projectnessie.model.Namespace.of(namespace.levels()))
                  .removeProperties(properties))
          .update();
      refresh();
      // always successful, otherwise an exception is thrown
      return true;
    } catch (NessieNamespaceNotFoundException e) {
      throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(
          String.format(
              "Cannot remove properties from Namespace '%s': ref '%s' is no longer valid.",
              namespace, getRef().getName()),
          e);
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

    CommitMultipleOperationsBuilder operations =
        getApi()
            .commitMultipleOperations()
            .commitMeta(
                NessieUtil.buildCommitMetadata(
                    String.format("Iceberg rename table from '%s' to '%s'", from, to),
                    catalogOptions))
            .operation(Operation.Delete.of(NessieUtil.toKey(from)))
            .operation(Operation.Put.of(NessieUtil.toKey(to), existingFromTable));

    try {
      Tasks.foreach(operations)
          .retry(5)
          .stopRetryOn(NessieNotFoundException.class)
          .throwFailureWhenFinished()
          .onFailure((o, exception) -> refresh())
          .run(
              ops -> {
                Branch branch = ops.branch((Branch) getRef().getReference()).commit();
                getRef().updateReference(branch);
              },
              BaseNessieClientServerException.class);
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
              "Cannot rename table '%s' to '%s': " + "ref '%s' no longer exists.",
              from.name(), to.name(), getRef().getName()),
          e);
    } catch (BaseNessieClientServerException e) {
      throw new CommitFailedException(
          e,
          "Cannot rename table '%s' to '%s': " + "the current reference is not up to date.",
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

    CommitMultipleOperationsBuilder commitBuilderBase =
        getApi()
            .commitMultipleOperations()
            .commitMeta(
                NessieUtil.buildCommitMetadata(
                    String.format("Iceberg delete table %s", identifier), catalogOptions))
            .operation(Operation.Delete.of(NessieUtil.toKey(identifier)));

    // We try to drop the table. Simple retry after ref update.
    boolean threw = true;
    try {
      Tasks.foreach(commitBuilderBase)
          .retry(5)
          .stopRetryOn(NessieNotFoundException.class)
          .throwFailureWhenFinished()
          .onFailure((o, exception) -> refresh())
          .run(
              commitBuilder -> {
                Branch branch = commitBuilder.branch((Branch) getRef().getReference()).commit();
                getRef().updateReference(branch);
              },
              BaseNessieClientServerException.class);
      threw = false;
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
    return !threw;
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
}
