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

package org.apache.iceberg.rest;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.BaseTransaction.TransactionTable;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UnboundPartitionSpec;
import org.apache.iceberg.UnboundSortOrder;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Catalog.TableBuilder;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.util.Tasks;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

public class CatalogHandlers {

  private CatalogHandlers() {
  }

  /**
   * Exception used to avoid retrying commits when assertions fail.
   * <p>
   * When a REST assertion fails, it will throw CommitFailedException to send back to the client. But the assertion
   * checks happen in the block that is retried if {@link TableOperations#commit(TableMetadata, TableMetadata)} throws
   * CommitFailedException. This is used to avoid retries for assertion failures, which are unwrapped and rethrown
   * outside of the commit loop.
   */
  private static class ValidationFailureException extends RuntimeException {
    private final CommitFailedException wrapped;

    private ValidationFailureException(CommitFailedException cause) {
      super(cause);
      this.wrapped = cause;
    }

    public CommitFailedException wrapped() {
      return wrapped;
    }
  }

  private static class CreateCommitInfo {
    private Schema schema;
    private UnboundPartitionSpec unboundPartitionSpec;
    private UnboundSortOrder unboundSortOrder;
    private List<MetadataUpdate> otherUpdates = Lists.newArrayList();

    private boolean otherUpdatesHasType(Class<? extends MetadataUpdate> updateClass) {
      return otherUpdates.stream().anyMatch(updateClass::isInstance);
    }
  }

  public static ListNamespacesResponse listNamespaces(SupportsNamespaces catalog, Namespace parent) {
    List<Namespace> results;
    if (parent.isEmpty()) {
      results = catalog.listNamespaces();
    } else {
      results = catalog.listNamespaces(parent);
    }

    return ListNamespacesResponse.builder().addAll(results).build();
  }

  public static CreateNamespaceResponse createNamespace(SupportsNamespaces catalog, CreateNamespaceRequest request) {
    Namespace namespace = request.namespace();
    catalog.createNamespace(namespace, request.properties());
    return CreateNamespaceResponse.builder()
        .withNamespace(namespace)
        .setProperties(catalog.loadNamespaceMetadata(namespace))
        .build();
  }

  public static GetNamespaceResponse loadNamespace(SupportsNamespaces catalog, Namespace namespace) {
    Map<String, String> properties = catalog.loadNamespaceMetadata(namespace);
    return GetNamespaceResponse.builder()
        .withNamespace(namespace)
        .setProperties(properties)
        .build();
  }

  public static void dropNamespace(SupportsNamespaces catalog, Namespace namespace) {
    boolean dropped = catalog.dropNamespace(namespace);
    if (!dropped) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
  }

  public static UpdateNamespacePropertiesResponse updateNamespaceProperties(
      SupportsNamespaces catalog, Namespace namespace, UpdateNamespacePropertiesRequest request) {
    request.validate();

    Set<String> removals = Sets.newHashSet(request.removals());
    Map<String, String> updates = request.updates();

    Map<String, String> startProperties = catalog.loadNamespaceMetadata(namespace);
    Set<String> missing = Sets.difference(removals, startProperties.keySet());

    if (!updates.isEmpty()) {
      catalog.setProperties(namespace, updates);
    }

    if (!removals.isEmpty()) {
      // remove the original set just in case there was an update just after loading properties
      catalog.removeProperties(namespace, removals);
    }

    return UpdateNamespacePropertiesResponse.builder()
            .addMissing(missing)
            .addUpdated(updates.keySet())
            .addRemoved(Sets.difference(removals, missing))
            .build();
  }

  public static ListTablesResponse listTables(Catalog catalog, Namespace namespace) {
    List<TableIdentifier> idents = catalog.listTables(namespace);
    return ListTablesResponse.builder().addAll(idents).build();
  }

  public static LoadTableResponse stageTableCreate(Catalog catalog, Namespace namespace, CreateTableRequest request) {
    request.validate();

    TableIdentifier ident = TableIdentifier.of(namespace, request.name());
    if (catalog.tableExists(ident)) {
      throw new AlreadyExistsException("Table already exists: %s", ident);
    }

    Map<String, String> properties = Maps.newHashMap();
    properties.put("created-at", OffsetDateTime.now().toString());
    properties.putAll(request.properties());

    String location;
    if (request.location() != null) {
      location = request.location();
    } else {
      location = catalog.buildTable(ident, request.schema())
          .withPartitionSpec(request.spec())
          .withSortOrder(request.writeOrder())
          .withProperties(properties)
          .createTransaction()
          .table()
          .location();
    }

    TableMetadata metadata = TableMetadata.newTableMetadata(
        request.schema(),
        request.spec() != null ? request.spec() : PartitionSpec.unpartitioned(),
        request.writeOrder() != null ? request.writeOrder() : SortOrder.unsorted(),
        location,
        properties);

    return LoadTableResponse.builder()
        .withTableMetadata(metadata)
        .build();
  }

  public static LoadTableResponse createTable(Catalog catalog, Namespace namespace, CreateTableRequest request) {
    request.validate();

    TableIdentifier ident = TableIdentifier.of(namespace, request.name());
    Table table = catalog.buildTable(ident, request.schema())
        .withLocation(request.location())
        .withPartitionSpec(request.spec())
        .withSortOrder(request.writeOrder())
        .withProperties(request.properties())
        .create();

    if (table instanceof BaseTable) {
      return LoadTableResponse.builder()
          .withTableMetadata(((BaseTable) table).operations().current())
          .build();
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
  }

  public static void dropTable(Catalog catalog, TableIdentifier ident) {
    boolean dropped = catalog.dropTable(ident);
    if (!dropped) {
      throw new NoSuchTableException("Table does not exist: %s", ident);
    }
  }

  public static LoadTableResponse loadTable(Catalog catalog, TableIdentifier ident) {
    Table table = catalog.loadTable(ident);

    if (table instanceof BaseTable) {
      return LoadTableResponse.builder()
          .withTableMetadata(((BaseTable) table).operations().current())
          .build();
    } else if (table instanceof BaseMetadataTable) {
      // metadata tables are loaded on the client side, return NoSuchTableException for now
      throw new NoSuchTableException("Table does not exist: %s", ident.toString());
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
  }

  public static void renameTable(Catalog catalog, RenameTableRequest request) {
    catalog.renameTable(request.source(), request.destination());
  }

  public static LoadTableResponse updateTable(Catalog catalog, TableIdentifier ident, UpdateTableRequest request) {
    TableMetadata finalMetadata;
    if (isCreate(request)) {
      finalMetadata = commitCreate(catalog, ident, request);
    } else {
      finalMetadata = commitUpdate(catalog, ident, request);
    }

    return LoadTableResponse.builder()
        .withTableMetadata(finalMetadata)
        .build();
  }

  private static boolean isCreate(UpdateTableRequest request) {
    boolean isCreate = request.requirements().stream()
        .anyMatch(UpdateTableRequest.UpdateRequirement.AssertTableDoesNotExist.class::isInstance);

    if (isCreate) {
      List<UpdateTableRequest.UpdateRequirement> invalidRequirements = request.requirements().stream()
          .filter(req -> !(req instanceof UpdateTableRequest.UpdateRequirement.AssertTableDoesNotExist))
          .collect(Collectors.toList());
      Preconditions.checkArgument(invalidRequirements.isEmpty(),
          "Invalid create requirements: %s", invalidRequirements);
    }

    return isCreate;
  }

  private static TableMetadata commitUpdate(Catalog catalog, TableIdentifier ident, UpdateTableRequest request) {
    Table table = catalog.loadTable(ident);
    if (table instanceof BaseTable) {
      TableOperations ops = ((BaseTable) table).operations();
      return commit(ops, request.requirements(), request.updates());
    } else {
      throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
    }
  }

  private static TableMetadata commitCreate(Catalog catalog, TableIdentifier ident, UpdateTableRequest request) {
    CreateCommitInfo info = extractCreateCommitInfo(request);
    if (info.schema == null) {
      throw new BadRequestException("No schema specified for create commit");
    }

    TableBuilder builder = catalog.buildTable(ident, info.schema);
    initTableBuilder(builder, info);

    Transaction transaction = builder.createOrReplaceTransaction();
    Preconditions.checkState(transaction instanceof BaseTransaction,
        "Cannot wrap catalog that does not produce BaseTransaction");

    // the only valid requirement is that the table will be created
    request.requirements().forEach(requirement ->
        requirement.validate(((BaseTransaction) transaction).underlyingOps().current()));

    TableOperations ops = ((TransactionTable) transaction.table()).operations();
    TableMetadata start = ops.current();
    TableMetadata.Builder metaBuilder = TableMetadata.buildFrom(start);
    info.otherUpdates.forEach(update -> update.applyTo(metaBuilder));

    ops.commit(start, metaBuilder.build());
    transaction.commitTransaction();

    return ops.current();
  }

  private static void initTableBuilder(TableBuilder builder, CreateCommitInfo info) {
    if (info.unboundPartitionSpec != null) {
      builder.withPartitionSpec(info.unboundPartitionSpec.bind(info.schema));
    }
    if (info.unboundSortOrder != null) {
      builder.withSortOrder(info.unboundSortOrder.bind(info.schema));
    }
  }

  private static CreateCommitInfo extractCreateCommitInfo(UpdateTableRequest request) {
    // Here we are extracting the initial schema, partition spec, and sort order, which
    // we will use to initialize the table. Other updates are applied on top of that.
    // If we add a second schema then we assume we have captured the necessary info for
    // the table init, and we will apply all updates after that.

    CreateCommitInfo result = new CreateCommitInfo();
    boolean initialSchema = true;
    for (MetadataUpdate update : request.updates()) {
      if (initialSchema && update instanceof MetadataUpdate.AddSchema) {
        if (result.schema == null) {
          result.schema = ((MetadataUpdate.AddSchema) update).schema();
        } else {
          initialSchema = false;
          addCreateCommitUpdate(result, update);
        }
      } else if (initialSchema && result.unboundPartitionSpec == null &&
          update instanceof MetadataUpdate.AddPartitionSpec) {
        result.unboundPartitionSpec = ((MetadataUpdate.AddPartitionSpec) update).spec();
      } else if (initialSchema && result.unboundSortOrder == null && update instanceof MetadataUpdate.AddSortOrder) {
        result.unboundSortOrder = ((MetadataUpdate.AddSortOrder) update).sortOrder();
      } else {
        addCreateCommitUpdate(result, update);
      }
    }

    return result;
  }

  private static void addCreateCommitUpdate(CreateCommitInfo commitInfo, MetadataUpdate update) {
    if (update instanceof MetadataUpdate.SetCurrentSchema) {
      if (commitInfo.otherUpdatesHasType(MetadataUpdate.AddSchema.class)) {
        commitInfo.otherUpdates.add(update);
      }
    } else if (update instanceof MetadataUpdate.SetDefaultPartitionSpec) {
      if (commitInfo.otherUpdatesHasType(MetadataUpdate.AddPartitionSpec.class)) {
        commitInfo.otherUpdates.add(update);
      }
    } else if (update instanceof MetadataUpdate.SetDefaultSortOrder) {
      if (commitInfo.otherUpdatesHasType(MetadataUpdate.AddSortOrder.class)) {
        commitInfo.otherUpdates.add(update);
      }
    } else {
      commitInfo.otherUpdates.add(update);
    }
  }

  private static TableMetadata commit(TableOperations ops, List<UpdateTableRequest.UpdateRequirement> requirements,
      List<MetadataUpdate> updates) {
    AtomicBoolean isRetry = new AtomicBoolean(false);
    try {
      Tasks.foreach(ops)
          .retry(COMMIT_NUM_RETRIES_DEFAULT)
          .exponentialBackoff(
              COMMIT_MIN_RETRY_WAIT_MS_DEFAULT, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT,
              COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT, 2.0 /* exponential */)
          .onlyRetryOn(CommitFailedException.class)
          .run(taskOps -> {
            TableMetadata base = isRetry.get() ? taskOps.refresh() : taskOps.current();
            isRetry.set(true);

            // validate requirements
            try {
              requirements.forEach(requirement -> requirement.validate(base));
            } catch (CommitFailedException e) {
              // wrap and rethrow outside of tasks to avoid unnecessary retry
              throw new ValidationFailureException(e);
            }

            // apply changes
            TableMetadata.Builder metadataBuilder = TableMetadata.buildFrom(base);
            updates.forEach(update -> update.applyTo(metadataBuilder));

            TableMetadata updated = metadataBuilder.build();
            if (updated.changes().isEmpty()) {
              // do not commit if the metadata has not changed
              return;
            }

            // commit
            taskOps.commit(base, updated);
          });

    } catch (ValidationFailureException e) {
      throw e.wrapped();
    }

    return ops.current();
  }
}
