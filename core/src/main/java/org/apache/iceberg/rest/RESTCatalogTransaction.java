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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PendingUpdate;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataDiffAccess;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.UpdateStatistics;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.CatalogTransaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.util.Tasks;

public class RESTCatalogTransaction implements CatalogTransaction {
  private final Map<TableIdentifier, Transaction> txByTable;
  private final Map<TableIdentifier, TableMetadata> initiallyReadTableMetadata;
  private final IsolationLevel isolationLevel;
  private final Catalog origin;
  private final RESTSessionCatalog sessionCatalog;
  private final SessionCatalog.SessionContext context;
  private boolean hasCommitted = false;

  public RESTCatalogTransaction(
      Catalog origin,
      RESTSessionCatalog sessionCatalog,
      SessionCatalog.SessionContext context,
      IsolationLevel isolationLevel) {
    Preconditions.checkArgument(null != origin, "Invalid origin catalog: null");
    Preconditions.checkArgument(null != sessionCatalog, "Invalid session catalog: null");
    Preconditions.checkArgument(null != context, "Invalid session context: null");
    Preconditions.checkArgument(null != isolationLevel, "Invalid isolation level: null");
    this.origin = origin;
    this.isolationLevel = isolationLevel;
    this.sessionCatalog = sessionCatalog;
    this.context = context;
    this.txByTable = Maps.newConcurrentMap();
    this.initiallyReadTableMetadata = Maps.newConcurrentMap();
    // TODO: we should probably have a TX ID for logging and other purposes
  }

  @Override
  public void commitTransaction() {
    Preconditions.checkState(!hasCommitted, "Transaction has already committed changes");

    try {
      for (TableIdentifier readTable : initiallyReadTableMetadata.keySet()) {
        // we need to check all read tables to determine whether they changed outside the catalog
        // TX after we initially read them
        if (IsolationLevel.SERIALIZABLE == isolationLevel) {
          TableMetadata currentTableMetadata =
              ((BaseTable) origin.loadTable(readTable)).operations().current();

          if (!currentTableMetadata
              .metadataFileLocation()
              .equals(initiallyReadTableMetadata.get(readTable).metadataFileLocation())) {
            throw new ValidationException(
                "%s isolation violation: Found table metadata updates to table '%s' after it was read",
                isolationLevel(), readTable);
          }
        }
      }

      Map<TableIdentifier, List<PendingUpdate>> pendingUpdatesByTable = Maps.newConcurrentMap();
      txByTable.forEach((key, value) -> pendingUpdatesByTable.put(key, value.pendingUpdates()));
      Map<TableIdentifier, UpdateTableRequest> updatesByTable = Maps.newHashMap();

      for (TableIdentifier affectedTable : pendingUpdatesByTable.keySet()) {
        List<TableMetadataDiffAccess.TableMetadataDiff> tableMetadataDiffs =
            pendingUpdatesByTable.get(affectedTable).stream()
                .filter(pendingUpdate -> pendingUpdate instanceof TableMetadataDiffAccess)
                .map(pendingUpdate -> (TableMetadataDiffAccess) pendingUpdate)
                .map(TableMetadataDiffAccess::tableMetadataDiff)
                .collect(Collectors.toList());

        // first one contains the base metadata to create requirements from
        TableMetadataDiffAccess.TableMetadataDiff firstOne = tableMetadataDiffs.get(0);
        // the last one contains all metadata updates
        TableMetadataDiffAccess.TableMetadataDiff lastOne =
            tableMetadataDiffs.get(tableMetadataDiffs.size() - 1);

        List<MetadataUpdate> metadataUpdates = lastOne.updated().changes();

        UpdateTableRequest.Builder builder = UpdateTableRequest.builderFor(firstOne.base());
        metadataUpdates.forEach(builder::update);
        UpdateTableRequest updateTableRequest = builder.build();
        updatesByTable.put(affectedTable, updateTableRequest);
      }

      // TODO: what if a TX failed, should we make sure it can't be committed multiple times?
      // TODO: should this be retryable?
      sessionCatalog.commitCatalogTransaction(context, updatesByTable);

      hasCommitted = true;
    } catch (CommitStateUnknownException e) {
      throw e;
    } catch (RuntimeException e) {
      rollback();
      throw e;
    }
  }

  @Override
  public void rollback() {
    Tasks.foreach(txByTable.values()).run(Transaction::rollback);
    txByTable.clear();
    initiallyReadTableMetadata.clear();
  }

  @Override
  public Catalog asCatalog() {
    return new AsTransactionalCatalog();
  }

  @Override
  public IsolationLevel isolationLevel() {
    return isolationLevel;
  }

  private Optional<Table> txTable(TableIdentifier identifier) {
    if (txByTable.containsKey(identifier)) {
      return Optional.ofNullable(txByTable.get(identifier).table());
    }

    return Optional.empty();
  }

  /**
   * We're using a {@link Transaction} per table so that we can keep track of pending changes for a
   * particular table.
   */
  private Transaction txForTable(Table table) {
    return txByTable.computeIfAbsent(
        identifierWithoutCatalog(table.name()),
        k -> Transactions.newTransaction(table.name(), ((HasTableOperations) table).operations()));
  }

  // TODO: this functionality should probably live somewhere else to be reusable
  private TableIdentifier identifierWithoutCatalog(String tableWithCatalog) {
    if (tableWithCatalog.startsWith(origin.name())) {
      return TableIdentifier.parse(tableWithCatalog.replace(origin.name() + ".", ""));
    }
    return TableIdentifier.parse(tableWithCatalog);
  }

  public class AsTransactionalCatalog implements Catalog {
    @Override
    public Table loadTable(TableIdentifier identifier) {
      Table table =
          RESTCatalogTransaction.this
              .txTable(identifier)
              .orElseGet(
                  () -> {
                    Table loadTable = origin.loadTable(identifier);

                    // we need to remember the very first version of table metadata that we read
                    if (IsolationLevel.SERIALIZABLE == isolationLevel()) {
                      initiallyReadTableMetadata.computeIfAbsent(
                          identifier, (ident) -> ((BaseTable) loadTable).operations().current());
                    }

                    return loadTable;
                  });

      TableOperations tableOps =
          table instanceof BaseTransaction.TransactionTable
              ? ((BaseTransaction.TransactionTable) table).operations()
              : ((BaseTable) table).operations();
      return new TransactionalTable(table, tableOps);
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return origin.listTables(namespace);
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      return origin.dropTable(identifier, purge);
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
      origin.renameTable(from, to);
    }
  }

  private class TransactionalTable extends BaseTable {
    private final Table table;

    private TransactionalTable(Table table, TableOperations ops) {
      super(ops, table.name());
      this.table = table;
    }

    @Override
    public UpdateSchema updateSchema() {
      return txForTable(table).updateSchema();
    }

    @Override
    public UpdatePartitionSpec updateSpec() {
      return txForTable(table).updateSpec();
    }

    @Override
    public UpdateProperties updateProperties() {
      return txForTable(table).updateProperties();
    }

    @Override
    public ReplaceSortOrder replaceSortOrder() {
      return txForTable(table).replaceSortOrder();
    }

    @Override
    public UpdateLocation updateLocation() {
      return txForTable(table).updateLocation();
    }

    @Override
    public AppendFiles newAppend() {
      return txForTable(table).newAppend();
    }

    @Override
    public AppendFiles newFastAppend() {
      return txForTable(table).newFastAppend();
    }

    @Override
    public RewriteFiles newRewrite() {
      return txForTable(table).newRewrite();
    }

    @Override
    public RewriteManifests rewriteManifests() {
      return txForTable(table).rewriteManifests();
    }

    @Override
    public OverwriteFiles newOverwrite() {
      return txForTable(table).newOverwrite();
    }

    @Override
    public RowDelta newRowDelta() {
      return txForTable(table).newRowDelta();
    }

    @Override
    public ReplacePartitions newReplacePartitions() {
      return txForTable(table).newReplacePartitions();
    }

    @Override
    public DeleteFiles newDelete() {
      return txForTable(table).newDelete();
    }

    @Override
    public UpdateStatistics updateStatistics() {
      return txForTable(table).updateStatistics();
    }

    @Override
    public ExpireSnapshots expireSnapshots() {
      return txForTable(table).expireSnapshots();
    }

    @Override
    public ManageSnapshots manageSnapshots() {
      return txForTable(table).manageSnapshots();
    }
  }
}
