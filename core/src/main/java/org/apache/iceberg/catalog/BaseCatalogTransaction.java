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
package org.apache.iceberg.catalog;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.DataTableScan;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.UpdateStatistics;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTCatalog;
import org.immutables.value.Value;

public class BaseCatalogTransaction implements CatalogTransaction {
  private final Map<TableIdentifier, Transaction> txByTable;
  private final Map<TableRef, TableMetadata> initiallyReadTableMetadataByRef;
  private final Map<TableIdentifier, Table> initiallyReadTables;
  private final IsolationLevel isolationLevel;
  private final Catalog origin;
  private boolean hasCommitted = false;

  public BaseCatalogTransaction(Catalog origin, IsolationLevel isolationLevel) {
    Preconditions.checkArgument(null != origin, "Invalid origin catalog: null");
    Preconditions.checkArgument(null != isolationLevel, "Invalid isolation level: null");
    Preconditions.checkArgument(
        origin instanceof SupportsCatalogTransactions,
        "Origin catalog does not support catalog transactions");
    this.origin = origin;
    this.isolationLevel = isolationLevel;
    this.txByTable = Maps.newHashMap();
    this.initiallyReadTableMetadataByRef = Maps.newHashMap();
    this.initiallyReadTables = Maps.newHashMap();
  }

  @Override
  public void commitTransaction() {
    Preconditions.checkState(!hasCommitted, "Transaction has already committed changes");

    try {
      // write skew is not possible in read-only transactions, so only perform that check if there
      // were any pending updates
      if (hasUpdates()) {
        validateSerializableIsolation();
      }

      List<TableCommit> tableCommits =
          txByTable.entrySet().stream()
              .filter(e -> e.getValue() instanceof BaseTransaction)
              .map(
                  e ->
                      TableCommit.create(
                          e.getKey(),
                          ((BaseTransaction) e.getValue()).startMetadata(),
                          ((BaseTransaction) e.getValue()).currentMetadata()))
              .collect(Collectors.toList());

      // TODO: should this be retryable?
      // only commit if there were change
      if (!tableCommits.isEmpty()) {
        // TODO: remove this cast once commitTransaction(..) is defined at the Catalog level
        ((RESTCatalog) origin).commitTransaction(tableCommits);
      }

      // TODO: we should probably be refreshing metadata from all affected tables after the TX
      // committed (alternatively to this approach we could load each affected table from the origin
      // catalog
      //      txByTable.values().stream()
      //          .filter(tx -> tx instanceof BaseTransaction)
      //          .map(tx -> (BaseTransaction) tx)
      //          .forEach(tx -> tx.underlyingOps().refresh());

      hasCommitted = true;
    } catch (CommitStateUnknownException e) {
      throw e;
    } catch (RuntimeException e) {
      rollback();
      throw e;
    }
  }

  private boolean hasUpdates() {
    return txByTable.values().stream()
        .filter(tx -> tx instanceof BaseTransaction)
        .map(tx -> (BaseTransaction) tx)
        .anyMatch(tx -> !tx.currentMetadata().changes().isEmpty());
  }

  /**
   * With SERIALIZABLE isolation we mainly need to check that write skew is not possible. Write skew
   * happens due to a transaction taking action based on an outdated premise (a fact that was true
   * when a table was initially loaded but then changed due to a concurrent update to the table
   * while this TX was in-progress). When this TX wants to commit, the original premise might not
   * hold anymore, thus we need to check whether the {@link org.apache.iceberg.Snapshot} a branch
   * was pointing to changed after it was initially read inside this TX. If no information of a
   * branch's snapshot is available, we check whether {@link TableMetadata} changed after it was
   * initially read.
   */
  private void validateSerializableIsolation() {
    for (TableRef readTable : initiallyReadTableMetadataByRef.keySet()) {
      // check all read tables to determine whether they changed outside the catalog
      // TX after they were initially read on a particular branch
      if (IsolationLevel.SERIALIZABLE == isolationLevel) {
        BaseTable table = (BaseTable) origin.loadTable(readTable.identifier());
        SnapshotRef snapshotRef = table.operations().current().ref(readTable.ref());
        SnapshotRef snapshotRefInsideTx =
            initiallyReadTableMetadataByRef.get(readTable).ref(readTable.ref());

        if (null != snapshotRef
            && null != snapshotRefInsideTx
            && snapshotRef.snapshotId() != snapshotRefInsideTx.snapshotId()) {
          throw new ValidationException(
              "%s isolation violation: Found table metadata updates to table '%s' after it was read on branch '%s'",
              isolationLevel(), readTable.identifier().toString(), readTable.ref());
        }

        if (null == snapshotRef || null == snapshotRefInsideTx) {
          TableMetadata currentTableMetadata = table.operations().current();

          if (!currentTableMetadata
              .metadataFileLocation()
              .equals(initiallyReadTableMetadataByRef.get(readTable).metadataFileLocation())) {
            throw new ValidationException(
                "%s isolation violation: Found table metadata updates to table '%s' after it was read",
                isolationLevel(), readTable.identifier());
          }
        }
      }
    }
  }

  private void rollback() {
    txByTable.clear();
    initiallyReadTableMetadataByRef.clear();
    initiallyReadTables.clear();
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

  /** We need to keep track of the tables that we read inside the TX to prevent read skew */
  private Optional<Table> initiallyReadTable(TableIdentifier identifier) {
    if (initiallyReadTables.containsKey(identifier)) {
      return Optional.ofNullable(initiallyReadTables.get(identifier));
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
          BaseCatalogTransaction.this
              .txTable(identifier)
              .orElseGet(
                  () ->
                      initiallyReadTable(identifier)
                          .orElseGet(
                              () -> {
                                Table loadTable = origin.loadTable(identifier);

                                initiallyReadTables.computeIfAbsent(identifier, ident -> loadTable);

                                // remember the very first version of table metadata that was read
                                if (IsolationLevel.SERIALIZABLE == isolationLevel()) {
                                  initiallyReadTableMetadataByRef.computeIfAbsent(
                                      ImmutableTableRef.builder()
                                          .identifier(identifier)
                                          .ref(SnapshotRef.MAIN_BRANCH)
                                          .build(),
                                      ident -> opsFromTable(loadTable).current());
                                }

                                return loadTable;
                              }));

      return new TransactionalTable(table, opsFromTable(table));
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

  private static TableOperations opsFromTable(Table table) {
    return table instanceof BaseTransaction.TransactionTable
        ? ((BaseTransaction.TransactionTable) table).operations()
        : ((BaseTable) table).operations();
  }

  private class TransactionalTable extends BaseTable {
    private final Table table;

    private TransactionalTable(Table table, TableOperations ops) {
      super(ops, table.name());
      this.table = table;
    }

    @Override
    public TableScan newScan() {
      TableScan tableScan = super.newScan();
      if (tableScan instanceof DataTableScan) {
        return new TransactionalTableScan((DataTableScan) tableScan);
      }

      return tableScan;
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

  private class TransactionalTableScan extends DataTableScan {
    protected TransactionalTableScan(DataTableScan delegate) {
      super(delegate.table(), delegate.schema(), delegate.context());
    }

    @Override
    public TableScan useRef(String name) {
      DataTableScan tableScan = (DataTableScan) super.useRef(name);

      if (IsolationLevel.SERIALIZABLE == isolationLevel()) {
        // store which version of the table on the given branch we read the first time
        initiallyReadTableMetadataByRef.computeIfAbsent(
            ImmutableTableRef.builder()
                .identifier(identifierWithoutCatalog(table().name()))
                .ref(name)
                .build(),
            ident -> opsFromTable(table()).current());
      }

      return tableScan;
    }
  }

  @Value.Immutable
  interface TableRef {
    TableIdentifier identifier();

    String ref();

    @Value.Lazy
    default String name() {
      return identifier().toString() + "@" + ref();
    }
  }
}
