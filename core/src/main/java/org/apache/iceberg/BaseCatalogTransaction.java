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
package org.apache.iceberg;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.BaseTransaction.TransactionTable;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.CatalogTransaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Tasks;

public abstract class BaseCatalogTransaction implements CatalogTransaction {
  private final Map<TableIdentifier, Transaction> txByTable;
  private final Map<TableIdentifier, TableMetadata> initiallyReadTableMetadata;
  private final IsolationLevel isolationLevel;
  private final BaseMetastoreCatalog origin;
  private boolean hasCommitted = false;

  public BaseCatalogTransaction(BaseMetastoreCatalog origin, IsolationLevel isolationLevel) {
    Preconditions.checkArgument(null != origin, "Invalid origin catalog: null");
    Preconditions.checkArgument(null != isolationLevel, "Invalid isolation level: null");
    this.origin = origin;
    this.isolationLevel = isolationLevel;
    this.txByTable = Maps.newConcurrentMap();
    this.initiallyReadTableMetadata = Maps.newConcurrentMap();
  }

  @Override
  public void rollback() {
    Tasks.foreach(txByTable.values()).run(Transaction::rollback);
    txByTable.clear();
    initiallyReadTableMetadata.clear();
  }

  protected Map<TableIdentifier, Transaction> txByTable() {
    return txByTable;
  }

  protected Map<TableIdentifier, TableMetadata> initiallyReadTableMetadata() {
    return initiallyReadTableMetadata;
  }

  protected Catalog origin() {
    return origin;
  }

  protected boolean hasCommitted() {
    return hasCommitted;
  }

  public void setHasCommitted(boolean hasCommitted) {
    this.hasCommitted = hasCommitted;
  }

  private TableIdentifier identifierWithoutCatalog(String tableWithCatalog) {
    if (tableWithCatalog.startsWith(origin.name())) {
      return TableIdentifier.parse(tableWithCatalog.replace(origin.name() + ".", ""));
    }
    return TableIdentifier.parse(tableWithCatalog);
  }

  @Override
  public Catalog asCatalog() {
    return new AsTransactionalCatalog();
  }

  private Optional<Table> txTable(TableIdentifier identifier) {
    if (txByTable.containsKey(identifier)) {
      return Optional.ofNullable(txByTable.get(identifier).table());
    }

    return Optional.empty();
  }

  private Transaction txForTable(Table table) {
    return txByTable.computeIfAbsent(
        identifierWithoutCatalog(table.name()),
        k -> {
          TableOperations operations = ((HasTableOperations) table).operations();
          return Transactions.newTransaction(table.name(), operations);
        });
  }

  @Override
  public IsolationLevel isolationLevel() {
    return isolationLevel;
  }

  public class AsTransactionalCatalog extends BaseMetastoreCatalog {
    @Override
    public Table loadTable(TableIdentifier identifier) {
      Table table =
          BaseCatalogTransaction.this
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
          table instanceof TransactionTable
              ? ((TransactionTable) table).operations()
              : ((BaseTable) table).operations();
      return new TransactionalTable(table, tableOps);
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
      Optional<Table> txTable = BaseCatalogTransaction.this.txTable(tableIdentifier);
      if (txTable.isPresent()) {
        return ((TransactionTable) txTable.get()).operations();
      }
      return origin.newTableOps(tableIdentifier);
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
      return origin.defaultWarehouseLocation(tableIdentifier);
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
