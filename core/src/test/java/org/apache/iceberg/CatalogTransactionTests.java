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

import static org.apache.iceberg.catalog.CatalogTransaction.IsolationLevel.SERIALIZABLE;
import static org.apache.iceberg.catalog.CatalogTransaction.IsolationLevel.SNAPSHOT;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.CatalogTransaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsCatalogTransactions;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public abstract class CatalogTransactionTests<
    C extends SupportsCatalogTransactions & SupportsNamespaces & Catalog> {

  @TempDir protected Path metadataDir;

  protected static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get()), required(4, "data", Types.StringType.get()));

  // Partition spec used to create tables
  protected static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  protected static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=0") // easy way to set partition data for now
          .withRecordCount(1)
          .build();
  protected static final DataFile FILE_B =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=1") // easy way to set partition data for now
          .withRecordCount(1)
          .build();
  protected static final DataFile FILE_C =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-c.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=2") // easy way to set partition data for now
          .withRecordCount(1)
          .build();
  protected static final DataFile FILE_D =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-d.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=3") // easy way to set partition data for now
          .withRecordCount(1)
          .build();

  protected abstract C catalog();

  @Test
  public void catalogTxWithSingleOp() {
    catalogTxWithSingleOp(CatalogTransaction.IsolationLevel.SNAPSHOT);
  }

  @Test
  public void catalogTxWithSingleOpWithSerializable() {
    catalogTxWithSingleOp(SERIALIZABLE);
  }

  private void catalogTxWithSingleOp(CatalogTransaction.IsolationLevel isolationLevel) {
    TableIdentifier identifier = TableIdentifier.of("ns", "tx-with-single-op");
    catalog().createTable(identifier, SCHEMA, SPEC);

    Table one = catalog().loadTable(identifier);
    TableMetadata base = ((BaseTable) one).operations().current();

    CatalogTransaction catalogTransaction = catalog().startTransaction(isolationLevel);
    Catalog txCatalog = catalogTransaction.asCatalog();
    txCatalog.loadTable(identifier).newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(base).isSameAs(((BaseTable) one).operations().refresh());
    assertThat(base.currentSnapshot()).isNull();

    catalogTransaction.commitTransaction();

    TableMetadata updated = ((BaseTable) one).operations().refresh();
    assertThat(base).isNotSameAs(updated);
    assertThat(base.lastUpdatedMillis()).isLessThan(updated.lastUpdatedMillis());

    assertThat(updated.currentSnapshot().addedDataFiles(catalog().loadTable(identifier).io()))
        .hasSize(2);
  }

  @Test
  public void txAgainstMultipleTables() {
    txAgainstMultipleTables(SNAPSHOT);
  }

  @Test
  public void txAgainstMultipleTablesWithSerializable() {
    txAgainstMultipleTables(SERIALIZABLE);
  }

  private void txAgainstMultipleTables(CatalogTransaction.IsolationLevel isolationLevel) {
    List<String> tables = Arrays.asList("a", "b", "c");
    for (String tbl : tables) {
      catalog().createTable(TableIdentifier.of("ns", tbl), SCHEMA);
    }

    TableIdentifier first = TableIdentifier.of("ns", "a");
    TableIdentifier second = TableIdentifier.of("ns", "b");
    TableIdentifier third = TableIdentifier.of("ns", "c");
    Table one = catalog().loadTable(first);
    Table two = catalog().loadTable(second);
    Table three = catalog().loadTable(third);

    TableMetadata baseMetadataOne = ((BaseTable) one).operations().current();
    TableMetadata baseMetadataTwo = ((BaseTable) two).operations().current();
    TableMetadata baseMetadataThree = ((BaseTable) three).operations().current();

    CatalogTransaction catalogTransaction = catalog().startTransaction(isolationLevel);
    Catalog txCatalog = catalogTransaction.asCatalog();

    txCatalog.loadTable(first).newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    assertThat(baseMetadataOne).isSameAs(((BaseTable) one).operations().refresh());

    txCatalog.loadTable(second).newDelete().deleteFile(FILE_C).commit();
    txCatalog.loadTable(second).newFastAppend().appendFile(FILE_B).appendFile(FILE_C).commit();
    assertThat(baseMetadataTwo).isSameAs(((BaseTable) two).operations().refresh());

    txCatalog.loadTable(third).newDelete().deleteFile(FILE_A).commit();
    txCatalog.loadTable(third).newAppend().appendFile(FILE_D).commit();

    assertThat(baseMetadataOne).isSameAs(((BaseTable) one).operations().refresh());
    assertThat(baseMetadataTwo).isSameAs(((BaseTable) two).operations().refresh());
    assertThat(baseMetadataThree).isSameAs(((BaseTable) three).operations().refresh());

    for (String tbl : tables) {
      TableMetadata current =
          ((BaseTable) catalog().loadTable(TableIdentifier.of("ns", tbl))).operations().refresh();
      assertThat(current.snapshots()).isEmpty();
    }

    catalogTransaction.commitTransaction();

    for (String tbl : tables) {
      TableMetadata current =
          ((BaseTable) catalog().loadTable(TableIdentifier.of("ns", tbl))).operations().refresh();
      assertThat(current.snapshots()).hasSizeGreaterThanOrEqualTo(1);
    }

    one = catalog().loadTable(first);
    two = catalog().loadTable(second);
    three = catalog().loadTable(third);
    assertThat(one.currentSnapshot().allManifests(one.io())).hasSize(1);
    assertThat(two.currentSnapshot().allManifests(two.io())).hasSize(1);
    assertThat(three.currentSnapshot().allManifests(three.io())).hasSize(1);

    assertThat(one.currentSnapshot().addedDataFiles(one.io())).hasSize(2);
    assertThat(two.currentSnapshot().addedDataFiles(two.io())).hasSize(2);
    assertThat(three.currentSnapshot().addedDataFiles(three.io())).hasSize(1);
  }

  @Test
  public void txAgainstMultipleTablesLastOneSchemaConflict() {
    txAgainstMultipleTablesLastOneSchemaConflict(SNAPSHOT);
  }

  @Test
  public void txAgainstMultipleTablesLastOneSchemaConflictWithSerializable() {
    txAgainstMultipleTablesLastOneSchemaConflict(SERIALIZABLE);
  }

  private void txAgainstMultipleTablesLastOneSchemaConflict(
      CatalogTransaction.IsolationLevel isolationLevel) {
    for (String tbl : Arrays.asList("a", "b", "c")) {
      catalog().createTable(TableIdentifier.of("ns", tbl), SCHEMA);
    }

    TableIdentifier first = TableIdentifier.of("ns", "a");
    TableIdentifier second = TableIdentifier.of("ns", "b");
    TableIdentifier third = TableIdentifier.of("ns", "c");
    Table one = catalog().loadTable(first);
    Table two = catalog().loadTable(second);
    Table three = catalog().loadTable(third);

    TableMetadata baseMetadataOne = ((BaseTable) one).operations().current();
    TableMetadata baseMetadataTwo = ((BaseTable) two).operations().current();
    TableMetadata baseMetadataThree = ((BaseTable) three).operations().current();

    CatalogTransaction catalogTransaction = catalog().startTransaction(isolationLevel);
    Catalog txCatalog = catalogTransaction.asCatalog();
    txCatalog.loadTable(first).newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    assertThat(baseMetadataOne).isSameAs(((BaseTable) one).operations().refresh());

    txCatalog.loadTable(second).newFastAppend().appendFile(FILE_B).appendFile(FILE_C).commit();
    txCatalog.loadTable(second).newDelete().deleteFile(FILE_C).commit();
    assertThat(baseMetadataTwo).isSameAs(((BaseTable) two).operations().refresh());

    txCatalog.loadTable(third).newDelete().deleteFile(FILE_A).commit();
    txCatalog.loadTable(third).newAppend().appendFile(FILE_D).commit();

    txCatalog.loadTable(third).updateSchema().renameColumn("data", "new-column").commit();

    assertThat(baseMetadataThree).isSameAs(((BaseTable) three).operations().refresh());

    // delete the colum we're trying to rename in the catalog TX
    three.updateSchema().deleteColumn("data").commit();

    assertThat(baseMetadataOne).isSameAs(((BaseTable) one).operations().refresh());
    assertThat(baseMetadataTwo).isSameAs(((BaseTable) two).operations().refresh());
    assertThat(baseMetadataThree).isNotSameAs(((BaseTable) three).operations().refresh());
    assertThat(((BaseTable) three).operations().refresh().schema().findField("data")).isNull();

    if (SERIALIZABLE == isolationLevel) {
      Assertions.assertThatThrownBy(catalogTransaction::commitTransaction)
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining(
              "SERIALIZABLE isolation violation: Found table metadata updates to table 'ns.c' after it was read");
    } else {
      String failureMsg = "Requirement failed: current schema changed: expected id 0 != 1";
      if (catalog() instanceof JdbcCatalog) {
        failureMsg = "Table metadata refresh is required";
      }

      Assertions.assertThatThrownBy(catalogTransaction::commitTransaction)
          .isInstanceOf(CommitFailedException.class)
          .hasMessageContaining(failureMsg);
    }

    if (catalog() instanceof JdbcCatalog) {
      // FIXME: partial updates are being applied due to how SQLite manages nested transactions and
      // how isolation is determined among connections and transactions
      // that means that we might not want to do a POC for JdbcCatalogTransaction
    } else {
      assertThat(baseMetadataOne).isSameAs(((BaseTable) one).operations().refresh());
      assertThat(baseMetadataTwo).isSameAs(((BaseTable) two).operations().refresh());
      assertThat(baseMetadataThree).isNotSameAs(((BaseTable) three).operations().refresh());
      assertThat(((BaseTable) three).operations().refresh().schema().findField("new-column"))
          .isNull();
      assertThat(((BaseTable) three).operations().refresh().schema().findField("data")).isNull();
      assertThat(((BaseTable) three).operations().refresh().schema().columns()).hasSize(1);
    }
  }

  @Test
  public void txAgainstMultipleTablesLastOneFails() {
    txAgainstMultipleTablesLastOneFails(SNAPSHOT);
  }

  @Test
  public void txAgainstMultipleTablesLastOneFailsWithSerializable() {
    txAgainstMultipleTablesLastOneFails(SERIALIZABLE);
  }

  private void txAgainstMultipleTablesLastOneFails(
      CatalogTransaction.IsolationLevel isolationLevel) {
    for (String tbl : Arrays.asList("a", "b", "c")) {
      catalog().createTable(TableIdentifier.of("ns", tbl), SCHEMA);
    }

    TableIdentifier first = TableIdentifier.of("ns", "a");
    TableIdentifier second = TableIdentifier.of("ns", "b");
    TableIdentifier third = TableIdentifier.of("ns", "c");
    Table one = catalog().loadTable(first);
    Table two = catalog().loadTable(second);
    Table three = catalog().loadTable(third);

    TableMetadata baseMetadataOne = ((BaseTable) one).operations().current();
    TableMetadata baseMetadataTwo = ((BaseTable) two).operations().current();
    TableMetadata baseMetadataThree = ((BaseTable) three).operations().current();

    CatalogTransaction catalogTransaction = catalog().startTransaction(isolationLevel);
    Catalog txCatalog = catalogTransaction.asCatalog();
    txCatalog.loadTable(first).newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    assertThat(baseMetadataOne).isSameAs(((BaseTable) one).operations().refresh());

    txCatalog.loadTable(second).newFastAppend().appendFile(FILE_B).appendFile(FILE_C).commit();
    txCatalog.loadTable(second).newDelete().deleteFile(FILE_C).commit();
    assertThat(baseMetadataTwo).isSameAs(((BaseTable) two).operations().refresh());

    txCatalog.loadTable(third).newDelete().deleteFile(FILE_A).commit();
    txCatalog.loadTable(third).newAppend().appendFile(FILE_D).commit();

    assertThat(baseMetadataThree).isSameAs(((BaseTable) three).operations().refresh());

    // perform updates outside the catalog TX
    three.newAppend().appendFile(FILE_A).appendFile(FILE_D).commit();
    Snapshot snapshot = ((BaseTable) three).operations().refresh().currentSnapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.summary().get(SnapshotSummary.ADDED_FILES_PROP)).isEqualTo("2");
    assertThat(snapshot.summary().get(SnapshotSummary.ADDED_RECORDS_PROP)).isEqualTo("2");

    assertThat(baseMetadataOne).isSameAs(((BaseTable) one).operations().refresh());
    assertThat(baseMetadataTwo).isSameAs(((BaseTable) two).operations().refresh());
    assertThat(baseMetadataThree).isNotSameAs(((BaseTable) three).operations().refresh());

    if (SERIALIZABLE == isolationLevel) {
      Assertions.assertThatThrownBy(catalogTransaction::commitTransaction)
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining(
              "SERIALIZABLE isolation violation: Found table metadata updates to table 'ns.c' after it was read");
    } else {
      if (catalog() instanceof JdbcCatalog) {
        // FIXME: why does this not fail for SQLite?
        catalogTransaction.commitTransaction();
      } else {
        Assertions.assertThatThrownBy(catalogTransaction::commitTransaction)
            .isInstanceOf(CommitFailedException.class)
            .hasMessageContaining("Requirement failed: branch main was created concurrently");
      }
    }

    if (catalog() instanceof JdbcCatalog) {
      // FIXME: why does the commit not fail with SQLite?
    } else {
      // the third update in the catalog TX fails, so we need to make sure that all changes from the
      // catalog TX are rolled back
      assertThat(baseMetadataOne).isSameAs(((BaseTable) one).operations().refresh());
      assertThat(baseMetadataTwo).isSameAs(((BaseTable) two).operations().refresh());
      assertThat(baseMetadataThree).isNotSameAs(((BaseTable) three).operations().refresh());

      assertThat(((BaseTable) one).operations().refresh().currentSnapshot()).isNull();
      assertThat(((BaseTable) two).operations().refresh().currentSnapshot()).isNull();
      assertThat(((BaseTable) three).operations().refresh().currentSnapshot()).isEqualTo(snapshot);
    }
  }

  @Test
  public void schemaUpdateVisibility() {
    schemaUpdateVisibility(CatalogTransaction.IsolationLevel.SNAPSHOT);
  }

  @Test
  public void schemaUpdateVisibilityWithSerializable() {
    schemaUpdateVisibility(SERIALIZABLE);
  }

  private void schemaUpdateVisibility(CatalogTransaction.IsolationLevel isolationLevel) {
    Namespace namespace = Namespace.of("test");
    TableIdentifier identifier = TableIdentifier.of(namespace, "table");

    catalog().createNamespace(namespace);
    catalog().createTable(identifier, SCHEMA);
    assertThat(catalog().tableExists(identifier)).isTrue();

    CatalogTransaction catalogTx = catalog().startTransaction(isolationLevel);
    Catalog txCatalog = catalogTx.asCatalog();

    String column = "new_col";

    assertThat(txCatalog.loadTable(identifier).schema().findField(column)).isNull();
    txCatalog
        .loadTable(identifier)
        .updateSchema()
        .addColumn(column, Types.BooleanType.get())
        .commit();

    // changes inside the catalog TX should be visible
    assertThat(txCatalog.loadTable(identifier).schema().findField(column)).isNotNull();

    // changes outside the catalog TX should not be visible
    assertThat(catalog().loadTable(identifier).schema().findField(column)).isNull();

    catalogTx.commitTransaction();

    assertThat(catalog().loadTable(identifier).schema().findField(column)).isNotNull();
    assertThat(txCatalog.loadTable(identifier).schema().findField(column)).isNotNull();
  }

  @Test
  public void readTableAfterLoadTableInsideTx() {
    readTableAfterLoadTableInsideTx(SNAPSHOT);
  }

  @Test
  public void readTableAfterLoadTableInsideTxWithSerializable() {
    readTableAfterLoadTableInsideTx(SERIALIZABLE);
  }

  private void readTableAfterLoadTableInsideTx(CatalogTransaction.IsolationLevel isolationLevel) {
    for (String tbl : Arrays.asList("a", "b")) {
      catalog().createTable(TableIdentifier.of("ns", tbl), SCHEMA);
    }

    TableIdentifier first = TableIdentifier.of("ns", "a");
    TableIdentifier second = TableIdentifier.of("ns", "b");
    Table two = catalog().loadTable(second);

    CatalogTransaction catalogTransaction = catalog().startTransaction(isolationLevel);
    Catalog txCatalog = catalogTransaction.asCatalog();
    txCatalog.loadTable(first).newAppend().appendFile(FILE_A).appendFile(FILE_D).commit();
    assertThat(Iterables.size(txCatalog.loadTable(first).newScan().planFiles())).isEqualTo(2);
    assertThat(Iterables.size(txCatalog.loadTable(second).newScan().planFiles())).isEqualTo(0);

    two.newFastAppend().appendFile(FILE_B).appendFile(FILE_C).appendFile(FILE_D).commit();

    // this should not be allowed with SERIALIZABLE after the table has been already read
    // within the catalog TX, but is allowed with SNAPSHOT
    assertThat(Iterables.size(txCatalog.loadTable(second).newScan().planFiles())).isEqualTo(3);

    if (SERIALIZABLE == isolationLevel) {
      Assertions.assertThatThrownBy(catalogTransaction::commitTransaction)
          .isInstanceOf(ValidationException.class)
          .hasMessage(
              "SERIALIZABLE isolation violation: Found table metadata updates to table 'ns.b' after it was read");

      assertThat(Iterables.size(catalog().loadTable(first).newScan().planFiles())).isEqualTo(0);
      assertThat(Iterables.size(catalog().loadTable(second).newScan().planFiles())).isEqualTo(3);
    } else {
      catalogTransaction.commitTransaction();

      assertThat(Iterables.size(catalog().loadTable(first).newScan().planFiles())).isEqualTo(2);
      assertThat(Iterables.size(catalog().loadTable(second).newScan().planFiles())).isEqualTo(3);
    }
  }

  @Test
  public void concurrentTx() {
    concurrentTx(SNAPSHOT);
  }

  @Test
  public void concurrentTxWithSerializable() {
    concurrentTx(SERIALIZABLE);
  }

  private void concurrentTx(CatalogTransaction.IsolationLevel isolationLevel) {
    TableIdentifier identifier = TableIdentifier.of("ns", "tbl");
    catalog().createTable(identifier, SCHEMA);
    Table one = catalog().loadTable(identifier);

    CatalogTransaction catalogTransaction = catalog().startTransaction(isolationLevel);
    Catalog txCatalog = catalogTransaction.asCatalog();

    // perform updates outside catalog TX but before table has been read inside the catalog TX
    one.newAppend().appendFile(FILE_A).appendFile(FILE_D).commit();

    Snapshot snapshot = ((BaseTable) one).operations().refresh().currentSnapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.summary().get(SnapshotSummary.ADDED_FILES_PROP)).isEqualTo("2");
    assertThat(snapshot.summary().get(SnapshotSummary.ADDED_RECORDS_PROP)).isEqualTo("2");
    assertThat(snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP)).isEqualTo("2");

    // this should not fail with any isolation level
    txCatalog.loadTable(identifier).newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    catalogTransaction.commitTransaction();

    snapshot = ((BaseTable) one).operations().refresh().currentSnapshot();
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.summary().get(SnapshotSummary.ADDED_FILES_PROP)).isEqualTo("2");
    assertThat(snapshot.summary().get(SnapshotSummary.ADDED_RECORDS_PROP)).isEqualTo("2");
    assertThat(snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP)).isEqualTo("4");
  }
}
