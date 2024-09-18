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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRevertDeleteProcedure extends ExtensionsTestBase {

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  private Table initTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    sql("DELETE FROM %s WHERE id = 1", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(spark.table(tableName).count()).isEqualTo(1);
    assertThat(table.snapshots()).hasSize(3);
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.DELETE);

    return table;
  }

  @TestTemplate
  public void testRevertDelete() {
    Table table = initTable();
    long snapshotId = table.currentSnapshot().snapshotId();

    List<Object[]> output =
        sql("CALL %s.system.revert_delete('%s', %d)", catalogName, tableIdent, snapshotId);

    assertThat(output.get(0)[0]).isEqualTo(1L);

    // check that the data was added back...
    table.refresh();
    assertThat(spark.table(tableName).count()).isEqualTo(2);
    assertThat(table.snapshots()).hasSize(4);
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.APPEND);

    // try to revert the same delete, this should fail...
    assertThatThrownBy(
            () ->
                sql("CALL %s.system.revert_delete('%s', %d)", catalogName, tableIdent, snapshotId))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageEndingWith("has already been reverted");

    // make sure nothing changed...
    table.refresh();
    assertThat(spark.table(tableName).count()).isEqualTo(2);
    assertThat(table.snapshots()).hasSize(4);
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.APPEND);
  }

  @TestTemplate
  public void testRevertDeleteDryRun() {
    Table table = initTable();

    List<Object[]> output =
        sql(
            "CALL %s.system.revert_delete('%s', %d, true)",
            catalogName, tableIdent, table.currentSnapshot().snapshotId());

    assertThat(output.get(0)[0]).isEqualTo(1L);

    // make sure nothing changed...
    table.refresh();
    assertThat(spark.table(tableName).count()).isEqualTo(1);
    assertThat(table.snapshots()).hasSize(3);
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.DELETE);
  }

  @TestTemplate
  public void testRevertDeleteInvalidSnapshot() {
    Table table = initTable();
    Snapshot firstSnapshot = table.snapshots().iterator().next();

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.revert_delete('%s', %d)",
                    catalogName, tableIdent, firstSnapshot.snapshotId()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageEndingWith("is not a delete operation");

    // make sure nothing changed...
    table.refresh();
    assertThat(spark.table(tableName).count()).isEqualTo(1);
    assertThat(table.snapshots()).hasSize(3);
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.DELETE);
  }

  @TestTemplate
  public void testRevertDeleteMissingFile() {
    Table table = initTable();
    Snapshot firstSnapshot = table.snapshots().iterator().next();
    String filePath = firstSnapshot.addedDataFiles(table.io()).iterator().next().path().toString();
    table.io().deleteFile(filePath);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.revert_delete('%s', %d)",
                    catalogName, tableIdent, table.currentSnapshot().snapshotId()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageEndingWith("does not exist");

    // make sure nothing changed...
    table.refresh();
    assertThat(spark.table(tableName).count()).isEqualTo(1);
    assertThat(table.snapshots()).hasSize(3);
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.DELETE);
  }

  @TestTemplate
  public void testRevertDeleteMergeOnRead() throws Exception {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg"
            + " TBLPROPERTIES('write.delete.mode'='merge-on-read')",
        tableName);

    // append 2 rows in a single data file, then delete one row...
    spark
        .sql("SELECT 1 id, 'a' data UNION ALL SELECT 2 id, 'b' data")
        .coalesce(1)
        .writeTo(tableName)
        .append();
    sql("DELETE FROM %s WHERE id = 1", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(spark.table(tableName).count()).isEqualTo(1);
    assertThat(table.snapshots()).hasSize(2);
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.DELETE);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.revert_delete('%s', %d)",
                    catalogName, tableIdent, table.currentSnapshot().snapshotId()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageEndingWith("is not supported");

    // make sure nothing changed...
    table.refresh();
    assertThat(spark.table(tableName).count()).isEqualTo(1);
    assertThat(table.snapshots()).hasSize(2);
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.DELETE);
  }

  @TestTemplate
  public void testRevertStagedDelete() throws Exception {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    table.newDelete().stageOnly().deleteFromRowFilter(Expressions.alwaysTrue()).commit();
    sql("REFRESH TABLE %s", tableName);

    assertThat(spark.table(tableName).count()).isEqualTo(1);
    assertThat(table.snapshots()).hasSize(2);
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.APPEND);

    long deleteSnapshotId = findDeleteSnapshot(table).snapshotId();

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.revert_delete('%s', %d)",
                    catalogName, tableIdent, deleteSnapshotId))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageEndingWith("not an ancestor of the current table state");

    // make sure nothing changed...
    table.refresh();
    assertThat(spark.table(tableName).count()).isEqualTo(1);
    assertThat(table.snapshots()).hasSize(2);
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.APPEND);
  }

  private Snapshot findDeleteSnapshot(Table table) {
    for (Snapshot snapshot : table.snapshots()) {
      if (snapshot.operation().equals(DataOperations.DELETE)) {
        return snapshot;
      }
    }
    throw new RuntimeException("Delete snapshot not found");
  }
}
