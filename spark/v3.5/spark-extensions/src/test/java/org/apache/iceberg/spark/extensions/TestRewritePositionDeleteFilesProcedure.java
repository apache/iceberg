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

import static org.apache.iceberg.SnapshotSummary.ADDED_FILE_SIZE_PROP;
import static org.apache.iceberg.SnapshotSummary.REMOVED_FILE_SIZE_PROP;
import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRewritePositionDeleteFilesProcedure extends ExtensionsTestBase {

  private void createTable() throws Exception {
    createTable(false);
  }

  private void createTable(boolean partitioned) throws Exception {
    String partitionStmt = partitioned ? "PARTITIONED BY (id)" : "";
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg %s TBLPROPERTIES"
            + "('format-version'='2', 'write.delete.mode'='merge-on-read')",
        tableName, partitionStmt);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(1, "b"),
            new SimpleRecord(1, "c"),
            new SimpleRecord(2, "d"),
            new SimpleRecord(2, "e"),
            new SimpleRecord(2, "f"),
            new SimpleRecord(3, "g"),
            new SimpleRecord(3, "h"),
            new SimpleRecord(3, "i"),
            new SimpleRecord(4, "j"),
            new SimpleRecord(4, "k"),
            new SimpleRecord(4, "l"),
            new SimpleRecord(5, "m"),
            new SimpleRecord(5, "n"),
            new SimpleRecord(5, "o"),
            new SimpleRecord(6, "p"),
            new SimpleRecord(6, "q"),
            new SimpleRecord(6, "r"));
    spark
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testExpireDeleteFilesAll() throws Exception {
    createTable();

    sql("DELETE FROM %s WHERE id=1", tableName);
    sql("DELETE FROM %s WHERE id=2", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(TestHelpers.deleteFiles(table)).hasSize(2);

    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_position_delete_files("
                + "table => '%s',"
                + "options => map("
                + "'rewrite-all','true'))",
            catalogName, tableIdent);
    table.refresh();

    Map<String, String> snapshotSummary = snapshotSummary();
    assertEquals(
        "Should delete 2 delete files and add 1",
        ImmutableList.of(
            row(
                2,
                1,
                Long.valueOf(snapshotSummary.get(REMOVED_FILE_SIZE_PROP)),
                Long.valueOf(snapshotSummary.get(ADDED_FILE_SIZE_PROP)))),
        output);

    assertThat(TestHelpers.deleteFiles(table)).hasSize(1);
  }

  @TestTemplate
  public void testExpireDeleteFilesNoOption() throws Exception {
    createTable();

    sql("DELETE FROM %s WHERE id=1", tableName);
    sql("DELETE FROM %s WHERE id=2", tableName);
    sql("DELETE FROM %s WHERE id=3", tableName);
    sql("DELETE FROM %s WHERE id=4", tableName);
    sql("DELETE FROM %s WHERE id=5", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(TestHelpers.deleteFiles(table)).hasSize(5);

    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_position_delete_files(" + "table => '%s')",
            catalogName, tableIdent);
    table.refresh();

    Map<String, String> snapshotSummary = snapshotSummary();
    assertEquals(
        "Should replace 5 delete files with 1",
        ImmutableList.of(
            row(
                5,
                1,
                Long.valueOf(snapshotSummary.get(REMOVED_FILE_SIZE_PROP)),
                Long.valueOf(snapshotSummary.get(ADDED_FILE_SIZE_PROP)))),
        output);
  }

  @TestTemplate
  public void testExpireDeleteFilesFilter() throws Exception {
    createTable(true);

    sql("DELETE FROM %s WHERE id = 1 and data='a'", tableName);
    sql("DELETE FROM %s WHERE id = 1 and data='b'", tableName);
    sql("DELETE FROM %s WHERE id = 2 and data='d'", tableName);
    sql("DELETE FROM %s WHERE id = 2 and data='e'", tableName);
    sql("DELETE FROM %s WHERE id = 3 and data='g'", tableName);
    sql("DELETE FROM %s WHERE id = 3 and data='h'", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(TestHelpers.deleteFiles(table)).hasSize(6);

    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_position_delete_files("
                + "table => '%s',"
                // data filter is ignored as it cannot be applied to position deletes
                + "where => 'id IN (1, 2) AND data=\"bar\"',"
                + "options => map("
                + "'rewrite-all','true'))",
            catalogName, tableIdent);
    table.refresh();

    Map<String, String> snapshotSummary = snapshotSummary();
    assertEquals(
        "Should delete 4 delete files and add 2",
        ImmutableList.of(
            row(
                4,
                2,
                Long.valueOf(snapshotSummary.get(REMOVED_FILE_SIZE_PROP)),
                Long.valueOf(snapshotSummary.get(ADDED_FILE_SIZE_PROP)))),
        output);

    assertThat(TestHelpers.deleteFiles(table)).hasSize(4);
  }

  @TestTemplate
  public void testInvalidOption() throws Exception {
    createTable();

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_position_delete_files("
                        + "table => '%s',"
                        + "options => map("
                        + "'foo', 'bar'))",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot use options [foo], they are not supported by the action or the rewriter BIN-PACK");
  }

  @TestTemplate
  public void testRewriteWithUntranslatedOrUnconvertedFilter() throws Exception {
    createTable();
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_position_delete_files(table => '%s', where => 'substr(encode(data, \"utf-8\"), 2) = \"fo\"')",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot translate Spark expression");

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rewrite_position_delete_files(table => '%s', where => 'substr(data, 2) = \"fo\"')",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot convert Spark filter");
  }

  @TestTemplate
  public void testRewriteForBigTables() throws Exception {
    final String dbTable = "big_mor_table";
    final int numColumns = 1010;

    // Creating column names
    List<String> columnNames = new ArrayList<>();
    for (int i = 1; i <= numColumns; i++) {
      columnNames.add("col" + i);
    }

    // Create 5 rows
    List<Row> data = new ArrayList<>();
    for (int i = 1; i <= 5; i++) {
      Object[] row = new Object[numColumns];
      row[0] = 1;
      for (int j = 1; j < numColumns; j++) {
        row[j] = i;
      }
      data.add(RowFactory.create(row));
    }

    // Creating DataFrame
    Dataset<Row> dfWithRow =
        spark.createDataFrame(
            data,
            DataTypes.createStructType(
                columnNames.stream()
                    .map(name -> DataTypes.createStructField(name, DataTypes.IntegerType, true))
                    .collect(Collectors.toList())));

    dfWithRow.createOrReplaceTempView("table_input");

    // Creating table using Iceberg
    String createTableSql =
        String.format(
            "CREATE TABLE %s "
                + "USING iceberg "
                + "PARTITIONED BY (col1) "
                + "TBLPROPERTIES ( "
                + "'format-version'='2', "
                + "'write.delete.mode'='merge-on-read', "
                + "'write.update.mode'='merge-on-read', "
                + "'write.merge.mode'='merge-on-read', "
                + "'write.distribution-mode'='hash', "
                + "'write.delete.distribution-mode'='hash', "
                + "'write.update.distribution-mode'='hash', "
                + "'write.merge.distribution-mode'='hash' "
                + ") "
                + "AS SELECT * FROM table_input",
            tableName);

    spark.sql(createTableSql);

    // Merge operations
    for (int i = 1; i <= 2; i++) {
      String mergeSql =
          String.format(
              "MERGE INTO %s t "
                  + "USING (SELECT * FROM table_input WHERE col2 = %d) s "
                  + "ON t.col2 = s.col2 "
                  + "WHEN MATCHED THEN UPDATE SET * "
                  + "WHEN NOT MATCHED THEN INSERT *",
              tableName, i);

      spark.sql(mergeSql);
    }

    assertThatCode(() -> sql("UPDATE %s SET col2 = 6 WHERE col2 =1", tableName))
        .doesNotThrowAnyException();

    assertThatCode(
            () ->
                sql(
                    "CALL %s.system.rewrite_position_delete_files("
                        + "table => '%s',"
                        + "options => map('rewrite-all', 'true'))",
                    catalogName, tableName))
        .doesNotThrowAnyException();
  }

  private Map<String, String> snapshotSummary() {
    return validationCatalog.loadTable(tableIdent).currentSnapshot().summary();
  }
}
