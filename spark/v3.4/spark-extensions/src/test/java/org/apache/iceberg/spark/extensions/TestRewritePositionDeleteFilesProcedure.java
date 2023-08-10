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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Encoders;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestRewritePositionDeleteFilesProcedure extends SparkExtensionsTestBase {

  public TestRewritePositionDeleteFilesProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

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
            new SimpleRecord(2, "c"),
            new SimpleRecord(2, "d"),
            new SimpleRecord(3, "e"),
            new SimpleRecord(3, "f"),
            new SimpleRecord(4, "g"),
            new SimpleRecord(4, "h"),
            new SimpleRecord(5, "i"),
            new SimpleRecord(5, "j"),
            new SimpleRecord(6, "k"),
            new SimpleRecord(6, "l"));
    spark
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testExpireDeleteFilesAll() throws Exception {
    createTable();

    sql("DELETE FROM %s WHERE id=1", tableName);
    sql("DELETE FROM %s WHERE id=2", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals(2, TestHelpers.deleteFiles(table).size());

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

    Assert.assertEquals(1, TestHelpers.deleteFiles(table).size());
  }

  @Test
  public void testExpireDeleteFilesNoOption() throws Exception {
    createTable();

    sql("DELETE FROM %s WHERE id=1", tableName);
    sql("DELETE FROM %s WHERE id=2", tableName);
    sql("DELETE FROM %s WHERE id=3", tableName);
    sql("DELETE FROM %s WHERE id=4", tableName);
    sql("DELETE FROM %s WHERE id=5", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals(5, TestHelpers.deleteFiles(table).size());

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

  @Test
  public void testExpireDeleteFilesFilter() throws Exception {
    createTable(true);

    sql("DELETE FROM %s WHERE data='a'", tableName);
    sql("DELETE FROM %s WHERE data='c'", tableName);
    sql("DELETE FROM %s WHERE data='e'", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals(3, TestHelpers.deleteFiles(table).size());

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
        "Should delete 2 delete files and add 2",
        ImmutableList.of(
            row(
                2,
                2,
                Long.valueOf(snapshotSummary.get(REMOVED_FILE_SIZE_PROP)),
                Long.valueOf(snapshotSummary.get(ADDED_FILE_SIZE_PROP)))),
        output);

    Assert.assertEquals(3, TestHelpers.deleteFiles(table).size());
  }

  @Test
  public void testInvalidOption() throws Exception {
    createTable();

    Assert.assertThrows(
        "Cannot use options [foo], they are not supported by the action or the rewriter BIN-PACK",
        IllegalArgumentException.class,
        () ->
            sql(
                "CALL %s.system.rewrite_position_delete_files("
                    + "table => '%s',"
                    + "options => map("
                    + "'foo', 'bar'))",
                catalogName, tableIdent));
  }

  private Map<String, String> snapshotSummary() {
    return validationCatalog.loadTable(tableIdent).currentSnapshot().summary();
  }
}
