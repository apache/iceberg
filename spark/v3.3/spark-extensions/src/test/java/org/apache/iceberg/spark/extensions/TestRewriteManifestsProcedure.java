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

import static org.apache.iceberg.TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestRewriteManifestsProcedure extends SparkExtensionsTestBase {

  public TestRewriteManifestsProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testRewriteManifestsInEmptyTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    List<Object[]> output = sql("CALL %s.system.rewrite_manifests('%s')", catalogName, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(0, 0)), output);
  }

  @Test
  public void testRewriteLargeManifests() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals(
        "Must have 1 manifest", 1, table.currentSnapshot().allManifests(table.io()).size());

    sql("ALTER TABLE %s SET TBLPROPERTIES ('commit.manifest.target-size-bytes' '1')", tableName);

    List<Object[]> output = sql("CALL %s.system.rewrite_manifests('%s')", catalogName, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(1, 4)), output);

    table.refresh();

    Assert.assertEquals(
        "Must have 4 manifests", 4, table.currentSnapshot().allManifests(table.io()).size());
  }

  @Test
  public void testRewriteManifestsNoOp() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals(
        "Must have 1 manifest", 1, table.currentSnapshot().allManifests(table.io()).size());

    List<Object[]> output = sql("CALL %s.system.rewrite_manifests('%s')", catalogName, tableIdent);
    // should not rewrite any manifests for no-op (output of rewrite is same as before and after)
    assertEquals("Procedure output must match", ImmutableList.of(row(0, 0)), output);

    table.refresh();

    Assert.assertEquals(
        "Must have 1 manifests", 1, table.currentSnapshot().allManifests(table.io()).size());
  }

  @Test
  public void testRewriteLargeManifestsOnDatePartitionedTableWithJava8APIEnabled() {
    withSQLConf(
        ImmutableMap.of("spark.sql.datetime.java8API.enabled", "true"),
        () -> {
          sql(
              "CREATE TABLE %s (id INTEGER, name STRING, dept STRING, ts DATE) USING iceberg PARTITIONED BY (ts)",
              tableName);
          try {
            spark
                .createDataFrame(
                    ImmutableList.of(
                        RowFactory.create(1, "John Doe", "hr", Date.valueOf("2021-01-01")),
                        RowFactory.create(2, "Jane Doe", "hr", Date.valueOf("2021-01-02")),
                        RowFactory.create(3, "Matt Doe", "hr", Date.valueOf("2021-01-03")),
                        RowFactory.create(4, "Will Doe", "facilities", Date.valueOf("2021-01-04"))),
                    spark.table(tableName).schema())
                .writeTo(tableName)
                .append();
          } catch (NoSuchTableException e) {
            // not possible as we already created the table above.
            throw new RuntimeException(e);
          }

          Table table = validationCatalog.loadTable(tableIdent);

          Assert.assertEquals(
              "Must have 1 manifest", 1, table.currentSnapshot().allManifests(table.io()).size());

          sql(
              "ALTER TABLE %s SET TBLPROPERTIES ('commit.manifest.target-size-bytes' '1')",
              tableName);

          List<Object[]> output =
              sql("CALL %s.system.rewrite_manifests('%s')", catalogName, tableIdent);
          assertEquals("Procedure output must match", ImmutableList.of(row(1, 4)), output);

          table.refresh();

          Assert.assertEquals(
              "Must have 4 manifests", 4, table.currentSnapshot().allManifests(table.io()).size());
        });
  }

  @Test
  public void testRewriteLargeManifestsOnTimestampPartitionedTableWithJava8APIEnabled() {
    withSQLConf(
        ImmutableMap.of("spark.sql.datetime.java8API.enabled", "true"),
        () -> {
          sql(
              "CREATE TABLE %s (id INTEGER, name STRING, dept STRING, ts TIMESTAMP) USING iceberg PARTITIONED BY (ts)",
              tableName);
          try {
            spark
                .createDataFrame(
                    ImmutableList.of(
                        RowFactory.create(
                            1, "John Doe", "hr", Timestamp.valueOf("2021-01-01 00:00:00")),
                        RowFactory.create(
                            2, "Jane Doe", "hr", Timestamp.valueOf("2021-01-02 00:00:00")),
                        RowFactory.create(
                            3, "Matt Doe", "hr", Timestamp.valueOf("2021-01-03 00:00:00")),
                        RowFactory.create(
                            4, "Will Doe", "facilities", Timestamp.valueOf("2021-01-04 00:00:00"))),
                    spark.table(tableName).schema())
                .writeTo(tableName)
                .append();
          } catch (NoSuchTableException e) {
            // not possible as we already created the table above.
            throw new RuntimeException(e);
          }

          Table table = validationCatalog.loadTable(tableIdent);

          Assert.assertEquals(
              "Must have 1 manifest", 1, table.currentSnapshot().allManifests(table.io()).size());

          sql(
              "ALTER TABLE %s SET TBLPROPERTIES ('commit.manifest.target-size-bytes' '1')",
              tableName);

          List<Object[]> output =
              sql("CALL %s.system.rewrite_manifests('%s')", catalogName, tableIdent);
          assertEquals("Procedure output must match", ImmutableList.of(row(1, 4)), output);

          table.refresh();

          Assert.assertEquals(
              "Must have 4 manifests", 4, table.currentSnapshot().allManifests(table.io()).size());
        });
  }

  @Test
  public void testRewriteSmallManifestsWithSnapshotIdInheritance() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' '%s')",
        tableName, SNAPSHOT_ID_INHERITANCE_ENABLED, "true");

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);
    sql("INSERT INTO TABLE %s VALUES (3, 'c')", tableName);
    sql("INSERT INTO TABLE %s VALUES (4, 'd')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals(
        "Must have 4 manifest", 4, table.currentSnapshot().allManifests(table.io()).size());

    List<Object[]> output =
        sql("CALL %s.system.rewrite_manifests(table => '%s')", catalogName, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(4, 1)), output);

    table.refresh();

    Assert.assertEquals(
        "Must have 1 manifests", 1, table.currentSnapshot().allManifests(table.io()).size());
  }

  @Test
  public void testRewriteSmallManifestsWithoutCaching() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals(
        "Must have 2 manifest", 2, table.currentSnapshot().allManifests(table.io()).size());

    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_manifests(use_caching => false, table => '%s')",
            catalogName, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(2, 1)), output);

    table.refresh();

    Assert.assertEquals(
        "Must have 1 manifests", 1, table.currentSnapshot().allManifests(table.io()).size());
  }

  @Test
  public void testRewriteManifestsCaseInsensitiveArgs() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals(
        "Must have 2 manifest", 2, table.currentSnapshot().allManifests(table.io()).size());

    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_manifests(usE_cAcHiNg => false, tAbLe => '%s')",
            catalogName, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(2, 1)), output);

    table.refresh();

    Assert.assertEquals(
        "Must have 1 manifests", 1, table.currentSnapshot().allManifests(table.io()).size());
  }

  @Test
  public void testInvalidRewriteManifestsCases() {
    AssertHelpers.assertThrows(
        "Should not allow mixed args",
        AnalysisException.class,
        "Named and positional arguments cannot be mixed",
        () -> sql("CALL %s.system.rewrite_manifests('n', table => 't')", catalogName));

    AssertHelpers.assertThrows(
        "Should not resolve procedures in arbitrary namespaces",
        NoSuchProcedureException.class,
        "not found",
        () -> sql("CALL %s.custom.rewrite_manifests('n', 't')", catalogName));

    AssertHelpers.assertThrows(
        "Should reject calls without all required args",
        AnalysisException.class,
        "Missing required parameters",
        () -> sql("CALL %s.system.rewrite_manifests()", catalogName));

    AssertHelpers.assertThrows(
        "Should reject calls with invalid arg types",
        AnalysisException.class,
        "Wrong arg type",
        () -> sql("CALL %s.system.rewrite_manifests('n', 2.2)", catalogName));

    AssertHelpers.assertThrows(
        "Should reject duplicate arg names name",
        AnalysisException.class,
        "Duplicate procedure argument: table",
        () -> sql("CALL %s.system.rewrite_manifests(table => 't', tAbLe => 't')", catalogName));

    AssertHelpers.assertThrows(
        "Should reject calls with empty table identifier",
        IllegalArgumentException.class,
        "Cannot handle an empty identifier",
        () -> sql("CALL %s.system.rewrite_manifests('')", catalogName));
  }

  @Test
  public void testReplacePartitionField() {
    sql(
        "CREATE TABLE %s (id int, ts timestamp, day_of_ts date) USING iceberg PARTITIONED BY (day_of_ts)",
        tableName);

    sql("ALTER TABLE %s SET TBLPROPERTIES ('format-version' = '2')", tableName);
    sql("ALTER TABLE %s REPLACE PARTITION FIELD day_of_ts WITH days(ts)\n", tableName);
    sql(
        "INSERT INTO %s VALUES (1, CAST('2022-01-01 10:00:00' AS TIMESTAMP), CAST('2022-01-01' AS DATE))",
        tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(1, Timestamp.valueOf("2022-01-01 10:00:00"), Date.valueOf("2022-01-01"))),
        sql("SELECT * FROM %s WHERE ts < current_timestamp()", tableName));

    sql("CALL %s.system.rewrite_manifests(table => '%s')", catalogName, tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(1, Timestamp.valueOf("2022-01-01 10:00:00"), Date.valueOf("2022-01-01"))),
        sql("SELECT * FROM %s WHERE ts < current_timestamp()", tableName));
  }
}
