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
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.assertj.core.api.Assertions;
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
    Assertions.assertThatThrownBy(
            () -> sql("CALL %s.system.rewrite_manifests('n', table => 't')", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage("Named and positional arguments cannot be mixed");

    Assertions.assertThatThrownBy(
            () -> sql("CALL %s.custom.rewrite_manifests('n', 't')", catalogName))
        .isInstanceOf(NoSuchProcedureException.class)
        .hasMessage("Procedure custom.rewrite_manifests not found");

    Assertions.assertThatThrownBy(() -> sql("CALL %s.system.rewrite_manifests()", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage("Missing required parameters: [table]");

    Assertions.assertThatThrownBy(
            () -> sql("CALL %s.system.rewrite_manifests('n', 2.2)", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith("Wrong arg type for use_caching");

    Assertions.assertThatThrownBy(
            () -> sql("CALL %s.system.rewrite_manifests(table => 't', tAbLe => 't')", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage("Could not build name to arg map: Duplicate procedure argument: table");

    Assertions.assertThatThrownBy(() -> sql("CALL %s.system.rewrite_manifests('')", catalogName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot handle an empty identifier for argument table");
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
    sql(
        "INSERT INTO %s VALUES (2, CAST('2022-01-01 11:00:00' AS TIMESTAMP), CAST('2022-01-01' AS DATE))",
        tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(1, Timestamp.valueOf("2022-01-01 10:00:00"), Date.valueOf("2022-01-01")),
            row(2, Timestamp.valueOf("2022-01-01 11:00:00"), Date.valueOf("2022-01-01"))),
        sql("SELECT * FROM %s WHERE ts < current_timestamp() order by 1 asc", tableName));

    List<Object[]> output =
        sql("CALL %s.system.rewrite_manifests(table => '%s')", catalogName, tableName);
    assertEquals("Procedure output must match", ImmutableList.of(row(2, 1)), output);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(1, Timestamp.valueOf("2022-01-01 10:00:00"), Date.valueOf("2022-01-01")),
            row(2, Timestamp.valueOf("2022-01-01 11:00:00"), Date.valueOf("2022-01-01"))),
        sql("SELECT * FROM %s WHERE ts < current_timestamp() order by 1 asc", tableName));
  }

  @Test
  public void testWriteManifestWithSpecId() {
    sql(
        "CREATE TABLE %s (id int, dt string, hr string) USING iceberg PARTITIONED BY (dt)",
        tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('commit.manifest-merge.enabled' = 'false')", tableName);

    sql("INSERT INTO %s VALUES (1, '2024-01-01', '00')", tableName);
    sql("INSERT INTO %s VALUES (2, '2024-01-01', '00')", tableName);
    assertEquals(
        "Should have 2 manifests and their partition spec id should be 0",
        ImmutableList.of(row(0), row(0)),
        sql("SELECT partition_spec_id FROM %s.manifests order by 1 asc", tableName));

    sql("ALTER TABLE %s ADD PARTITION FIELD hr", tableName);
    sql("INSERT INTO %s VALUES (3, '2024-01-01', '00')", tableName);
    assertEquals(
        "Should have 3 manifests and their partition spec id should be 0 and 1",
        ImmutableList.of(row(0), row(0), row(1)),
        sql("SELECT partition_spec_id FROM %s.manifests order by 1 asc", tableName));

    List<Object[]> output = sql("CALL %s.system.rewrite_manifests('%s')", catalogName, tableIdent);
    assertEquals("Nothing should be rewritten", ImmutableList.of(row(0, 0)), output);

    output =
        sql(
            "CALL %s.system.rewrite_manifests(table => '%s', spec_id => 0)",
            catalogName, tableIdent);
    assertEquals("There should be 2 manifests rewriten", ImmutableList.of(row(2, 1)), output);
    assertEquals(
        "Should have 2 manifests and their partition spec id should be 0 and 1",
        ImmutableList.of(row(0), row(1)),
        sql("SELECT partition_spec_id FROM %s.manifests order by 1 asc", tableName));
  }

  @Test
  public void testRewriteManifestsConditionInPartitionedTable() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, ts timestamp) USING iceberg PARTITIONED BY (hours(ts))",
        tableName);

    sql(
        "INSERT INTO TABLE %s VALUES (1, cast('2024-01-08T07:00:00+00:00' as timestamp))",
        tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (2, cast('2024-01-07T06:00:00+00:00' as timestamp))",
        tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (3, cast('2024-01-07T05:00:00+00:00' as timestamp))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals(
        "Must have 3 manifest", 3, table.currentSnapshot().allManifests(table.io()).size());

    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_manifests(use_caching => false, table => '%s',"
                + " where => 'ts = cast(\"2024-01-08T07:00:00+00:00\" as timestamp)')",
            catalogName, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(0, 0)), output);

    output =
        sql(
            "CALL %s.system.rewrite_manifests(use_caching => false, table => '%s',"
                + " where => 'ts <= cast(\"2024-01-07T07:00:00+00:00\" as timestamp)')",
            catalogName, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(2, 1)), output);

    // test inclusive
    sql(
        "INSERT INTO TABLE %s VALUES (4, cast('2024-01-07T04:00:00+00:00' as timestamp))",
        tableName);
    output =
        sql(
            "CALL %s.system.rewrite_manifests(use_caching => false, table => '%s',"
                + " where => 'ts <= cast(\"2024-01-07T05:00:00+00:00\" as timestamp)')",
            catalogName, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(2, 1)), output);

    sql(
        "INSERT INTO TABLE %s VALUES (5, cast('2024-01-07T06:00:00+00:00' as timestamp))",
        tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (6, cast('2024-01-07T06:00:00+00:00' as timestamp))",
        tableName);
    // where condition always true
    output =
        sql(
            "CALL %s.system.rewrite_manifests(use_caching => false, table => '%s', where => 'id = 2')",
            catalogName, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(4, 1)), output);
  }

  @Test
  public void testRewriteManifestsConditionInUnpartitionedTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, ts timestamp) USING iceberg", tableName);

    sql(
        "INSERT INTO TABLE %s VALUES (1, cast('2024-01-08T07:00:00+00:00' as timestamp))",
        tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (2, cast('2024-01-07T06:00:00+00:00' as timestamp))",
        tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (2, cast('2024-01-07T06:00:00+00:00' as timestamp))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals(
        "Must have 3 manifest", 3, table.currentSnapshot().allManifests(table.io()).size());

    // where condition always true
    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_manifests(use_caching => false, table => '%s',"
                + " where => 'ts = cast(\"2024-01-08T07:00:00+00:00\" as timestamp)')",
            catalogName, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(3, 1)), output);

    sql(
        "INSERT INTO TABLE %s VALUES (2, cast('2024-01-07T06:00:00+00:00' as timestamp))",
        tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (3, cast('2024-01-07T06:00:00+00:00' as timestamp))",
        tableName);
    output =
        sql(
            "CALL %s.system.rewrite_manifests(use_caching => false, table => '%s',"
                + " where => 'ts <= cast(\"2024-01-07T07:00:00+00:00\" as timestamp)')",
            catalogName, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(3, 1)), output);

    sql(
        "INSERT INTO TABLE %s VALUES (4, cast('2024-01-07T06:00:00+00:00' as timestamp))",
        tableName);
    sql(
        "INSERT INTO TABLE %s VALUES (5, cast('2024-01-07T06:00:00+00:00' as timestamp))",
        tableName);
    output =
        sql(
            "CALL %s.system.rewrite_manifests(use_caching => false, table => '%s', where => 'id = 2')",
            catalogName, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(3, 1)), output);
  }

  @Test
  public void testRewriteManifestsConditionWithMultiSpecId() {
    sql(
        "CREATE TABLE %s (id int, dt string, hr string) USING iceberg PARTITIONED BY (dt)",
        tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('commit.manifest-merge.enabled' = 'false')", tableName);

    sql("INSERT INTO %s VALUES (1, '2024-01-01', '00')", tableName);
    sql("INSERT INTO %s VALUES (2, '2024-01-01', '00')", tableName);
    assertEquals(
        "Should have 2 manifests and their partition spec id should be 0",
        ImmutableList.of(row(0), row(0)),
        sql("SELECT partition_spec_id FROM %s.manifests order by 1 asc", tableName));

    sql("ALTER TABLE %s ADD PARTITION FIELD hr", tableName);
    sql("INSERT INTO %s VALUES (3, '2024-01-01', '00')", tableName);
    sql("INSERT INTO %s VALUES (4, '2024-01-01', '00')", tableName);
    sql("INSERT INTO %s VALUES (5, '2024-01-02', '00')", tableName);
    assertEquals(
        "Should have 5 manifests and their partition spec id should be 0 and 1",
        ImmutableList.of(row(0), row(0), row(1), row(1), row(1)),
        sql("SELECT partition_spec_id FROM %s.manifests order by 1 asc", tableName));

    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_manifests(table => '%s', where => 'dt = \"2024-01-01\"')",
            catalogName, tableIdent);
    assertEquals("There should be 2 manifests rewritten", ImmutableList.of(row(2, 1)), output);

    output =
        sql(
            "CALL %s.system.rewrite_manifests(table => '%s', spec_id => 0, where => 'dt = \"2024-01-01\"')",
            catalogName, tableIdent);
    assertEquals("There should be 2 manifests rewritten", ImmutableList.of(row(2, 1)), output);
    assertEquals(
        "Should have 3 manifests and their partition spec id should be 0 and 1",
        ImmutableList.of(row(0), row(1), row(1)),
        sql("SELECT partition_spec_id FROM %s.manifests order by 1 asc", tableName));
  }
}
