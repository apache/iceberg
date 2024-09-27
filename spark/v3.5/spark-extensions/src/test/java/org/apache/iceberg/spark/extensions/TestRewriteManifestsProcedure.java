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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRewriteManifestsProcedure extends ExtensionsTestBase {

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testRewriteManifestsInEmptyTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    List<Object[]> output = sql("CALL %s.system.rewrite_manifests('%s')", catalogName, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(0, 0)), output);
  }

  @TestTemplate
  public void testRewriteLargeManifests() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.currentSnapshot().allManifests(table.io()))
        .as("Must have 1 manifest")
        .hasSize(1);

    sql("ALTER TABLE %s SET TBLPROPERTIES ('commit.manifest.target-size-bytes' '1')", tableName);

    List<Object[]> output = sql("CALL %s.system.rewrite_manifests('%s')", catalogName, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(1, 4)), output);

    table.refresh();

    assertThat(table.currentSnapshot().allManifests(table.io()))
        .as("Must have 4 manifests")
        .hasSize(4);
  }

  @TestTemplate
  public void testRewriteManifestsNoOp() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.currentSnapshot().allManifests(table.io()))
        .as("Must have 1 manifest")
        .hasSize(1);

    List<Object[]> output = sql("CALL %s.system.rewrite_manifests('%s')", catalogName, tableIdent);
    // should not rewrite any manifests for no-op (output of rewrite is same as before and after)
    assertEquals("Procedure output must match", ImmutableList.of(row(0, 0)), output);

    table.refresh();

    assertThat(table.currentSnapshot().allManifests(table.io()))
        .as("Must have 1 manifest")
        .hasSize(1);
  }

  @TestTemplate
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

          assertThat(table.currentSnapshot().allManifests(table.io()))
              .as("Must have 1 manifest")
              .hasSize(1);

          sql(
              "ALTER TABLE %s SET TBLPROPERTIES ('commit.manifest.target-size-bytes' '1')",
              tableName);

          List<Object[]> output =
              sql("CALL %s.system.rewrite_manifests('%s')", catalogName, tableIdent);
          assertEquals("Procedure output must match", ImmutableList.of(row(1, 4)), output);

          table.refresh();

          assertThat(table.currentSnapshot().allManifests(table.io()))
              .as("Must have 4 manifests")
              .hasSize(4);
        });
  }

  @TestTemplate
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

          assertThat(table.currentSnapshot().allManifests(table.io()))
              .as("Must have 1 manifest")
              .hasSize(1);

          sql(
              "ALTER TABLE %s SET TBLPROPERTIES ('commit.manifest.target-size-bytes' '1')",
              tableName);

          List<Object[]> output =
              sql("CALL %s.system.rewrite_manifests('%s')", catalogName, tableIdent);
          assertEquals("Procedure output must match", ImmutableList.of(row(1, 4)), output);

          table.refresh();

          assertThat(table.currentSnapshot().allManifests(table.io()))
              .as("Must have 4 manifests")
              .hasSize(4);
        });
  }

  @TestTemplate
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

    assertThat(table.currentSnapshot().allManifests(table.io()))
        .as("Must have 4 manifests")
        .hasSize(4);

    List<Object[]> output =
        sql("CALL %s.system.rewrite_manifests(table => '%s')", catalogName, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(4, 1)), output);

    table.refresh();

    assertThat(table.currentSnapshot().allManifests(table.io()))
        .as("Must have 1 manifest")
        .hasSize(1);
  }

  @TestTemplate
  public void testRewriteSmallManifestsWithoutCaching() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.currentSnapshot().allManifests(table.io()))
        .as("Must have 2 manifest")
        .hasSize(2);

    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_manifests(use_caching => false, table => '%s')",
            catalogName, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(2, 1)), output);

    table.refresh();

    assertThat(table.currentSnapshot().allManifests(table.io()))
        .as("Must have 1 manifest")
        .hasSize(1);
  }

  @TestTemplate
  public void testRewriteManifestsCaseInsensitiveArgs() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)",
        tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.currentSnapshot().allManifests(table.io()))
        .as("Must have 2 manifests")
        .hasSize(2);

    List<Object[]> output =
        sql(
            "CALL %s.system.rewrite_manifests(usE_cAcHiNg => false, tAbLe => '%s')",
            catalogName, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(2, 1)), output);

    table.refresh();

    assertThat(table.currentSnapshot().allManifests(table.io()))
        .as("Must have 1 manifest")
        .hasSize(1);
  }

  @TestTemplate
  public void testInvalidRewriteManifestsCases() {
    assertThatThrownBy(
            () -> sql("CALL %s.system.rewrite_manifests('n', table => 't')", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage("Named and positional arguments cannot be mixed");

    assertThatThrownBy(() -> sql("CALL %s.custom.rewrite_manifests('n', 't')", catalogName))
        .isInstanceOf(NoSuchProcedureException.class)
        .hasMessage("Procedure custom.rewrite_manifests not found");

    assertThatThrownBy(() -> sql("CALL %s.system.rewrite_manifests()", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage("Missing required parameters: [table]");

    assertThatThrownBy(() -> sql("CALL %s.system.rewrite_manifests('n', 2.2)", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith("Wrong arg type for use_caching");

    assertThatThrownBy(
            () -> sql("CALL %s.system.rewrite_manifests(table => 't', tAbLe => 't')", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage("Could not build name to arg map: Duplicate procedure argument: table");

    assertThatThrownBy(() -> sql("CALL %s.system.rewrite_manifests('')", catalogName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot handle an empty identifier for argument table");
  }

  @TestTemplate
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

  @TestTemplate
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
}
