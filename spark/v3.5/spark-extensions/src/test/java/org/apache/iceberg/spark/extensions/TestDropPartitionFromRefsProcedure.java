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
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestDropPartitionFromRefsProcedure extends ExtensionsTestBase {

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testDropPartitionFromSingleTag() {
    sql(
        "CREATE TABLE %s (id int NOT NULL, dt string) USING iceberg PARTITIONED BY (dt)",
        tableName);
    sql("INSERT INTO %s VALUES (1, '2024-01-01'), (2, '2024-01-02')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();

    sql("ALTER TABLE %s CREATE TAG v1 AS OF VERSION %d", tableName, snapshotId);

    List<Object[]> result =
        sql(
            "CALL %s.system.drop_partition_from_refs("
                + "table => '%s', where => 'dt = \"2024-01-01\"')",
            catalogName, tableIdent);

    assertThat(result).hasSize(1);
    assertThat(result.get(0)[0]).isEqualTo("v1");
    assertThat((long) result.get(0)[1]).isEqualTo(snapshotId);
    assertThat((long) result.get(0)[2]).isNotEqualTo(snapshotId);

    // tag should no longer see the dropped partition
    assertEquals(
        "Tag should only contain rows from remaining partition",
        ImmutableList.of(row(2, "2024-01-02")),
        sql("SELECT * FROM %s VERSION AS OF 'v1' ORDER BY id", tableName));

    // main branch is unaffected
    assertEquals(
        "Main should still contain all rows",
        ImmutableList.of(row(1, "2024-01-01"), row(2, "2024-01-02")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testDeduplicatesTagsSharingSnapshot() {
    sql(
        "CREATE TABLE %s (id int NOT NULL, dt string) USING iceberg PARTITIONED BY (dt)",
        tableName);
    sql("INSERT INTO %s VALUES (1, '2024-01-01'), (2, '2024-01-02')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    long sharedSnapshotId = table.currentSnapshot().snapshotId();

    // two tags pointing at the same snapshot
    sql("ALTER TABLE %s CREATE TAG v1 AS OF VERSION %d", tableName, sharedSnapshotId);
    sql("ALTER TABLE %s CREATE TAG v2 AS OF VERSION %d", tableName, sharedSnapshotId);

    List<Object[]> result =
        sql(
            "CALL %s.system.drop_partition_from_refs("
                + "table => '%s', where => 'dt = \"2024-01-01\"')",
            catalogName, tableIdent);

    // both tags updated, but manifests rewritten only once
    assertThat(result).hasSize(2);

    table.refresh();
    long newSnapshotV1 = table.snapshot("v1").snapshotId();
    long newSnapshotV2 = table.snapshot("v2").snapshotId();

    // both tags now point to the same new snapshot (deduplication)
    assertThat(newSnapshotV1).isEqualTo(newSnapshotV2);
    assertThat(newSnapshotV1).isNotEqualTo(sharedSnapshotId);

    for (String tag : List.of("v1", "v2")) {
      assertEquals(
          "Tag " + tag + " should only contain rows from remaining partition",
          ImmutableList.of(row(2, "2024-01-02")),
          sql("SELECT * FROM %s VERSION AS OF '%s' ORDER BY id", tableName, tag));
    }
  }

  @TestTemplate
  public void testDropFromMultipleManifests() {
    sql(
        "CREATE TABLE %s (id int NOT NULL, dt string) USING iceberg PARTITIONED BY (dt)",
        tableName);
    // two separate inserts produce two manifest files, each covering one partition
    sql("INSERT INTO %s VALUES (1, '2024-01-01')", tableName);
    sql("INSERT INTO %s VALUES (2, '2024-01-02')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();

    sql("ALTER TABLE %s CREATE TAG v1 AS OF VERSION %d", tableName, snapshotId);

    List<Object[]> result =
        sql(
            "CALL %s.system.drop_partition_from_refs("
                + "table => '%s', where => 'dt = \"2024-01-01\"')",
            catalogName, tableIdent);

    assertThat(result).hasSize(1);
    assertThat(result.get(0)[0]).isEqualTo("v1");

    // tag should only contain the dt=2024-01-02 partition; the other was dropped
    assertEquals(
        "Tag should only contain rows from remaining partition",
        ImmutableList.of(row(2, "2024-01-02")),
        sql("SELECT * FROM %s VERSION AS OF 'v1' ORDER BY id", tableName));
  }

  @TestTemplate
  public void testDryRunDoesNotCommit() {
    sql(
        "CREATE TABLE %s (id int NOT NULL, dt string) USING iceberg PARTITIONED BY (dt)",
        tableName);
    sql("INSERT INTO %s VALUES (1, '2024-01-01'), (2, '2024-01-02')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();

    sql("ALTER TABLE %s CREATE TAG v1 AS OF VERSION %d", tableName, snapshotId);

    sql(
        "CALL %s.system.drop_partition_from_refs("
            + "table => '%s', where => 'dt = \"2024-01-01\"', dry_run => true)",
        catalogName, tableIdent);

    table.refresh();
    // tag must still point at the original snapshot
    assertThat(table.snapshot("v1").snapshotId()).isEqualTo(snapshotId);

    assertEquals(
        "Tag data should be unchanged after dry run",
        ImmutableList.of(row(1, "2024-01-01"), row(2, "2024-01-02")),
        sql("SELECT * FROM %s VERSION AS OF 'v1' ORDER BY id", tableName));
  }

  @TestTemplate
  public void testMainBranchIsNeverTouched() {
    sql(
        "CREATE TABLE %s (id int NOT NULL, dt string) USING iceberg PARTITIONED BY (dt)",
        tableName);
    sql("INSERT INTO %s VALUES (1, '2024-01-01'), (2, '2024-01-02')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    long mainSnapshotBefore = table.currentSnapshot().snapshotId();

    // refType=all still must not touch main
    sql(
        "CALL %s.system.drop_partition_from_refs("
            + "table => '%s', where => 'dt = \"2024-01-01\"', refs => 'all')",
        catalogName, tableIdent);

    table.refresh();
    assertThat(table.currentSnapshot().snapshotId())
        .as("main snapshot must not change")
        .isEqualTo(mainSnapshotBefore);

    assertEquals(
        "Main branch data must be unchanged",
        ImmutableList.of(row(1, "2024-01-01"), row(2, "2024-01-02")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testInvalidRefsValueThrows() {
    sql(
        "CREATE TABLE %s (id int NOT NULL, dt string) USING iceberg PARTITIONED BY (dt)",
        tableName);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.drop_partition_from_refs("
                        + "table => '%s', where => 'dt = \"2024-01-01\"', refs => 'unknown')",
                    catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid refs value");
  }

  @TestTemplate
  public void testNamedArgs() {
    sql(
        "CREATE TABLE %s (id int NOT NULL, dt string) USING iceberg PARTITIONED BY (dt)",
        tableName);
    sql("INSERT INTO %s VALUES (1, '2024-01-01'), (2, '2024-01-02')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();
    sql("ALTER TABLE %s CREATE TAG v1 AS OF VERSION %d", tableName, snapshotId);

    List<Object[]> result =
        sql(
            "CALL %s.system.drop_partition_from_refs("
                + "where => 'dt = \"2024-01-01\"', table => '%s')",
            catalogName, tableIdent);

    assertThat(result).hasSize(1);
    assertThat(result.get(0)[0]).isEqualTo("v1");
  }

  @TestTemplate
  public void testTargetsBranchesWhenRefTypeIsBranches() {
    sql(
        "CREATE TABLE %s (id int NOT NULL, dt string) USING iceberg PARTITIONED BY (dt)",
        tableName);
    sql("INSERT INTO %s VALUES (1, '2024-01-01'), (2, '2024-01-02')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();

    sql("ALTER TABLE %s CREATE BRANCH audit AS OF VERSION %d", tableName, snapshotId);

    List<Object[]> result =
        sql(
            "CALL %s.system.drop_partition_from_refs("
                + "table => '%s', where => 'dt = \"2024-01-01\"', refs => 'branches')",
            catalogName, tableIdent);

    assertThat(result).hasSize(1);
    assertThat(result.get(0)[0]).isEqualTo("audit");

    assertEquals(
        "Branch should only contain rows from remaining partition",
        ImmutableList.of(row(2, "2024-01-02")),
        sql("SELECT * FROM %s VERSION AS OF 'audit' ORDER BY id", tableName));
  }

  @TestTemplate
  public void testNoRefsMatchedReturnsEmptyResult() {
    sql(
        "CREATE TABLE %s (id int NOT NULL, dt string) USING iceberg PARTITIONED BY (dt)",
        tableName);
    sql("INSERT INTO %s VALUES (1, '2024-01-01')", tableName);
    // no tags created

    List<Object[]> result =
        sql(
            "CALL %s.system.drop_partition_from_refs("
                + "table => '%s', where => 'dt = \"2024-01-01\"')",
            catalogName, tableIdent);

    assertThat(result).isEmpty();
  }
}
