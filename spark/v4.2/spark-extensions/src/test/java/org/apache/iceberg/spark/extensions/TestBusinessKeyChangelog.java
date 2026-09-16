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

import java.sql.Timestamp;
import java.util.List;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

class TestBusinessKeyChangelog extends ExtensionsTestBase {

  @AfterEach
  void removeTableAndView() {
    spark.catalog().dropTempView("business_key_expected");
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  void matchesChangelogViewWithoutLineage() {
    createTable();
    Table table = validationCatalog.loadTable(tableIdent);
    long startSnapshot = table.currentSnapshot().snapshotId();
    sql("UPDATE %s SET data = 'updated' WHERE id = 1", tableName);
    table.refresh();
    Snapshot end = table.currentSnapshot();
    Dataset<Row> changes = changes(end.sequenceNumber(), end.sequenceNumber(), true);
    Timestamp timestamp = new Timestamp(end.timestampMillis());
    assertThat(changes.collectAsList())
        .containsExactlyInAnyOrder(
            RowFactory.create(
                1L, "a", null, null, "update_preimage", end.sequenceNumber(), timestamp),
            RowFactory.create(
                1L, "updated", null, null, "update_postimage", end.sequenceNumber(), timestamp));
    assertThat(changes.schema().apply("_row_id").nullable()).isTrue();
    assertThat(changes.schema().apply("_last_updated_sequence_number").nullable()).isTrue();

    sql(
        "CALL %s.system.create_changelog_view(table => '%s', "
            + "changelog_view => 'business_key_expected', compute_updates => true, "
            + "identifier_columns => array('id'), "
            + "options => map('start-snapshot-id', '%d', 'end-snapshot-id', '%d'))",
        catalogName, tableName, startSnapshot, end.snapshotId());
    List<Row> expected =
        spark
            .sql(
                "SELECT id, data, "
                    + "CASE _change_type WHEN 'UPDATE_BEFORE' THEN 'update_preimage' "
                    + "WHEN 'UPDATE_AFTER' THEN 'update_postimage' ELSE lower(_change_type) END AS _change_type "
                    + "FROM business_key_expected")
            .collectAsList();
    assertThat(changes.select("id", "data", "_change_type").collectAsList())
        .containsExactlyInAnyOrderElementsOf(expected);
    assertThat(changes.filter("data = 'updated'").select("_change_type").collectAsList())
        .containsExactly(RowFactory.create("update_postimage"));
    assertThat(changes.select("_change_type").collectAsList())
        .containsExactlyInAnyOrder(
            RowFactory.create("update_preimage"), RowFactory.create("update_postimage"));
  }

  @TestTemplate
  void keepsDeleteInsertWhenUpdateImagesAreDisabled() {
    createTable();
    sql("UPDATE %s SET data = 'updated' WHERE id = 1", tableName);
    long version = validationCatalog.loadTable(tableIdent).currentSnapshot().sequenceNumber();
    assertThat(
            changes(version, version, false).select("id", "data", "_change_type").collectAsList())
        .containsExactlyInAnyOrder(
            RowFactory.create(1L, "a", "delete"), RowFactory.create(1L, "updated", "insert"));
  }

  @TestTemplate
  void treatsBusinessKeyChangeAsDeleteAndInsert() {
    createTable();
    sql("UPDATE %s SET id = 3 WHERE id = 1", tableName);
    long version = validationCatalog.loadTable(tableIdent).currentSnapshot().sequenceNumber();
    assertThat(changes(version, version, true).select("id", "data", "_change_type").collectAsList())
        .containsExactlyInAnyOrder(
            RowFactory.create(1L, "a", "delete"), RowFactory.create(3L, "a", "insert"));
  }

  @TestTemplate
  void pairsWithinEachCommitAndRemovesSameValueRewrites() {
    createTable();
    sql("UPDATE %s SET data = 'updated' WHERE id = 1", tableName);
    long first = validationCatalog.loadTable(tableIdent).currentSnapshot().sequenceNumber();
    sql("UPDATE %s SET data = 'a' WHERE id = 1", tableName);
    long second = validationCatalog.loadTable(tableIdent).currentSnapshot().sequenceNumber();
    assertThat(
            changes(first, second, true)
                .select("data", "_change_type", "_commit_version")
                .collectAsList())
        .containsExactlyInAnyOrder(
            RowFactory.create("a", "update_preimage", first),
            RowFactory.create("updated", "update_postimage", first),
            RowFactory.create("updated", "update_preimage", second),
            RowFactory.create("a", "update_postimage", second));
    sql("UPDATE %s SET data = 'a' WHERE id = 1", tableName);
    long sameValue = validationCatalog.loadTable(tableIdent).currentSnapshot().sequenceNumber();
    assertThat(changes(sameValue, sameValue, true).collectAsList()).isEmpty();
  }

  @TestTemplate
  void rejectsAmbiguousBusinessKeys() {
    createTable();
    sql("INSERT INTO %s VALUES (1, 'other')", tableName);
    sql("UPDATE %s SET data = 'updated' WHERE id = 1", tableName);
    long version = validationCatalog.loadTable(tableIdent).currentSnapshot().sequenceNumber();
    assertThatThrownBy(() -> changes(version, version, true).collectAsList())
        .hasStackTraceContaining("multiple rows with the same identifier");
  }

  @TestTemplate
  void validatesBusinessKeyOptionsAndRejectsStreaming() {
    createTable();
    for (String identifiers : List.of("", "missing", "id,id", "data.nested", "_row_id")) {
      assertThatThrownBy(
              () ->
                  spark
                      .read()
                      .option("identifier-columns", identifiers)
                      .option("computeUpdates", "true")
                      .changes(tableName)
                      .collectAsList())
          .hasStackTraceContaining("identifier column");
    }
    for (String mode : List.of("none", "netChanges")) {
      assertThatThrownBy(
              () ->
                  spark
                      .read()
                      .option("identifier-columns", "id")
                      .option("deduplicationMode", mode)
                      .changes(tableName)
                      .collectAsList())
          .hasStackTraceContaining("requires deduplicationMode=dropCarryovers");
    }
    assertThatThrownBy(
            () ->
                spark
                    .readStream()
                    .option("identifier-columns", "id")
                    .option("computeUpdates", "true")
                    .changes(tableName)
                    .explain())
        .hasStackTraceContaining("Business-key CDC currently supports batch reads only");
  }

  @TestTemplate
  void resolvesQuotedCompositeKeysAndNullableValues() {
    sql(
        "CREATE TABLE %s (`Tenant.ID` string, id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='2', 'write.update.mode'='copy-on-write')",
        tableName);
    sql(
        "INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES "
            + "('x', CAST(NULL AS BIGINT), CAST(NULL AS STRING)), ('y', 1, 'b')",
        tableName);
    sql("UPDATE %s SET data = 'updated' WHERE `Tenant.ID` = 'x'", tableName);
    long version = validationCatalog.loadTable(tableIdent).currentSnapshot().sequenceNumber();
    Dataset<Row> result =
        spark
            .read()
            .option("identifier-columns", "tenant.id, ID")
            .option("computeUpdates", "true")
            .option("startingVersion", String.valueOf(version))
            .option("endingVersion", String.valueOf(version))
            .changes(tableName);
    assertThat(result.selectExpr("`Tenant.ID`", "id", "data", "_change_type").collectAsList())
        .containsExactlyInAnyOrder(
            RowFactory.create("x", null, null, "update_preimage"),
            RowFactory.create("x", null, "updated", "update_postimage"));
  }

  @TestTemplate
  void usesValueSemanticsWhenLineageIsAvailable() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3', 'write.update.mode'='copy-on-write')",
        tableName);
    sql("INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 'a'), (2, 'b')", tableName);
    sql("UPDATE %s SET data = 'a' WHERE id = 1", tableName);
    long version = validationCatalog.loadTable(tableIdent).currentSnapshot().sequenceNumber();
    assertThat(changes(version, version, true).collectAsList()).isEmpty();
    sql("UPDATE %s SET data = 'updated' WHERE id = 1", tableName);
    long updated = validationCatalog.loadTable(tableIdent).currentSnapshot().sequenceNumber();
    assertThat(
            changes(updated, updated, true)
                .select("_row_id", "_last_updated_sequence_number")
                .collectAsList())
        .containsExactly(RowFactory.create(null, null), RowFactory.create(null, null));
  }

  @TestTemplate
  void supportsCompositeKeys() {
    sql(
        "CREATE TABLE %s (tenant string, id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='2', 'write.update.mode'='copy-on-write')",
        tableName);
    sql(
        "INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES " + "('x', 1, 'a'), ('y', 1, 'b')",
        tableName);
    sql("UPDATE %s SET data = 'updated' WHERE tenant = 'x'", tableName);
    long version = validationCatalog.loadTable(tableIdent).currentSnapshot().sequenceNumber();
    Dataset<Row> result =
        spark
            .read()
            .option("identifier-columns", "tenant, id")
            .option("computeUpdates", "true")
            .option("startingVersion", String.valueOf(version))
            .option("endingVersion", String.valueOf(version))
            .changes(tableName);
    assertThat(result.select("tenant", "id", "data", "_change_type").collectAsList())
        .containsExactlyInAnyOrder(
            RowFactory.create("x", 1L, "a", "update_preimage"),
            RowFactory.create("x", 1L, "updated", "update_postimage"));
  }

  private void createTable() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='2', 'write.update.mode'='copy-on-write')",
        tableName);
    sql("INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 'a'), (2, 'b')", tableName);
  }

  private Dataset<Row> changes(long start, long end, boolean computeUpdates) {
    return spark.sql(
        String.format(
            "SELECT * FROM %s CHANGES FROM VERSION %d TO VERSION %d "
                + "WITH (`identifier-columns` = 'id', computeUpdates = '%s')",
            tableName, start, end, computeUpdates));
  }
}
