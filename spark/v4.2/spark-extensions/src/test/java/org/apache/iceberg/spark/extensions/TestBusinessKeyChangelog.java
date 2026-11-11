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
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

class TestBusinessKeyChangelog extends ExtensionsTestBase {

  private static final Set<String> RANGE_OPTIONS =
      Set.of(
          "startingversion",
          "endingversion",
          "startingtimestamp",
          "endingtimestamp",
          "startingboundinclusive",
          "endingboundinclusive");

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
    for (String identifiers :
        List.of("", " ", "missing", "id,id", "id,ID", "id,", "data.nested", "_row_id")) {
      assertRejected(
          Map.of("identifier-columns", identifiers, "computeUpdates", "true"), "identifier column");
    }
    for (boolean computeUpdates : List.of(false, true)) {
      for (String mode : List.of("none", "netChanges")) {
        assertRejected(
            Map.of(
                "identifier-columns",
                "id",
                "deduplicationMode",
                mode,
                "computeUpdates",
                String.valueOf(computeUpdates)),
            "requires deduplicationMode=dropCarryovers");
      }
      assertThatThrownBy(
              () ->
                  spark
                      .readStream()
                      .option("identifier-columns", "id")
                      .option("computeUpdates", computeUpdates)
                      .changes(tableName)
                      .explain())
          .hasStackTraceContaining("Business-key CDC currently supports batch reads only");
      assertThatThrownBy(
              () ->
                  spark
                      .sql(
                          String.format(
                              "SELECT * FROM STREAM %s CHANGES "
                                  + "WITH (`identifier-columns` = 'id', computeUpdates = '%s')",
                              tableName, computeUpdates))
                      .explain())
          .hasStackTraceContaining("Business-key CDC currently supports batch reads only");
    }
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
        equivalentChanges(
            Map.of(
                "identifier-columns",
                "tenant.id, ID",
                "computeUpdates",
                "true",
                "startingVersion",
                String.valueOf(version),
                "endingVersion",
                String.valueOf(version)));
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
        equivalentChanges(
            Map.of(
                "identifier-columns",
                "tenant, id",
                "computeUpdates",
                "true",
                "startingVersion",
                String.valueOf(version),
                "endingVersion",
                String.valueOf(version)));
    assertThat(result.select("tenant", "id", "data", "_change_type").collectAsList())
        .containsExactlyInAnyOrder(
            RowFactory.create("x", 1L, "a", "update_preimage"),
            RowFactory.create("x", 1L, "updated", "update_postimage"));
  }

  @TestTemplate
  void readsEachChangeTypeWithBothUpdateSettings() {
    createTable();
    sql("UPDATE %s SET data = 'updated' WHERE id = 1", tableName);
    Snapshot update = currentSnapshot();
    sql("DELETE FROM %s WHERE id = 2", tableName);
    Snapshot delete = currentSnapshot();
    sql("INSERT INTO %s VALUES (3, 'c')", tableName);
    Snapshot insert = currentSnapshot();

    for (boolean computeUpdates : List.of(false, true)) {
      assertThat(
              changes(update.sequenceNumber(), insert.sequenceNumber(), computeUpdates)
                  .collectAsList())
          .containsExactlyInAnyOrder(
              cdcRow(1L, "a", computeUpdates ? "update_preimage" : "delete", update),
              cdcRow(1L, "updated", computeUpdates ? "update_postimage" : "insert", update),
              cdcRow(2L, "b", "delete", delete),
              cdcRow(3L, "c", "insert", insert));
    }
  }

  @TestTemplate
  void appliesVersionAndTimestampRangeBoundsToWholeCommits() {
    createTable();
    waitUntilAfter(currentSnapshot().timestampMillis());
    sql("UPDATE %s SET data = 'updated' WHERE id = 1", tableName);
    Snapshot update = currentSnapshot();
    waitUntilAfter(update.timestampMillis());
    sql("DELETE FROM %s WHERE id = 2", tableName);
    Snapshot delete = currentSnapshot();
    waitUntilAfter(delete.timestampMillis());
    sql("INSERT INTO %s VALUES (3, 'c')", tableName);
    Snapshot insert = currentSnapshot();

    for (boolean timestamps : List.of(false, true)) {
      for (boolean includeStart : List.of(false, true)) {
        for (boolean includeEnd : List.of(false, true)) {
          Map<String, String> options = new LinkedHashMap<>();
          options.put("identifier-columns", "id");
          options.put("computeUpdates", "true");
          options.put("startingBoundInclusive", String.valueOf(includeStart));
          options.put("endingBoundInclusive", String.valueOf(includeEnd));
          options.put(
              timestamps ? "startingTimestamp" : "startingVersion",
              timestamps ? timestamp(update) : String.valueOf(update.sequenceNumber()));
          options.put(
              timestamps ? "endingTimestamp" : "endingVersion",
              timestamps ? timestamp(insert) : String.valueOf(insert.sequenceNumber()));

          assertThat(equivalentChanges(options).collectAsList())
              .containsExactlyInAnyOrderElementsOf(
                  expectedRangeChanges(update, delete, insert, includeStart, includeEnd));
        }
      }
    }

    for (boolean timestamps : List.of(false, true)) {
      Map<String, String> options =
          Map.of(
              "identifier-columns",
              "id",
              timestamps ? "startingTimestamp" : "startingVersion",
              timestamps ? timestamp(insert) : String.valueOf(insert.sequenceNumber()));
      assertThat(equivalentChanges(options).collectAsList())
          .containsExactly(cdcRow(3L, "c", "insert", insert));
    }
    assertThat(
            equivalentChanges(
                    Map.of(
                        "identifier-columns",
                        "id",
                        "startingVersion",
                        String.valueOf(update.sequenceNumber()),
                        "endingVersion",
                        String.valueOf(update.sequenceNumber()),
                        "startingBoundInclusive",
                        "false"))
                .collectAsList())
        .isEmpty();
  }

  @TestTemplate
  void defaultsToCarryoverRemovalWithoutUpdateImages() {
    createTable();
    Snapshot initial = currentSnapshot();
    sql("UPDATE %s SET data = 'updated' WHERE id = 1", tableName);
    Snapshot update = currentSnapshot();
    List<Row> expected =
        List.of(
            cdcRow(1L, "a", "insert", initial),
            cdcRow(2L, "b", "insert", initial),
            cdcRow(1L, "a", "delete", update),
            cdcRow(1L, "updated", "insert", update));

    assertThat(equivalentChanges(Map.of("identifier-columns", "id")).collectAsList())
        .containsExactlyInAnyOrderElementsOf(expected);
    assertThat(
            equivalentChanges(
                    Map.of(
                        "identifier-columns", "id",
                        "deduplicationMode", "dropCarryovers",
                        "computeUpdates", "false"))
                .collectAsList())
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @TestTemplate
  void acceptsCaseInsensitiveOptionNamesAndValues() {
    createTable();
    sql("UPDATE %s SET data = 'updated' WHERE id = 1", tableName);
    Snapshot update = currentSnapshot();
    assertThat(
            equivalentChanges(
                    Map.of(
                        "IDENTIFIER-COLUMNS", "ID",
                        "COMPUTEUPDATES", "TrUe",
                        "DEDUPLICATIONMODE", "DROPCARRYOVERS",
                        "STARTINGVERSION", String.valueOf(update.sequenceNumber()),
                        "ENDINGVERSION", String.valueOf(update.sequenceNumber())))
                .collectAsList())
        .containsExactlyInAnyOrder(
            cdcRow(1L, "a", "update_preimage", update),
            cdcRow(1L, "updated", "update_postimage", update));
  }

  @TestTemplate
  void respectsCaseSensitiveIdentifierResolution() {
    sql(
        "CREATE TABLE %s (`ID` bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='2', 'write.update.mode'='copy-on-write')",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    sql("UPDATE %s SET data = 'updated' WHERE ID = 1", tableName);
    Snapshot update = currentSnapshot();

    withSQLConf(
        Map.of("spark.sql.caseSensitive", "true"),
        () -> {
          assertRejected(Map.of("identifier-columns", "id"), "identifier column");
          assertThat(
                  equivalentChanges(
                          Map.of(
                              "identifier-columns", "ID",
                              "computeUpdates", "true",
                              "startingVersion", String.valueOf(update.sequenceNumber())))
                      .collectAsList())
              .containsExactlyInAnyOrder(
                  cdcRow(1L, "a", "update_preimage", update),
                  cdcRow(1L, "updated", "update_postimage", update));
        });
  }

  @TestTemplate
  void comparesBinaryKeysAndNestedBinaryPayloadsByValue() {
    sql(
        "CREATE TABLE %s (id binary, data struct<patches:array<binary>,label:string>) "
            + "USING iceberg TBLPROPERTIES "
            + "('format-version'='2', 'write.update.mode'='copy-on-write')",
        tableName);
    sql(
        "INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES "
            + "(X'01', named_struct('patches', array(X'0A'), 'label', 'a')), "
            + "(X'02', named_struct('patches', array(X'0B'), 'label', 'b'))",
        tableName);
    sql(
        "UPDATE %s SET data = named_struct('patches', array(X'0C'), 'label', 'updated') "
            + "WHERE id = X'01'",
        tableName);
    Snapshot update = currentSnapshot();
    for (boolean computeUpdates : List.of(false, true)) {
      assertThat(
              changes(update.sequenceNumber(), update.sequenceNumber(), computeUpdates)
                  .selectExpr("hex(id)", "hex(data.patches[0])", "data.label", "_change_type")
                  .collectAsList())
          .containsExactlyInAnyOrder(
              RowFactory.create("01", "0A", "a", computeUpdates ? "update_preimage" : "delete"),
              RowFactory.create(
                  "01", "0C", "updated", computeUpdates ? "update_postimage" : "insert"));
    }
    assertRejected(Map.of("identifier-columns", "data"), "top-level primitive field");
  }

  @TestTemplate
  void supportsMapPayloadsWhenGroupingByBusinessKey() {
    sql(
        "CREATE TABLE %s (id bigint, data map<string,string>) USING iceberg "
            + "TBLPROPERTIES ('format-version'='2', 'write.update.mode'='copy-on-write')",
        tableName);
    sql(
        "INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES "
            + "(1, map('value', 'a')), (2, map('value', 'b'))",
        tableName);
    sql("UPDATE %s SET data = map('value', 'updated') WHERE id = 1", tableName);
    Snapshot update = currentSnapshot();
    assertThat(
            changes(update.sequenceNumber(), update.sequenceNumber(), true)
                .selectExpr("id", "data['value']", "_change_type")
                .collectAsList())
        .containsExactlyInAnyOrder(
            RowFactory.create(1L, "a", "update_preimage"),
            RowFactory.create(1L, "updated", "update_postimage"));
    assertRejected(
        Map.of("identifier-columns", "id", "computeUpdates", "false"),
        "Business-key CDC grouping columns must be orderable");
  }

  @TestTemplate
  void preservesDuplicateValuesWithoutComputingUpdates() {
    createTable();
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    sql("DELETE FROM %s WHERE id = 1", tableName);
    Snapshot delete = currentSnapshot();
    assertThat(changes(delete.sequenceNumber(), delete.sequenceNumber(), false).collectAsList())
        .containsExactlyInAnyOrder(
            cdcRow(1L, "a", "delete", delete), cdcRow(1L, "a", "delete", delete));
  }

  @TestTemplate
  void rejectsInvalidNativeOptionsWithoutDiscardingBusinessKeys() {
    createTable();
    assertRejected(
        Map.of("identifier-columns", "id", "computeUpdates", "not-a-boolean"), "not-a-boolean");
    assertRejected(
        Map.of("identifier-columns", "id", "deduplicationMode", "unknown-mode"), "unknown-mode");
    assertRejected(
        Map.of("identifier-columns", "id", "start-snapshot-id", "1"),
        "is not supported for Spark CDC");
  }

  private void createTable() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='2', 'write.update.mode'='copy-on-write')",
        tableName);
    sql("INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 'a'), (2, 'b')", tableName);
  }

  private Dataset<Row> changes(long start, long end, boolean computeUpdates) {
    return equivalentChanges(
        Map.of(
            "identifier-columns", "id",
            "startingVersion", String.valueOf(start),
            "endingVersion", String.valueOf(end),
            "computeUpdates", String.valueOf(computeUpdates)));
  }

  private Dataset<Row> equivalentChanges(Map<String, String> options) {
    Dataset<Row> sqlChanges = sqlChanges(options);
    Dataset<Row> dataFrameChanges = spark.read().options(options).changes(tableName);
    assertThat(dataFrameChanges.schema())
        .withFailMessage(
            "SQL schema: %s%nDataFrame schema: %s",
            sqlChanges.schema().json(), dataFrameChanges.schema().json())
        .isEqualTo(sqlChanges.schema());
    assertThat(dataFrameChanges.toJSON().collectAsList())
        .containsExactlyInAnyOrderElementsOf(sqlChanges.toJSON().collectAsList());
    return sqlChanges;
  }

  private Dataset<Row> sqlChanges(Map<String, String> options) {
    CaseInsensitiveStringMap nativeOptions = new CaseInsensitiveStringMap(options);
    String startVersion = nativeOptions.get("startingVersion");
    String startTimestamp = nativeOptions.get("startingTimestamp");
    String endVersion = nativeOptions.get("endingVersion");
    String endTimestamp = nativeOptions.get("endingTimestamp");
    StringBuilder range = new StringBuilder();
    if (startTimestamp != null) {
      range.append("TIMESTAMP ").append(quoted(startTimestamp));
    } else {
      long oldestVersion = Long.MAX_VALUE;
      for (Snapshot snapshot : validationCatalog.loadTable(tableIdent).snapshots()) {
        oldestVersion = Math.min(oldestVersion, snapshot.sequenceNumber());
      }
      range.append("VERSION ").append(startVersion != null ? startVersion : oldestVersion);
    }
    if (!nativeOptions.getBoolean("startingBoundInclusive", true)) {
      range.append(" EXCLUSIVE");
    }
    if (endVersion != null || endTimestamp != null) {
      range.append(" TO ");
      if (endTimestamp != null) {
        range.append("TIMESTAMP ").append(quoted(endTimestamp));
      } else {
        range.append("VERSION ").append(endVersion);
      }
      if (!nativeOptions.getBoolean("endingBoundInclusive", true)) {
        range.append(" EXCLUSIVE");
      }
    }
    String sqlOptions =
        options.entrySet().stream()
            .filter(entry -> !RANGE_OPTIONS.contains(entry.getKey().toLowerCase(Locale.ROOT)))
            .map(entry -> "`" + entry.getKey() + "` = " + quoted(entry.getValue()))
            .collect(Collectors.joining(", "));
    return spark.sql(
        String.format("SELECT * FROM %s CHANGES FROM %s WITH (%s)", tableName, range, sqlOptions));
  }

  private void assertRejected(Map<String, String> options, String message) {
    assertThatThrownBy(() -> sqlChanges(options).collectAsList()).hasStackTraceContaining(message);
    assertThatThrownBy(() -> spark.read().options(options).changes(tableName).collectAsList())
        .hasStackTraceContaining(message);
  }

  private Snapshot currentSnapshot() {
    return validationCatalog.loadTable(tableIdent).currentSnapshot();
  }

  private static Row cdcRow(long id, String data, String changeType, Snapshot snapshot) {
    return RowFactory.create(
        id,
        data,
        null,
        null,
        changeType,
        snapshot.sequenceNumber(),
        new Timestamp(snapshot.timestampMillis()));
  }

  private static List<Row> expectedRangeChanges(
      Snapshot update, Snapshot delete, Snapshot insert, boolean includeStart, boolean includeEnd) {
    List<Row> expected = Lists.newArrayList();
    if (includeStart) {
      expected.add(cdcRow(1L, "a", "update_preimage", update));
      expected.add(cdcRow(1L, "updated", "update_postimage", update));
    }
    expected.add(cdcRow(2L, "b", "delete", delete));
    if (includeEnd) {
      expected.add(cdcRow(3L, "c", "insert", insert));
    }
    return expected;
  }

  private static String timestamp(Snapshot snapshot) {
    return Instant.ofEpochMilli(snapshot.timestampMillis()).toString();
  }

  private static String quoted(String value) {
    return "'" + value.replace("'", "''") + "'";
  }
}
