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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ChangelogUtil;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.ChangelogContext;
import org.apache.spark.sql.connector.catalog.ChangelogContext.DeduplicationMode;
import org.apache.spark.sql.connector.catalog.ChangelogRange;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

class TestSparkChangelog extends TestBaseWithCatalog {

  @AfterEach
  void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  void readsRawCopyOnWriteChangesWithoutRowLineage() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='2', 'write.update.mode'='copy-on-write')",
        tableName);
    sql("INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 'a'), (2, 'b')", tableName);
    sql("UPDATE %s SET data = 'updated' WHERE id = 1", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = table.currentSnapshot();
    long version = snapshot.sequenceNumber();
    Timestamp timestamp = new Timestamp(snapshot.timestampMillis());
    Dataset<Row> changes =
        spark.sql(
            String.format(
                "SELECT * FROM %s CHANGES FROM VERSION %d TO VERSION %d "
                    + "WITH (deduplicationMode = 'none', computeUpdates = 'false')",
                tableName, version, version));

    assertThat(changes.schema().fieldNames())
        .containsExactly(
            "id",
            "data",
            "_row_id",
            "_last_updated_sequence_number",
            "_change_type",
            "_commit_version",
            "_commit_timestamp");
    assertThat(changes.schema().apply("_row_id").nullable()).isTrue();
    assertThat(changes.schema().apply("_last_updated_sequence_number").nullable()).isTrue();
    assertThat(changes.collectAsList())
        .containsExactlyInAnyOrder(
            RowFactory.create(1L, "a", null, null, "delete", version, timestamp),
            RowFactory.create(1L, "updated", null, null, "insert", version, timestamp),
            RowFactory.create(2L, "b", null, null, "delete", version, timestamp),
            RowFactory.create(2L, "b", null, null, "insert", version, timestamp));
  }

  @TestTemplate
  void rejectsBusinessKeyReadWithoutExtensions() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='2')",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    assertThatThrownBy(
            () ->
                spark
                    .read()
                    .option("identifier-columns", "id")
                    .option("computeUpdates", "true")
                    .changes(tableName)
                    .collectAsList())
        .hasMessageContaining("Business-key CDC requires IcebergSparkSessionExtensions");
  }

  @TestTemplate
  void rejectsPostProcessingWithoutRowLineage() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='2')",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    List<Map<String, String>> options =
        List.of(
            Map.of(),
            Map.of("deduplicationMode", "dropCarryovers"),
            Map.of("deduplicationMode", "netChanges"),
            Map.of("computeUpdates", "true"),
            Map.of("deduplicationMode", "none", "computeUpdates", "true"));
    for (Map<String, String> option : options) {
      assertThatThrownBy(() -> spark.read().options(option).changes(tableName).collectAsList())
          .hasMessageContaining("Spark CDC post-processing requires row lineage");
    }
  }

  @TestTemplate
  void rejectsRawCdcWithoutCommitSequenceNumbers() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='1')",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    assertThatThrownBy(
            () ->
                spark.read().option("deduplicationMode", "none").changes(tableName).collectAsList())
        .hasMessageContaining("format version 2 or later");

    sql("ALTER TABLE %s SET TBLPROPERTIES ('format-version'='2')", tableName);
    assertThatThrownBy(
            () ->
                spark.read().option("deduplicationMode", "none").changes(tableName).collectAsList())
        .hasMessageContaining("without a commit sequence number");
  }

  @TestTemplate
  void rawCdcStillRejectsDeleteFiles() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')",
        tableName);
    sql("INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 'a'), (2, 'b')", tableName);
    sql("DELETE FROM %s WHERE id = 1", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.currentSnapshot().deleteManifests(table.io())).isNotEmpty();
    assertThatThrownBy(
            () ->
                spark.read().option("deduplicationMode", "none").changes(tableName).collectAsList())
        .hasMessageContaining("Delete files are currently not supported in changelog scans");
  }

  @TestTemplate
  void readsChangesUsingSparkCdcSyntax() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    long firstVersion = table.currentSnapshot().sequenceNumber();

    sql("INSERT INTO %s VALUES (3, 'c')", tableName);
    table.refresh();
    long secondVersion = table.currentSnapshot().sequenceNumber();

    assertThat(
            sql(
                "SELECT id, data, _change_type, _commit_version "
                    + "FROM %s CHANGES FROM VERSION %d TO VERSION %d ORDER BY id",
                tableName, firstVersion, secondVersion))
        .containsExactly(
            row(1L, "a", "insert", firstVersion),
            row(2L, "b", "insert", firstVersion),
            row(3L, "c", "insert", secondVersion));
  }

  @TestTemplate
  void readsCopyOnWriteChangesUsingSparkCdcSyntax() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);
    sql("INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 'a'), (2, 'b')", tableName);

    sql("DELETE FROM %s WHERE id = 1", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    long deleteVersion = table.currentSnapshot().sequenceNumber();
    assertThat(table.currentSnapshot().deleteManifests(table.io())).isEmpty();

    assertThat(
            sql(
                "SELECT id, data, _change_type, _commit_version "
                    + "FROM %s CHANGES FROM VERSION %d TO VERSION %d "
                    + "ORDER BY _change_type, id",
                tableName, deleteVersion, deleteVersion))
        .containsExactly(row(1L, "a", "delete", deleteVersion));

    assertThat(
            sql(
                "SELECT id, data, _change_type, _commit_version "
                    + "FROM %s CHANGES FROM VERSION %d TO VERSION %d "
                    + "WITH (deduplicationMode = 'none') "
                    + "ORDER BY _change_type, id",
                tableName, deleteVersion, deleteVersion))
        .containsExactly(
            row(1L, "a", "delete", deleteVersion),
            row(2L, "b", "delete", deleteVersion),
            row(2L, "b", "insert", deleteVersion));
  }

  @TestTemplate
  void computesUpdatesForCopyOnWriteChanges() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

    sql("UPDATE %s SET data = 'updated' WHERE id = 1", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    long updateVersion = table.currentSnapshot().sequenceNumber();

    assertThat(
            sql(
                "SELECT id, data, _change_type, _commit_version "
                    + "FROM %s CHANGES FROM VERSION %d TO VERSION %d "
                    + "WITH (computeUpdates = 'true') ORDER BY data",
                tableName, updateVersion, updateVersion))
        .containsExactly(
            row(1L, "a", "update_preimage", updateVersion),
            row(1L, "updated", "update_postimage", updateVersion));
  }

  @TestTemplate
  void batchScanPinsSnapshotAtBuild() throws Exception {
    Table table = createCdcTable();
    long firstVersion = table.currentSnapshot().sequenceNumber();
    SparkChangelog changelog =
        new SparkChangelog(table, context(new ChangelogRange.UnboundedRange()));
    Scan pinned = changelog.newScanBuilder(CaseInsensitiveStringMap.empty()).build();

    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    table.refresh();
    long secondVersion = table.currentSnapshot().sequenceNumber();

    assertThat(readChanges(pinned.toBatch())).containsExactly(row(1L, "a", "insert", firstVersion));
    Scan latest = changelog.newScanBuilder(CaseInsensitiveStringMap.empty()).build();
    assertThat(readChanges(latest.toBatch()))
        .containsExactlyInAnyOrder(
            row(1L, "a", "insert", firstVersion), row(2L, "b", "insert", secondVersion));
  }

  @TestTemplate
  void keepsDisjointCdcRangesSeparateWithExchangeReuse() {
    Table table = createCdcTable();
    long firstVersion = table.currentSnapshot().sequenceNumber();
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    table.refresh();
    long secondVersion = table.currentSnapshot().sequenceNumber();

    withSQLConf(
        Map.of("spark.sql.adaptive.enabled", "false", "spark.sql.exchange.reuse", "true"),
        () ->
            assertThat(
                    sql(
                        "SELECT /*+ REPARTITION(2) */ id, data, _commit_version FROM %s "
                            + "CHANGES FROM VERSION %d TO VERSION %d "
                            + "WITH (deduplicationMode = 'none') UNION ALL "
                            + "SELECT /*+ REPARTITION(2) */ id, data, _commit_version FROM %s "
                            + "CHANGES FROM VERSION %d TO VERSION %d "
                            + "WITH (deduplicationMode = 'none')",
                        tableName,
                        firstVersion,
                        firstVersion,
                        tableName,
                        secondVersion,
                        secondVersion))
                .containsExactlyInAnyOrder(
                    row(1L, "a", firstVersion), row(2L, "b", secondVersion)));
  }

  @TestTemplate
  void nativeStreamIsUnsupported() {
    createCdcTable();
    String queryName = "native_cdc_unsupported";
    assertThatThrownBy(
            () -> {
              StreamingQuery query = null;
              try {
                query =
                    spark
                        .readStream()
                        .option("deduplicationMode", "none")
                        .changes(tableName)
                        .writeStream()
                        .format("memory")
                        .queryName(queryName)
                        .option("checkpointLocation", temp.resolve("unsupported-stream").toString())
                        .trigger(Trigger.AvailableNow())
                        .start();
                query.awaitTermination(60_000);
              } finally {
                if (query != null) {
                  query.stop();
                }
                spark.catalog().dropTempView(queryName);
              }
            })
        .hasStackTraceContaining("DATA_SOURCE_MICRO_BATCH_SCAN_NOT_SUPPORTED");
  }

  @TestTemplate
  void readsExclusiveVersionBounds() {
    Table table = createCdcTable();
    long first = table.currentSnapshot().sequenceNumber();
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    table.refresh();
    long second = table.currentSnapshot().sequenceNumber();
    ChangelogContext empty =
        context(
            new ChangelogRange.VersionRange(
                String.valueOf(first), Optional.of(String.valueOf(first)), false, false));
    assertThat(
            new SparkChangelog(table, empty)
                .newScanBuilder(CaseInsensitiveStringMap.empty())
                .build()
                .toBatch()
                .planInputPartitions())
        .isEmpty();
    assertThat(
            spark
                .read()
                .option("startingVersion", first)
                .option("endingVersion", second)
                .option("startingBoundInclusive", false)
                .changes(tableName)
                .select("id")
                .collectAsList())
        .containsExactly(RowFactory.create(2L));
  }

  @TestTemplate
  void readsRawHistoryButRejectsPostProcessingAfterUpgrade() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='2')",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    long version = table.currentSnapshot().sequenceNumber();
    sql("ALTER TABLE %s SET TBLPROPERTIES ('format-version'='3')", tableName);
    table.refresh();
    assertThat(
            spark
                .read()
                .option("deduplicationMode", "none")
                .option("startingVersion", String.valueOf(version))
                .option("endingVersion", String.valueOf(version))
                .changes(tableName)
                .select("id", "data", "_row_id", "_last_updated_sequence_number", "_change_type")
                .collectAsList())
        .containsExactly(RowFactory.create(1L, "a", null, null, "insert"));
    ChangelogContext context =
        new ChangelogContext(
            new ChangelogRange.VersionRange(
                String.valueOf(version), Optional.of(String.valueOf(version)), true, true),
            DeduplicationMode.DROP_CARRYOVERS,
            false);
    assertThatThrownBy(
            () ->
                new SparkChangelog(table, context)
                    .newScanBuilder(CaseInsensitiveStringMap.empty())
                    .build()
                    .toBatch()
                    .planInputPartitions())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("without row lineage");
  }

  @TestTemplate
  void filtersTimestampRangesWhenCommitTimeMovesBackwards() {
    Table table = createCdcTable();
    Snapshot first = table.currentSnapshot();
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    table.refresh();
    Snapshot second = table.currentSnapshot();
    sql("INSERT INTO %s VALUES (3, 'c')", tableName);
    table.refresh();
    Snapshot third = table.currentSnapshot();
    Table skewed =
        withTimestamps(
            table,
            Map.of(first.snapshotId(), 100L, second.snapshotId(), 90L, third.snapshotId(), 110L));
    ChangelogContext context =
        context(new ChangelogRange.TimestampRange(95000, Optional.of(115000L), true, true));
    List<ScanTaskGroup<ChangelogScanTask>> groups =
        SparkChangelog.planTasks(
            skewed,
            table
                .newIncrementalChangelogScan()
                .project(ChangelogUtil.changelogSchema(SparkChangelog.cdcDataSchema(table))),
            context,
            third.snapshotId());
    assertThat(groups).isNotEmpty();
    assertThat(groups.stream().flatMap(group -> group.tasks().stream()))
        .extracting(ChangelogScanTask::commitSnapshotId)
        .containsOnly(first.snapshotId(), third.snapshotId());
  }

  @TestTemplate
  void legacyChangelogAllowsCommitVersionDataColumn() {
    sql("CREATE TABLE %s (id bigint, _commit_version string) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'business-version')", tableName);
    assertThat(sql("SELECT id, _commit_version, _change_type FROM %s.changes", tableName))
        .containsExactly(row(1L, "business-version", "INSERT"));
  }

  @TestTemplate
  void nativeBatchOptionsAgreeAcrossSqlAndDataFrame() {
    NativeHistory history = createNativeHistory();
    for (String mode : List.of("none", "dropCarryovers", "netChanges")) {
      for (boolean computeUpdates : List.of(false, true)) {
        Map<String, String> options = nativeOptions(history, mode, computeUpdates);
        String query = nativeSql(history, mode, computeUpdates);
        if (mode.equals("none") && computeUpdates) {
          assertThatThrownBy(() -> spark.read().options(options).changes(tableName).collectAsList())
              .hasMessageContaining("UPDATE_DETECTION_REQUIRES_CARRY_OVER_REMOVAL");
          assertThatThrownBy(() -> spark.sql(query).collectAsList())
              .hasMessageContaining("UPDATE_DETECTION_REQUIRES_CARRY_OVER_REMOVAL");
          continue;
        }

        List<Row> expected = expectedNativeChanges(history, mode, computeUpdates);
        Dataset<Row> changes = spark.read().options(options).changes(tableName);
        assertThat(changes.collectAsList())
            .as("DataFrame %s, computeUpdates=%s", mode, computeUpdates)
            .containsExactlyInAnyOrderElementsOf(expected);
        assertThat(spark.sql(query).collectAsList())
            .as("SQL %s, computeUpdates=%s", mode, computeUpdates)
            .containsExactlyInAnyOrderElementsOf(expected);
        assertThat(changes.limit(expected.size()).collectAsList())
            .as(
                "DataFrame LIMIT must follow %s post-processing, computeUpdates=%s",
                mode, computeUpdates)
            .containsExactlyInAnyOrderElementsOf(expected);
        assertThat(spark.sql(query + " LIMIT " + expected.size()).collectAsList())
            .as("SQL LIMIT must follow %s post-processing, computeUpdates=%s", mode, computeUpdates)
            .containsExactlyInAnyOrderElementsOf(expected);

        List<Row> filtered =
            expected.stream()
                .filter(row -> row.getString(1).equals("updated"))
                .map(row -> RowFactory.create(row.getString(4), row.getLong(5)))
                .toList();
        assertThat(
                changes
                    .filter("data = 'updated'")
                    .select("_change_type", "_commit_version")
                    .collectAsList())
            .as("Filtering must follow %s post-processing", mode)
            .containsExactlyInAnyOrderElementsOf(filtered);
        assertThat(changes.select("_change_type").collectAsList())
            .containsExactlyInAnyOrderElementsOf(
                expected.stream().map(row -> RowFactory.create(row.getString(4))).toList());
      }
    }

    Map<String, String> bounds =
        Map.of(
            "startingVersion", String.valueOf(history.firstUpdate().sequenceNumber()),
            "endingVersion", String.valueOf(history.transientDelete().sequenceNumber()));
    List<Row> expectedDefault = expectedNativeChanges(history, "dropCarryovers", false);
    assertThat(spark.read().options(bounds).changes(tableName).collectAsList())
        .containsExactlyInAnyOrderElementsOf(expectedDefault);
    assertThat(
            spark
                .sql(
                    String.format(
                        "SELECT * FROM %s CHANGES FROM VERSION %d TO VERSION %d",
                        tableName,
                        history.firstUpdate().sequenceNumber(),
                        history.transientDelete().sequenceNumber()))
                .collectAsList())
        .containsExactlyInAnyOrderElementsOf(expectedDefault);
    assertThat(
            spark
                .read()
                .options(bounds)
                .option("DeDuPliCaTiOnMoDe", "DROPCARRYOVERS")
                .option("CoMpUtEuPdAtEs", "TrUe")
                .changes(tableName)
                .collectAsList())
        .containsExactlyInAnyOrderElementsOf(
            expectedNativeChanges(history, "dropCarryovers", true));
  }

  @TestTemplate
  void nativeVersionAndTimestampBoundsAgreeAcrossApis() {
    List<Snapshot> snapshots = createBoundaryHistory();
    for (boolean timestamps : List.of(false, true)) {
      for (boolean startInclusive : List.of(false, true)) {
        for (boolean endInclusive : List.of(false, true)) {
          List<Row> expected = boundaryRows(snapshots, startInclusive, endInclusive);
          assertThat(
                  spark
                      .read()
                      .options(boundaryOptions(snapshots, timestamps, startInclusive, endInclusive))
                      .changes(tableName)
                      .select("id", "_commit_version", "_commit_timestamp")
                      .collectAsList())
              .containsExactlyInAnyOrderElementsOf(expected);
          assertThat(
                  spark
                      .sql(boundarySql(snapshots, timestamps, startInclusive, endInclusive))
                      .collectAsList())
              .containsExactlyInAnyOrderElementsOf(expected);
        }
      }
    }

    String justAfterFirst =
        Instant.ofEpochMilli(snapshots.get(0).timestampMillis()).plusNanos(1000).toString();
    String justBeforeLast =
        Instant.ofEpochMilli(snapshots.get(2).timestampMillis()).minusNanos(1000).toString();
    List<Row> middle = boundaryRows(snapshots, false, false);
    assertThat(
            spark
                .read()
                .option("startingTimestamp", justAfterFirst)
                .option("endingTimestamp", justBeforeLast)
                .option("deduplicationMode", "none")
                .changes(tableName)
                .select("id", "_commit_version", "_commit_timestamp")
                .collectAsList())
        .containsExactlyInAnyOrderElementsOf(middle);
    assertThat(
            spark
                .sql(
                    String.format(
                        "SELECT id, _commit_version, _commit_timestamp FROM %s "
                            + "CHANGES FROM TIMESTAMP '%s' TO TIMESTAMP '%s' "
                            + "WITH (deduplicationMode = 'none')",
                        tableName, justAfterFirst, justBeforeLast))
                .collectAsList())
        .containsExactlyInAnyOrderElementsOf(middle);
  }

  @TestTemplate
  void nativeOptionsRejectInvalidRangesAndModes() {
    List<Snapshot> snapshots = createBoundaryHistory();
    String first = String.valueOf(snapshots.get(0).sequenceNumber());
    String last = String.valueOf(snapshots.get(2).sequenceNumber());
    List<Map<String, String>> invalidOptions =
        List.of(
            Map.of("deduplicationMode", "unsupported"),
            Map.of("startingVersion", first, "startingTimestamp", "2020-01-01"),
            Map.of("endingVersion", last),
            Map.of("endingTimestamp", "2020-01-01"),
            Map.of("startingTimestamp", "not-a-timestamp"));
    for (Map<String, String> options : invalidOptions) {
      assertThatThrownBy(() -> spark.read().options(options).changes(tableName).collectAsList())
          .hasMessageContaining("INVALID_CDC_OPTION");
    }

    assertThatThrownBy(
            () ->
                spark
                    .read()
                    .option("startingVersion", "not-a-version")
                    .changes(tableName)
                    .collectAsList())
        .hasMessageContaining("Invalid Iceberg snapshot sequence number");
    assertThatThrownBy(
            () ->
                spark
                    .read()
                    .option("startingVersion", Long.MAX_VALUE)
                    .changes(tableName)
                    .collectAsList())
        .hasMessageContaining("Cannot find Iceberg snapshot with sequence number");

    for (boolean timestamps : List.of(false, true)) {
      String kind = timestamps ? "Timestamp" : "Version";
      String start = nativeBound(snapshots.get(2), timestamps);
      String end = nativeBound(snapshots.get(0), timestamps);
      assertThatThrownBy(
              () ->
                  spark
                      .read()
                      .option("starting" + kind, start)
                      .option("ending" + kind, end)
                      .changes(tableName)
                      .collectAsList())
          .hasMessageContaining("starting" + kind)
          .hasMessageContaining("ending" + kind);
      String sqlStart = timestamps ? "'" + start + "'" : start;
      String sqlEnd = timestamps ? "'" + end + "'" : end;
      assertThatThrownBy(
              () ->
                  spark
                      .sql(
                          String.format(
                              "SELECT * FROM %s CHANGES FROM %s %s TO %s %s",
                              tableName, kind, sqlStart, kind, sqlEnd))
                      .collectAsList())
          .hasMessageContaining("starting" + kind)
          .hasMessageContaining("ending" + kind);
    }
  }

  private NativeHistory createNativeHistory() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3', 'write.update.mode'='copy-on-write', "
            + "'write.delete.mode'='copy-on-write')",
        tableName);
    sql("INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 'a'), (2, 'b')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot initial = table.currentSnapshot();
    Map<Long, Long> rowIds = Maps.newHashMap();
    for (Row row : spark.table(tableName).select("id", "_row_id").collectAsList()) {
      rowIds.put(row.getLong(0), row.getLong(1));
    }

    waitUntilAfter(initial.timestampMillis());
    sql("UPDATE %s SET data = 'updated' WHERE id = 1", tableName);
    table.refresh();
    Snapshot firstUpdate = table.currentSnapshot();
    waitUntilAfter(firstUpdate.timestampMillis());
    sql("UPDATE %s SET data = 'final' WHERE id = 1", tableName);
    table.refresh();
    Snapshot secondUpdate = table.currentSnapshot();
    waitUntilAfter(secondUpdate.timestampMillis());
    sql("DELETE FROM %s WHERE id = 2", tableName);
    table.refresh();
    Snapshot delete = table.currentSnapshot();
    waitUntilAfter(delete.timestampMillis());
    sql("INSERT INTO %s VALUES (3, 'transient')", tableName);
    table.refresh();
    Snapshot transientInsert = table.currentSnapshot();
    rowIds.put(3L, spark.table(tableName).filter("id = 3").select("_row_id").first().getLong(0));
    waitUntilAfter(transientInsert.timestampMillis());
    sql("DELETE FROM %s WHERE id = 3", tableName);
    table.refresh();
    return new NativeHistory(
        initial,
        firstUpdate,
        secondUpdate,
        delete,
        transientInsert,
        table.currentSnapshot(),
        rowIds);
  }

  private Map<String, String> nativeOptions(
      NativeHistory history, String mode, boolean computeUpdates) {
    return Map.of(
        "startingVersion", String.valueOf(history.firstUpdate().sequenceNumber()),
        "endingVersion", String.valueOf(history.transientDelete().sequenceNumber()),
        "deduplicationMode", mode,
        "computeUpdates", String.valueOf(computeUpdates));
  }

  private String nativeSql(NativeHistory history, String mode, boolean computeUpdates) {
    return String.format(
        "SELECT * FROM %s CHANGES FROM VERSION %d TO VERSION %d "
            + "WITH (deduplicationMode = '%s', computeUpdates = '%s')",
        tableName,
        history.firstUpdate().sequenceNumber(),
        history.transientDelete().sequenceNumber(),
        mode,
        computeUpdates);
  }

  private List<Row> expectedNativeChanges(
      NativeHistory history, String mode, boolean computeUpdates) {
    Snapshot initial = history.initial();
    Snapshot first = history.firstUpdate();
    Snapshot second = history.secondUpdate();
    Snapshot delete = history.delete();
    Snapshot insert = history.transientInsert();
    Snapshot remove = history.transientDelete();
    if (mode.equals("none")) {
      return List.of(
          nativeEvent(history, 1, "a", "delete", first, initial),
          nativeEvent(history, 2, "b", "delete", first, initial),
          nativeEvent(history, 1, "updated", "insert", first, first),
          nativeEvent(history, 2, "b", "insert", first, initial),
          nativeEvent(history, 1, "updated", "delete", second, first),
          nativeEvent(history, 2, "b", "delete", second, initial),
          nativeEvent(history, 1, "final", "insert", second, second),
          nativeEvent(history, 2, "b", "insert", second, initial),
          nativeEvent(history, 1, "final", "delete", delete, second),
          nativeEvent(history, 2, "b", "delete", delete, initial),
          nativeEvent(history, 1, "final", "insert", delete, second),
          nativeEvent(history, 3, "transient", "insert", insert, insert),
          nativeEvent(history, 3, "transient", "delete", remove, insert));
    }

    String before = computeUpdates ? "update_preimage" : "delete";
    String after = computeUpdates ? "update_postimage" : "insert";
    if (mode.equals("netChanges")) {
      return List.of(
          nativeEvent(history, 1, "a", before, first, initial),
          nativeEvent(history, 1, "final", after, second, second),
          nativeEvent(history, 2, "b", "delete", delete, initial));
    }

    return List.of(
        nativeEvent(history, 1, "a", before, first, initial),
        nativeEvent(history, 1, "updated", after, first, first),
        nativeEvent(history, 1, "updated", before, second, first),
        nativeEvent(history, 1, "final", after, second, second),
        nativeEvent(history, 2, "b", "delete", delete, initial),
        nativeEvent(history, 3, "transient", "insert", insert, insert),
        nativeEvent(history, 3, "transient", "delete", remove, insert));
  }

  private static Row nativeEvent(
      NativeHistory history,
      long id,
      String data,
      String change,
      Snapshot commit,
      Snapshot rowWrite) {
    return RowFactory.create(
        id,
        data,
        history.rowIds().get(id),
        rowWrite.sequenceNumber(),
        change,
        commit.sequenceNumber(),
        new Timestamp(commit.timestampMillis()));
  }

  private record NativeHistory(
      Snapshot initial,
      Snapshot firstUpdate,
      Snapshot secondUpdate,
      Snapshot delete,
      Snapshot transientInsert,
      Snapshot transientDelete,
      Map<Long, Long> rowIds) {}

  private List<Snapshot> createBoundaryHistory() {
    Table table = createCdcTable();
    List<Snapshot> snapshots = Lists.newArrayList(table.currentSnapshot());
    waitUntilAfter(table.currentSnapshot().timestampMillis());
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    table.refresh();
    snapshots.add(table.currentSnapshot());
    waitUntilAfter(table.currentSnapshot().timestampMillis());
    sql("INSERT INTO %s VALUES (3, 'c')", tableName);
    table.refresh();
    snapshots.add(table.currentSnapshot());
    return snapshots;
  }

  private static String nativeBound(Snapshot snapshot, boolean timestamps) {
    return timestamps
        ? Instant.ofEpochMilli(snapshot.timestampMillis()).toString()
        : String.valueOf(snapshot.sequenceNumber());
  }

  private static Map<String, String> boundaryOptions(
      List<Snapshot> snapshots, boolean timestamps, boolean startInclusive, boolean endInclusive) {
    String kind = timestamps ? "Timestamp" : "Version";
    return Map.of(
        "starting" + kind,
        nativeBound(snapshots.get(0), timestamps),
        "ending" + kind,
        nativeBound(snapshots.get(2), timestamps),
        "startingBoundInclusive",
        String.valueOf(startInclusive),
        "endingBoundInclusive",
        String.valueOf(endInclusive),
        "deduplicationMode",
        "none");
  }

  private String boundarySql(
      List<Snapshot> snapshots, boolean timestamps, boolean startInclusive, boolean endInclusive) {
    String kind = timestamps ? "TIMESTAMP" : "VERSION";
    String start = nativeBound(snapshots.get(0), timestamps);
    String end = nativeBound(snapshots.get(2), timestamps);
    return String.format(
        "SELECT id, _commit_version, _commit_timestamp FROM %s "
            + "CHANGES FROM %s %s %s TO %s %s %s WITH (deduplicationMode = 'none')",
        tableName,
        kind,
        timestamps ? "'" + start + "'" : start,
        startInclusive ? "INCLUSIVE" : "EXCLUSIVE",
        kind,
        timestamps ? "'" + end + "'" : end,
        endInclusive ? "INCLUSIVE" : "EXCLUSIVE");
  }

  private static List<Row> boundaryRows(
      List<Snapshot> snapshots, boolean startInclusive, boolean endInclusive) {
    List<Row> expected = Lists.newArrayList();
    if (startInclusive) {
      expected.add(boundaryRow(1, snapshots.get(0)));
    }

    expected.add(boundaryRow(2, snapshots.get(1)));
    if (endInclusive) {
      expected.add(boundaryRow(3, snapshots.get(2)));
    }

    return expected;
  }

  private static Row boundaryRow(long id, Snapshot commit) {
    return RowFactory.create(id, commit.sequenceNumber(), new Timestamp(commit.timestampMillis()));
  }

  private Table createCdcTable() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    return validationCatalog.loadTable(tableIdent);
  }

  private static ChangelogContext context(ChangelogRange range) {
    return new ChangelogContext(range, DeduplicationMode.NONE, false);
  }

  private List<Object[]> readChanges(Batch batch) throws Exception {
    PartitionReaderFactory factory = batch.createReaderFactory();
    List<Object[]> rows = Lists.newArrayList();
    for (InputPartition partition : batch.planInputPartitions()) {
      try (PartitionReader<InternalRow> reader = factory.createReader(partition)) {
        while (reader.next()) {
          InternalRow row = reader.get();
          rows.add(
              row(
                  row.getLong(0),
                  row.getUTF8String(1).toString(),
                  row.getUTF8String(4).toString(),
                  row.getLong(5)));
        }
      }
    }
    return rows;
  }

  private static Table withTimestamps(Table table, Map<Long, Long> timestamps) {
    Table controlled = mock(Table.class, delegatesTo(table));
    for (Snapshot snapshot : table.snapshots()) {
      Snapshot timed = mock(Snapshot.class, delegatesTo(snapshot));
      when(timed.timestampMillis()).thenReturn(timestamps.get(snapshot.snapshotId()));
      when(controlled.snapshot(snapshot.snapshotId())).thenReturn(timed);
      if (snapshot.snapshotId() == table.currentSnapshot().snapshotId()) {
        when(controlled.currentSnapshot()).thenReturn(timed);
      }
    }
    return controlled;
  }

  @TestTemplate
  void tableChangesKeepsIcebergChangelogColumns() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);

    assertThat(spark.table(tableName + ".changes").schema().fieldNames())
        .containsExactly("id", "data", "_change_type", "_change_ordinal", "_commit_snapshot_id");
    long snapshotId = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
    assertThat(sql("SELECT * FROM %s.changes", tableName))
        .containsExactly(row(1L, "a", "INSERT", 0, snapshotId));
  }
}
