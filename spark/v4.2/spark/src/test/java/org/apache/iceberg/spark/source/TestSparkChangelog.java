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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ChangelogUtil;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.ChangelogContext;
import org.apache.spark.sql.connector.catalog.ChangelogContext.DeduplicationMode;
import org.apache.spark.sql.connector.catalog.ChangelogRange;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
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
  void resumesRawStreamWithoutRowLineage() throws Exception {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='copy-on-write')",
        tableName);
    sql("INSERT INTO %s SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 'a'), (2, 'b')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    long firstVersion = table.currentSnapshot().sequenceNumber();
    Dataset<Row> changes =
        spark
            .readStream()
            .option("deduplicationMode", "none")
            .option("computeUpdates", "false")
            .option("startingVersion", String.valueOf(firstVersion))
            .changes(tableName);
    String checkpoint = temp.resolve("raw-cdc-checkpoint").toString();
    String output = temp.resolve("raw-cdc-output").toString();
    writeAvailableChanges(changes, checkpoint, output);
    assertThat(spark.read().parquet(output).count()).isEqualTo(2);

    sql("DELETE FROM %s WHERE id = 1", tableName);
    table.refresh();
    long deleteVersion = table.currentSnapshot().sequenceNumber();
    writeAvailableChanges(changes, checkpoint, output);
    Dataset<Row> result = spark.read().parquet(output);
    assertThat(result.select("id", "data", "_change_type", "_commit_version").collectAsList())
        .containsExactlyInAnyOrder(
            RowFactory.create(1L, "a", "insert", firstVersion),
            RowFactory.create(2L, "b", "insert", firstVersion),
            RowFactory.create(1L, "a", "delete", deleteVersion),
            RowFactory.create(2L, "b", "delete", deleteVersion),
            RowFactory.create(2L, "b", "insert", deleteVersion));
    assertThat(
            result
                .filter("_row_id IS NOT NULL OR _last_updated_sequence_number IS NOT NULL")
                .count())
        .isZero();
  }

  private void writeAvailableChanges(Dataset<Row> changes, String checkpoint, String output)
      throws Exception {
    StreamingQuery query =
        changes
            .writeStream()
            .format("parquet")
            .option("checkpointLocation", checkpoint)
            .trigger(Trigger.AvailableNow())
            .start(output);
    try {
      assertThat(query.awaitTermination(60_000)).isTrue();
    } finally {
      query.stop();
    }
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
        .hasStackTraceContaining("Business-key CDC requires IcebergSparkSessionExtensions");
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
          .hasStackTraceContaining("Spark CDC post-processing requires row lineage");
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
        .hasStackTraceContaining("format version 2 or later");

    sql("ALTER TABLE %s SET TBLPROPERTIES ('format-version'='2')", tableName);
    assertThatThrownBy(
            () ->
                spark.read().option("deduplicationMode", "none").changes(tableName).collectAsList())
        .hasStackTraceContaining("without a commit sequence number");
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
        .hasStackTraceContaining("Delete files are currently not supported in changelog scans");
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
  void availableNowPinsLatestSnapshot() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    long availableSnapshotId = table.currentSnapshot().snapshotId();
    SparkReadConf readConf = new SparkReadConf(spark, table, CaseInsensitiveStringMap.empty());
    SparkChangelogMicroBatchStream stream =
        new SparkChangelogMicroBatchStream(
            JavaSparkContext.fromSparkContext(spark.sparkContext()),
            table,
            readConf,
            SparkChangelogTable.cdcDataSchema(table),
            temp.resolve("cdc-available-now").toString(),
            new SparkChangelogRange(context(new ChangelogRange.UnboundedRange())));

    stream.prepareForTriggerAvailableNow();
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);

    StreamingOffset latestOffset = (StreamingOffset) stream.latestOffset();
    assertThat(latestOffset.snapshotId()).isEqualTo(availableSnapshotId);
    assertThat(latestOffset.position()).isZero();
    assertThat(stream.latestOffset(stream.initialOffset(), ReadLimit.allAvailable()))
        .isEqualTo(latestOffset);
    stream.stop();
  }

  @TestTemplate
  void streamsChangesUsingSparkCdcApi() throws Exception {
    String queryName = "iceberg_cdc_changes";
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);

    Dataset<Row> changes = spark.readStream().changes(tableName);
    StreamingQuery query =
        changes
            .writeStream()
            .format("memory")
            .queryName(queryName)
            .trigger(Trigger.AvailableNow())
            .start();
    query.awaitTermination();

    assertThat(sql("SELECT id, data, _change_type FROM %s ORDER BY id", queryName))
        .containsExactly(row(1L, "a", "insert"), row(2L, "b", "insert"));
    spark.catalog().dropTempView(queryName);
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
            new SparkChangelogTable(table, empty)
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
  void streamsNewCommitsWithOnlyStartingVersion() throws Exception {
    Table table = createCdcTable();
    ChangelogContext context =
        context(
            new ChangelogRange.VersionRange(
                String.valueOf(table.currentSnapshot().sequenceNumber()),
                Optional.empty(),
                true,
                true));
    MicroBatchStream stream = stream(table, context, "open-version");
    try {
      Offset firstEnd = stream.latestOffset();
      assertThat(readChanges(stream, stream.initialOffset(), firstEnd))
          .containsExactly(row(1L, "a", "insert", table.currentSnapshot().sequenceNumber()));
      sql("INSERT INTO %s VALUES (2, 'b')", tableName);
      Offset secondEnd = stream.latestOffset();
      assertThat(secondEnd).isNotEqualTo(firstEnd);
      assertThat(readChanges(stream, firstEnd, secondEnd))
          .containsExactly(row(2L, "b", "insert", table.currentSnapshot().sequenceNumber()));
    } finally {
      stream.stop();
    }
  }

  @TestTemplate
  void waitsForFutureTimestampWithoutReadingHistory() throws Exception {
    Table table = createCdcTable();
    long future = Long.MAX_VALUE;
    MicroBatchStream stream =
        stream(
            table,
            context(new ChangelogRange.TimestampRange(future, Optional.empty(), true, true)),
            "future");
    try {
      assertThat(readChanges(stream, stream.initialOffset(), stream.latestOffset())).isEmpty();
    } finally {
      stream.stop();
    }
  }

  @TestTemplate
  void preservesEmptyRangeInStreams() throws Exception {
    Table table = createCdcTable();
    String version = String.valueOf(table.currentSnapshot().sequenceNumber());
    MicroBatchStream stream =
        stream(
            table,
            context(new ChangelogRange.VersionRange(version, Optional.of(version), false, false)),
            "empty");
    try {
      assertThat(readChanges(stream, stream.initialOffset(), stream.latestOffset())).isEmpty();
    } finally {
      stream.stop();
    }
  }

  @TestTemplate
  void keepsReadSchemaAfterAddingColumn() throws Exception {
    Table table = createCdcTable();
    MicroBatchStream stream = stream(table, context(new ChangelogRange.UnboundedRange()), "schema");
    try {
      Offset firstEnd = stream.latestOffset();
      readChanges(stream, stream.initialOffset(), firstEnd);
      sql("ALTER TABLE %s ADD COLUMN extra string", tableName);
      sql("INSERT INTO %s VALUES (2, 'b', 'extra')", tableName);
      Offset secondEnd = stream.latestOffset();
      assertThat(readChanges(stream, firstEnd, secondEnd))
          .containsExactly(row(2L, "b", "insert", table.currentSnapshot().sequenceNumber()));
    } finally {
      stream.stop();
    }
  }

  @TestTemplate
  void rejectsIncompatibleStreamingSchemaChange() {
    Table table = createCdcTable();
    MicroBatchStream stream = stream(table, context(new ChangelogRange.UnboundedRange()), "drop");
    try {
      Offset firstEnd = stream.latestOffset();
      table.updateSchema().deleteColumn("data").commit();
      sql("INSERT INTO %s VALUES (2)", tableName);
      Offset secondEnd = stream.latestOffset();
      assertThatThrownBy(() -> stream.planInputPartitions(firstEnd, secondEnd))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("incompatible schema change");
    } finally {
      stream.stop();
    }
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
                new SparkChangelogTable(table, context)
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
    SparkChangelogRange range =
        new SparkChangelogRange(
            context(new ChangelogRange.TimestampRange(95000, Optional.of(115000L), true, true)));
    List<ScanTaskGroup<ChangelogScanTask>> groups =
        range.planTasks(
            skewed,
            table
                .newIncrementalChangelogScan()
                .project(ChangelogUtil.changelogSchema(SparkChangelogTable.cdcDataSchema(table))),
            null,
            third.snapshotId());
    assertThat(groups).isNotEmpty();
    assertThat(groups.stream().flatMap(group -> group.tasks().stream()))
        .extracting(ChangelogScanTask::commitSnapshotId)
        .containsOnly(first.snapshotId(), third.snapshotId());
  }

  @TestTemplate
  void rejectsLateCommitBeforeStreamingPostProcessing() {
    Table table = createCdcTable();
    Snapshot first = table.currentSnapshot();
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    table.refresh();
    Snapshot second = table.currentSnapshot();
    Table skewed =
        withTimestamps(table, Map.of(first.snapshotId(), 100L, second.snapshotId(), 100L));
    ChangelogContext context =
        new ChangelogContext(
            new ChangelogRange.UnboundedRange(), DeduplicationMode.DROP_CARRYOVERS, false);
    SparkChangelogMicroBatchStream stream =
        new SparkChangelogMicroBatchStream(
            JavaSparkContext.fromSparkContext(spark.sparkContext()),
            skewed,
            new SparkReadConf(spark, table, CaseInsensitiveStringMap.empty()),
            SparkChangelogTable.cdcDataSchema(table),
            temp.resolve("late").toString(),
            new SparkChangelogRange(context));
    try {
      assertThatThrownBy(
              () ->
                  stream.planTaskGroups(
                      new StreamingOffset(first.snapshotId(), 0, false),
                      new StreamingOffset(second.snapshotId(), 0, false)))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Cannot stream CDC post-processing");
    } finally {
      stream.stop();
    }
  }

  @TestTemplate
  void resumesFromCheckpointOffsetWithOpenRange() throws Exception {
    Table table = createCdcTable();
    ChangelogContext context = context(new ChangelogRange.UnboundedRange());
    MicroBatchStream first = stream(table, context, "restart");
    String endJson;
    try {
      Offset end = first.latestOffset();
      readChanges(first, first.initialOffset(), end);
      endJson = end.json();
    } finally {
      first.stop();
    }
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    MicroBatchStream resumed = stream(table, context, "restart");
    try {
      assertThat(readChanges(resumed, resumed.deserializeOffset(endJson), resumed.latestOffset()))
          .containsExactly(row(2L, "b", "insert", table.currentSnapshot().sequenceNumber()));
    } finally {
      resumed.stop();
    }
  }

  @TestTemplate
  void legacyChangelogAllowsCommitVersionDataColumn() {
    sql("CREATE TABLE %s (id bigint, _commit_version string) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'business-version')", tableName);
    assertThat(sql("SELECT id, _commit_version, _change_type FROM %s.changes", tableName))
        .containsExactly(row(1L, "business-version", "INSERT"));
  }

  @TestTemplate
  void readsInclusiveStartAfterItsParentExpires() throws Exception {
    Table table = createCdcTable();
    long parentId = table.currentSnapshot().snapshotId();
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    table.refresh();
    String version = String.valueOf(table.currentSnapshot().sequenceNumber());
    table.expireSnapshots().expireSnapshotId(parentId).cleanExpiredFiles(false).commit();
    MicroBatchStream stream =
        stream(
            table,
            context(new ChangelogRange.VersionRange(version, Optional.empty(), true, true)),
            "expired-parent");
    try {
      assertThat(readChanges(stream, stream.initialOffset(), stream.latestOffset()))
          .containsExactly(row(2L, "b", "insert", table.currentSnapshot().sequenceNumber()));
    } finally {
      stream.stop();
    }
  }

  @TestTemplate
  void admissionKeepsEqualTimestampSnapshotsTogether() {
    Table table = createCdcTable();
    Snapshot first = table.currentSnapshot();
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    table.refresh();
    Snapshot second = table.currentSnapshot();
    sql("INSERT INTO %s VALUES (3, 'c'), (4, 'd')", tableName);
    table.refresh();
    Snapshot third = table.currentSnapshot();
    Table controlled =
        withTimestamps(
            table,
            Map.of(first.snapshotId(), 100L, second.snapshotId(), 100L, third.snapshotId(), 110L));
    SparkChangelogMicroBatchStream stream =
        new SparkChangelogMicroBatchStream(
            JavaSparkContext.fromSparkContext(spark.sparkContext()),
            controlled,
            new SparkReadConf(spark, table, CaseInsensitiveStringMap.empty()),
            SparkChangelogTable.cdcDataSchema(table),
            temp.resolve("admission").toString(),
            new SparkChangelogRange(context(new ChangelogRange.UnboundedRange())));
    try {
      StreamingOffset end =
          (StreamingOffset) stream.latestOffset(stream.initialOffset(), ReadLimit.maxRows(1));
      assertThat(end.snapshotId()).isEqualTo(second.snapshotId());
      StreamingOffset next = (StreamingOffset) stream.latestOffset(end, ReadLimit.maxRows(1));
      assertThat(next.snapshotId()).isEqualTo(third.snapshotId());
      assertThat(next.position()).isZero();
      // Raw CDC does not use Spark's event-time watermark and can read equal timestamps.
      assertThat(stream.planTaskGroups(new StreamingOffset(first.snapshotId(), 0, false), end))
          .isNotEmpty();
    } finally {
      stream.stop();
    }
  }

  @TestTemplate
  void legacyChangelogProjectsFileAndChangeMetadataTogether() {
    Table table = createCdcTable();
    assertThat(
            sql("SELECT id, _change_type, _commit_snapshot_id, _file FROM %s.changes", tableName))
        .singleElement()
        .satisfies(
            row -> {
              assertThat(row[0]).isEqualTo(1L);
              assertThat(row[1]).isEqualTo("INSERT");
              assertThat(row[2]).isEqualTo(table.currentSnapshot().snapshotId());
              assertThat(row[3]).isInstanceOf(String.class);
              assertThat((String) row[3]).contains(".parquet");
            });
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

  private MicroBatchStream stream(Table table, ChangelogContext context, String checkpoint) {
    return new SparkChangelogTable(table, context)
        .newScanBuilder(CaseInsensitiveStringMap.empty())
        .build()
        .toMicroBatchStream(temp.resolve(checkpoint).toString());
  }

  private List<Object[]> readChanges(MicroBatchStream stream, Offset start, Offset end)
      throws Exception {
    List<Object[]> rows = Lists.newArrayList();
    for (InputPartition partition : stream.planInputPartitions(start, end)) {
      try (PartitionReader<InternalRow> reader =
          stream.createReaderFactory().createReader(partition)) {
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

    assertThat(sql("SELECT id, data, _change_type, _commit_snapshot_id FROM %s.changes", tableName))
        .hasSize(1)
        .allSatisfy(
            row -> {
              assertThat(row[2]).isEqualTo("INSERT");
              assertThat(row[3]).isInstanceOf(Long.class);
            });
  }
}
