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
package org.apache.iceberg.flink.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkReadConf;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestFlinkTableStatistics {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "score", Types.DoubleType.get()));

  @TempDir private Path warehouse;
  @TempDir private Path appendDir;

  private Table table;
  private GenericAppenderHelper appender;

  @BeforeEach
  public void createTable() {
    this.table =
        new HadoopTables()
            .create(
                SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of("format-version", "2"),
                warehouse.resolve("tbl").toString());
    this.appender = new GenericAppenderHelper(table, FileFormat.PARQUET, appendDir);
  }

  private Record record(int id, String data, Double score) {
    Record record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    record.setField("score", score);
    return record;
  }

  private TableStats stats(List<Expression> filters, boolean columnStatsEnabled) {
    FlinkReadConf readConf = new FlinkReadConf(table, ImmutableMap.of(), new Configuration());
    return FlinkTableStatistics.reportStatistics(table, readConf, filters, columnStatsEnabled);
  }

  /** Two files: ids 1-3 (one null data, one null score) and ids 4-5. */
  private void appendTwoFiles() throws Exception {
    appender.appendToTable(
        ImmutableList.of(record(1, "a", 1.0), record(2, null, 2.5), record(3, "c", null)));
    appender.appendToTable(ImmutableList.of(record(4, "d", 4.0), record(5, "e", 5.0)));
  }

  @Test
  public void testFreshTableReportsZeroRows() {
    TableStats stats = stats(ImmutableList.of(), false);
    assertThat(stats.getRowCount()).isEqualTo(0L);
  }

  @Test
  public void testUnfilteredRowCountFromSnapshotSummary() throws Exception {
    appendTwoFiles();
    TableStats stats = stats(ImmutableList.of(), false);
    assertThat(stats.getRowCount()).isEqualTo(5L);
  }

  @Test
  public void testFilteredRowCountFromPlannedFiles() throws Exception {
    appendTwoFiles();
    TableStats stats = stats(ImmutableList.of(Expressions.greaterThanOrEqual("id", 4)), false);
    // file with ids 1-3 is pruned by its column bounds
    assertThat(stats.getRowCount()).isEqualTo(2L);
  }

  @Test
  public void testColumnStatsFromManifests() throws Exception {
    appendTwoFiles();
    TableStats stats = stats(ImmutableList.of(), true);

    assertThat(stats.getRowCount()).isEqualTo(5L);
    ColumnStats idStats = stats.getColumnStats().get("id");
    assertThat(idStats.getNullCount()).isEqualTo(0L);
    assertThat(idStats.getMin()).isEqualTo(1);
    assertThat(idStats.getMax()).isEqualTo(5);

    ColumnStats scoreStats = stats.getColumnStats().get("score");
    assertThat(scoreStats.getNullCount()).isEqualTo(1L);
    assertThat(scoreStats.getMin()).isEqualTo(1.0);
    assertThat(scoreStats.getMax()).isEqualTo(5.0);

    // string column: nullCount yes, min/max never
    ColumnStats dataStats = stats.getColumnStats().get("data");
    assertThat(dataStats.getNullCount()).isEqualTo(1L);
    assertThat(dataStats.getMin()).isNull();
    assertThat(dataStats.getMax()).isNull();
  }

  @Test
  public void testColumnStatsDisabled() throws Exception {
    appendTwoFiles();
    TableStats stats = stats(ImmutableList.of(), false);
    assertThat(stats.getColumnStats()).isEmpty();
  }

  @Test
  public void testMetricsModeNoneOmitsColumnStats() throws Exception {
    table.updateProperties().set(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none").commit();
    appendTwoFiles();
    TableStats stats = stats(ImmutableList.of(), true);
    // row count survives (recordCount is always present), column stats are absent
    assertThat(stats.getRowCount()).isEqualTo(5L);
    assertThat(stats.getColumnStats()).isEmpty();
  }

  @Test
  public void testDeleteFilesTolerated() throws Exception {
    appendTwoFiles();
    DataFile dataFile = appender.writeFile(ImmutableList.of(record(6, "f", 6.0)));
    appender.appendToTable(dataFile);
    DeleteFile posDeletes =
        FileHelpers.writeDeleteFile(
                table,
                Files.localOutput(File.createTempFile("junit", null, appendDir.toFile())),
                ImmutableList.of(Pair.of((CharSequence) dataFile.location(), 0L)))
            .first();
    table.newRowDelta().addDeletes(posDeletes).commit();

    TableStats stats = stats(ImmutableList.of(), true);
    // overestimate: deleted row still counted (6, not 5) — documented estimate semantics
    assertThat(stats.getRowCount()).isEqualTo(6L);
  }

  private IcebergTableSource createTableSource(
      Map<String, String> properties, Configuration flinkConf) {
    ResolvedSchema flinkSchema =
        ResolvedSchema.of(
            Column.physical("id", DataTypes.INT().notNull()),
            Column.physical("data", DataTypes.STRING()),
            Column.physical("score", DataTypes.DOUBLE()));
    return new IcebergTableSource(
        TableLoader.fromHadoopTable(table.location()), flinkSchema, properties, flinkConf);
  }

  @Test
  public void testSourceReportsStatistics() throws Exception {
    appendTwoFiles();
    TableStats stats = createTableSource(Maps.newHashMap(), new Configuration()).reportStatistics();
    assertThat(stats.getRowCount()).isEqualTo(5L);
    assertThat(stats.getColumnStats().get("id").getMax()).isEqualTo(5);
  }

  @Test
  public void testStreamingSourceReportsUnknown() throws Exception {
    appendTwoFiles();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("streaming", "true");
    TableStats stats = createTableSource(properties, new Configuration()).reportStatistics();
    assertThat(stats).isEqualTo(TableStats.UNKNOWN);
  }

  /** First snapshot has 3 rows; a second append brings the table to 5. */
  private Snapshot firstSnapshot() throws Exception {
    appender.appendToTable(
        ImmutableList.of(record(1, "a", 1.0), record(2, null, 2.5), record(3, "c", null)));
    Snapshot snapshot = table.currentSnapshot();
    Thread.sleep(2); // keep as-of-timestamp strictly between the two snapshots
    appender.appendToTable(ImmutableList.of(record(4, "d", 4.0), record(5, "e", 5.0)));
    return snapshot;
  }

  @Test
  public void testSnapshotIdReportsStatsForThatSnapshot() throws Exception {
    Snapshot first = firstSnapshot();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("snapshot-id", String.valueOf(first.snapshotId()));
    TableStats stats = createTableSource(properties, new Configuration()).reportStatistics();
    assertThat(stats.getRowCount()).isEqualTo(3L);
    assertThat(stats.getColumnStats().get("id").getMax()).isEqualTo(3);
  }

  @Test
  public void testTagReportsStatsForTaggedSnapshot() throws Exception {
    Snapshot first = firstSnapshot();
    table.manageSnapshots().createTag("v1", first.snapshotId()).commit();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("tag", "v1");
    TableStats stats = createTableSource(properties, new Configuration()).reportStatistics();
    assertThat(stats.getRowCount()).isEqualTo(3L);
  }

  @Test
  public void testBranchReportsStatsForBranchHead() throws Exception {
    Snapshot first = firstSnapshot();
    table.manageSnapshots().createBranch("b1", first.snapshotId()).commit();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("branch", "b1");
    TableStats stats = createTableSource(properties, new Configuration()).reportStatistics();
    assertThat(stats.getRowCount()).isEqualTo(3L);
  }

  @Test
  public void testAsOfTimestampReportsStatsForThatTime() throws Exception {
    Snapshot first = firstSnapshot();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("as-of-timestamp", String.valueOf(first.timestampMillis()));
    TableStats stats = createTableSource(properties, new Configuration()).reportStatistics();
    assertThat(stats.getRowCount()).isEqualTo(3L);
  }

  @Test
  public void testIncrementalReadReportsUnknown() throws Exception {
    Snapshot first = firstSnapshot();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("start-snapshot-id", String.valueOf(first.snapshotId()));
    TableStats stats = createTableSource(properties, new Configuration()).reportStatistics();
    assertThat(stats).isEqualTo(TableStats.UNKNOWN);
  }

  @Test
  public void testUnknownRefReportsUnknown() throws Exception {
    appendTwoFiles();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("tag", "does-not-exist");
    TableStats stats = createTableSource(properties, new Configuration()).reportStatistics();
    assertThat(stats).isEqualTo(TableStats.UNKNOWN);
  }

  @Test
  public void testColumnStatsFlagDisablesColumnStats() throws Exception {
    appendTwoFiles();
    Configuration flinkConf = new Configuration();
    flinkConf.set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_REPORT_COLUMN_STATISTICS, false);
    TableStats stats = createTableSource(Maps.newHashMap(), flinkConf).reportStatistics();
    assertThat(stats.getRowCount()).isEqualTo(5L);
    assertThat(stats.getColumnStats()).isEmpty();
  }

  @Test
  public void testNdvFromPuffinStatistics() throws Exception {
    appendTwoFiles();
    long snapshotId = table.currentSnapshot().snapshotId();
    GenericStatisticsFile statisticsFile =
        new GenericStatisticsFile(
            snapshotId,
            table.location() + "/metadata/stats.puffin",
            100,
            42,
            ImmutableList.of(
                new GenericBlobMetadata(
                    StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                    snapshotId,
                    table.currentSnapshot().sequenceNumber(),
                    ImmutableList.of(1),
                    ImmutableMap.of("ndv", "5"))));
    table.updateStatistics().setStatistics(statisticsFile).commit();

    TableStats stats = stats(ImmutableList.of(), true);
    assertThat(stats.getColumnStats().get("id").getNdv()).isEqualTo(5L);
    // no blob for field 2 → no NDV, other stats still present
    assertThat(stats.getColumnStats().get("data").getNdv()).isNull();
    assertThat(stats.getColumnStats().get("data").getNullCount()).isEqualTo(1L);
  }

  @Test
  public void testRowCountOverflowReturnsUnknown() throws Exception {
    appendTwoFiles();
    // metadata-only files with huge record counts; their sum overflows a long
    for (int i = 0; i < 2; i++) {
      DataFile huge =
          DataFiles.builder(PartitionSpec.unpartitioned())
              .withPath(warehouse.resolve("tbl/data/fake-" + i + ".parquet").toString())
              .withFileSizeInBytes(10)
              .withFormat(FileFormat.PARQUET)
              .withRecordCount(Long.MAX_VALUE - 1)
              .build();
      table.newAppend().appendFile(huge).commit();
    }

    // filtered → planned-files path; the fake files have no column bounds, so they cannot
    // be pruned, and summing their record counts must hit the overflow sentinel guard
    TableStats stats = stats(ImmutableList.of(Expressions.greaterThanOrEqual("id", 0)), false);
    assertThat(stats).isEqualTo(TableStats.UNKNOWN);
  }
}
