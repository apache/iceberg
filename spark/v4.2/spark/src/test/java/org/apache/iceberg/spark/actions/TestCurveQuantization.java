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
package org.apache.iceberg.spark.actions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.actions.CurveQuantization.ColumnRange;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TestCurveQuantization extends TestBase {

  private static final TableIdentifier TABLE_IDENT = TableIdentifier.of("default", "curve_tbl");
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "int_col", Types.IntegerType.get()),
          Types.NestedField.optional(2, "double_col", Types.DoubleType.get()),
          Types.NestedField.optional(3, "date_col", Types.DateType.get()),
          Types.NestedField.optional(4, "ts_col", Types.TimestampType.withoutZone()),
          Types.NestedField.optional(5, "string_col", Types.StringType.get()));
  private static final int INT_ID = 1;
  private static final int DOUBLE_ID = 2;
  private static final int DATE_ID = 3;
  private static final int TS_ID = 4;

  @AfterEach
  void removeTable() {
    catalog.dropTable(TABLE_IDENT);
  }

  @Test
  void rangeUnionsBoundsAcrossFiles() {
    Table table = createTable(ImmutableMap.of());
    List<FileScanTask> tasks = ImmutableList.of(intFile(1, 5), intFile(3, 9));

    ColumnRange range = onlyRange(tasks, table, "int_col");

    assertThat(range.isNone()).isFalse();
    assertThat(range.min()).isEqualTo(1.0);
    assertThat(range.max()).isEqualTo(9.0);
  }

  @Test
  void rangeSkipsFilesWhereColumnIsAllNull() {
    Table table = createTable(ImmutableMap.of());
    FileScanTask allNull = withoutBounds(INT_ID, 3L, 3L, null);
    List<FileScanTask> tasks = ImmutableList.of(intFile(1, 5), allNull);

    ColumnRange range = onlyRange(tasks, table, "int_col");

    assertThat(range.min()).isEqualTo(1.0);
    assertThat(range.max()).isEqualTo(5.0);
  }

  @Test
  void rangeSkipsFilesWhereColumnIsOnlyNullOrNaN() {
    Table table = createTable(ImmutableMap.of());
    FileScanTask nullsAndNaNs = withoutBounds(DOUBLE_ID, 4L, 1L, 3L);
    List<FileScanTask> tasks = ImmutableList.of(doubleFile(2.0, 4.0), nullsAndNaNs);

    ColumnRange range = onlyRange(tasks, table, "double_col");

    assertThat(range.min()).isEqualTo(2.0);
    assertThat(range.max()).isEqualTo(4.0);
  }

  @Test
  void rangeSkipsFilesWithoutCountsForColumn() {
    Table table = createTable(ImmutableMap.of());
    FileScanTask noMetrics = withoutMetrics();
    List<FileScanTask> tasks = ImmutableList.of(intFile(1, 5), noMetrics);

    ColumnRange range = onlyRange(tasks, table, "int_col");

    assertThat(range.min()).isEqualTo(1.0);
    assertThat(range.max()).isEqualTo(5.0);
  }

  @Test
  void rangeIsNoneWhenNoFileHasBounds() {
    Table table = createTable(ImmutableMap.of());
    FileScanTask allNull = withoutBounds(INT_ID, 3L, 3L, null);

    ColumnRange range = onlyRange(ImmutableList.of(allNull), table, "int_col");

    assertThat(range.isNone()).isTrue();
  }

  @Test
  void rangeFailsWhenFileHasValuesButNoBounds() {
    Table table = createTable(ImmutableMap.of());
    FileScanTask countsOnly = withoutBounds(INT_ID, 3L, 0L, null);
    List<FileScanTask> tasks = ImmutableList.of(intFile(1, 5), countsOnly);

    assertThatThrownBy(() -> CurveQuantization.ranges(tasks, table, ImmutableList.of("int_col")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("int_col")
        .hasMessageContaining(countsOnly.file().location())
        .hasMessageContaining("write.metadata.metrics");
  }

  @Test
  void rangeFailsOnInfiniteBounds() {
    Table table = createTable(ImmutableMap.of());
    List<FileScanTask> tasks = ImmutableList.of(doubleFile(1.0, Double.POSITIVE_INFINITY));

    assertThatThrownBy(() -> CurveQuantization.ranges(tasks, table, ImmutableList.of("double_col")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("double_col");
  }

  @Test
  void rangeFailsWhenNoFileHasCountsAndMetricsAreOff() {
    Table table = createTable(ImmutableMap.of(TableProperties.DEFAULT_WRITE_METRICS_MODE, "none"));
    List<FileScanTask> tasks = filesWithoutMetrics();

    assertThatThrownBy(() -> CurveQuantization.ranges(tasks, table, ImmutableList.of("int_col")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("int_col")
        .hasMessageContaining("write.metadata.metrics");
  }

  @Test
  void rangeIsNoneWhenFilesPredateColumn() {
    Table table = createTable(ImmutableMap.of());

    ColumnRange range = onlyRange(filesWithoutMetrics(), table, "int_col");

    assertThat(range.isNone()).isTrue();
  }

  @Test
  void rangeReadsDatesAsDaysAndTimestampsAsMicros() {
    Table table = createTable(ImmutableMap.of());
    int minDay = DateTimeUtil.isoDateToDays("2024-01-02");
    int maxDay = DateTimeUtil.isoDateToDays("2024-01-12");
    long minMicros = DateTimeUtil.isoTimestampToMicros("2024-01-02T03:04:05.000006");
    long maxMicros = DateTimeUtil.isoTimestampToMicros("2024-03-04T05:06:07.000008");
    FileScanTask temporal =
        task(
            new Metrics(
                3L,
                null,
                ImmutableMap.of(DATE_ID, 3L, TS_ID, 3L),
                ImmutableMap.of(DATE_ID, 0L, TS_ID, 0L),
                null,
                ImmutableMap.of(
                    DATE_ID, bound(Types.DateType.get(), minDay),
                    TS_ID, bound(Types.TimestampType.withoutZone(), minMicros)),
                ImmutableMap.of(
                    DATE_ID, bound(Types.DateType.get(), maxDay),
                    TS_ID, bound(Types.TimestampType.withoutZone(), maxMicros))));

    List<ColumnRange> ranges =
        CurveQuantization.ranges(
            ImmutableList.of(temporal), table, ImmutableList.of("date_col", "ts_col"));

    assertThat(ranges).hasSize(2);
    assertThat(ranges.get(0).min()).isEqualTo((double) minDay);
    assertThat(ranges.get(0).max()).isEqualTo((double) maxDay);
    assertThat(ranges.get(1).min()).isEqualTo((double) minMicros);
    assertThat(ranges.get(1).max()).isEqualTo((double) maxMicros);
  }

  @Test
  void quantizationBitsRejectsInvalidValues() {
    for (String bits : ImmutableList.of("7", "12", "40", "-8")) {
      Map<String, String> options = ImmutableMap.of(CurveQuantization.QUANTIZATION_BITS, bits);
      assertThatThrownBy(() -> CurveQuantization.quantizationBits(options))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Cannot use quantization-bits %s, it must be 0 (off), 8, 16, 24 or 32", bits);
    }
  }

  @Test
  void validateColumnsRejectsStringColumn() {
    Table table = createTable(ImmutableMap.of());

    assertThatThrownBy(
            () -> CurveQuantization.validateColumns(table, ImmutableList.of("string_col")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot quantize column string_col of type string, supported types are int, long, date, timestamp, float and double");
  }

  @Test
  void hilbertStatsColumnsUseSchemaNames() {
    Table table = createTable(ImmutableMap.of());
    SparkHilbertFileRewriteRunner runner =
        new SparkHilbertFileRewriteRunner(spark, table, ImmutableList.of("INT_COL"));

    List<String> statsColumns =
        runner.statsColumns(ImmutableMap.of(CurveQuantization.QUANTIZATION_BITS, "16"));

    assertThat(statsColumns).containsExactly("int_col");
  }

  private Table createTable(Map<String, String> properties) {
    return catalog.createTable(TABLE_IDENT, SCHEMA, PartitionSpec.unpartitioned(), properties);
  }

  private static ColumnRange onlyRange(List<FileScanTask> tasks, Table table, String column) {
    List<ColumnRange> ranges = CurveQuantization.ranges(tasks, table, ImmutableList.of(column));
    assertThat(ranges).hasSize(1);
    return ranges.get(0);
  }

  private static List<FileScanTask> filesWithoutMetrics() {
    return ImmutableList.of(withoutMetrics(), withoutMetrics());
  }

  private static FileScanTask intFile(int lower, int upper) {
    return bounded(INT_ID, Types.IntegerType.get(), lower, upper);
  }

  private static FileScanTask doubleFile(double lower, double upper) {
    return bounded(DOUBLE_ID, Types.DoubleType.get(), lower, upper);
  }

  private static FileScanTask bounded(int fieldId, Type type, Object lower, Object upper) {
    return task(
        new Metrics(
            3L,
            null,
            ImmutableMap.of(fieldId, 3L),
            ImmutableMap.of(fieldId, 0L),
            null,
            ImmutableMap.of(fieldId, bound(type, lower)),
            ImmutableMap.of(fieldId, bound(type, upper))));
  }

  private static FileScanTask withoutBounds(
      int fieldId, long valueCount, long nullCount, Long nanCount) {
    return task(
        new Metrics(
            3L,
            null,
            ImmutableMap.of(fieldId, valueCount),
            ImmutableMap.of(fieldId, nullCount),
            nanCount == null ? null : ImmutableMap.of(fieldId, nanCount)));
  }

  private static FileScanTask withoutMetrics() {
    return task(new Metrics(3L));
  }

  private static ByteBuffer bound(Type type, Object value) {
    return Conversions.toByteBuffer(type, value);
  }

  private static FileScanTask task(Metrics metrics) {
    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data-" + UUID.randomUUID() + ".parquet")
            .withFileSizeInBytes(10)
            .withMetrics(metrics)
            .build();
    return new MockFileScanTask(file);
  }
}
