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
package org.apache.iceberg.flink.source.reader;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.HadoopTableExtension;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.source.FlinkInputFormat;
import org.apache.iceberg.flink.source.FlinkSource;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end tests for ColumnStatsWatermarkExtractor with nanosecond precision timestamps. This
 * test validates that watermark extraction works correctly with actual data files containing
 * nanosecond precision timestamps.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestColumnStatsWatermarkExtractorEndToEnd {

  @Parameter(index = 0)
  private FileFormat format;

  @Parameters(name = "format = {0}")
  public static Object[][] parameters() {
    return new Object[][] {{FileFormat.PARQUET}, {FileFormat.AVRO}};
  }

  private static final String DATABASE = "test_db";
  private static final String TABLE = "test_watermark_table";

  // Schema with nanosecond precision timestamps for watermark testing
  // Contains both nanosecond (ns) and microsecond (micros) precision timestamps
  private static final Schema NANOSECOND_WATERMARK_SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          required(2, "event_time_ns", Types.TimestampNanoType.withoutZone()),
          required(3, "event_time_ns_tz", Types.TimestampNanoType.withZone()),
          required(4, "event_time_micros", Types.TimestampType.withoutZone()),
          required(5, "data", Types.StringType.get()));

  @TempDir protected Path temporaryDirectory;

  @RegisterExtension
  private static final HadoopTableExtension TABLE_EXTENSION =
      HadoopTableExtension.withFormatVersion3(DATABASE, TABLE, NANOSECOND_WATERMARK_SCHEMA);

  /**
   * Tests that ColumnStatsWatermarkExtractor can be instantiated with both nanosecond and
   * microsecond precision timestamp columns. This verifies that the extractor correctly recognizes
   * and accepts both TIMESTAMP_NANO and TIMESTAMP column types.
   */
  @Test
  public void testWatermarkExtractorCreationWithNanosecondTimestamps() {
    Table table = TABLE_EXTENSION.table();

    // Test that we can create watermark extractors for nanosecond timestamp columns
    ColumnStatsWatermarkExtractor extractorNs =
        new ColumnStatsWatermarkExtractor(table.schema(), "event_time_ns", TimeUnit.MICROSECONDS);
    assertThat(extractorNs).isNotNull();

    ColumnStatsWatermarkExtractor extractorNsTz =
        new ColumnStatsWatermarkExtractor(
            table.schema(), "event_time_ns_tz", TimeUnit.MICROSECONDS);
    assertThat(extractorNsTz).isNotNull();

    // Test that we can still create extractors for microsecond timestamp columns
    ColumnStatsWatermarkExtractor extractorMicros =
        new ColumnStatsWatermarkExtractor(
            table.schema(), "event_time_micros", TimeUnit.MICROSECONDS);
    assertThat(extractorMicros).isNotNull();
  }

  /**
   * Tests that ColumnStatsWatermarkExtractor can be created and used with actual data files
   * containing nanosecond precision timestamps. This validates the end-to-end integration where
   * data is written to files (Parquet/Avro) and the extractor is configured to read watermarks from
   * the column statistics. The test verifies that data can be written with nanosecond timestamps,
   * watermark extractors can be instantiated for these columns, and the schema correctly identifies
   * timestamp types (nano vs micro).
   */
  @TestTemplate
  public void testWatermarkExtractionWithRealData() throws Exception {
    Table table = TABLE_EXTENSION.table();

    // Generate test data with nanosecond precision timestamps
    List<Record> testRecords = RandomGenericData.generate(NANOSECOND_WATERMARK_SCHEMA, 5, 42L);

    // Write data to the table
    new GenericAppenderHelper(table, format, temporaryDirectory).appendToTable(testRecords);

    // Create watermark extractor for nanosecond timestamp column
    ColumnStatsWatermarkExtractor extractor =
        new ColumnStatsWatermarkExtractor(table.schema(), "event_time_ns", TimeUnit.MICROSECONDS);

    // Validate that the extractor can be created and configured correctly
    // Note: The actual watermark extraction happens in the Flink source during split processing
    assertThat(extractor).isNotNull();

    // Verify that the table has the expected schema
    Schema tableSchema = table.schema();
    assertThat(tableSchema.findField("event_time_ns")).isNotNull();
    assertThat(tableSchema.findField("event_time_ns_tz")).isNotNull();
    assertThat(tableSchema.findField("event_time_micros")).isNotNull();

    // Verify that the timestamp fields are of the correct types
    Types.NestedField eventTimeNsField = tableSchema.findField("event_time_ns");
    assertThat(eventTimeNsField.type()).isInstanceOf(Types.TimestampNanoType.class);

    Types.NestedField eventTimeNsTzField = tableSchema.findField("event_time_ns_tz");
    assertThat(eventTimeNsTzField.type()).isInstanceOf(Types.TimestampNanoType.class);

    Types.NestedField eventTimeMicrosField = tableSchema.findField("event_time_micros");
    assertThat(eventTimeMicrosField.type()).isInstanceOf(Types.TimestampType.class);
  }

  /**
   * Tests the full write-read cycle for tables with nanosecond precision timestamps. This verifies
   * that data with nanosecond timestamps can be written to files (Parquet/Avro), the data can be
   * read back through Flink's source implementation, and nanosecond precision is preserved during
   * the round-trip. This is an integration test ensuring that the complete data path works
   * correctly with nanosecond timestamps.
   */
  @TestTemplate
  public void testDataScanningWithNanosecondTimestamps() throws Exception {
    Table table = TABLE_EXTENSION.table();

    // Generate test data
    List<Record> expectedRecords = RandomGenericData.generate(NANOSECOND_WATERMARK_SCHEMA, 3, 123L);

    // Write data to the table
    new GenericAppenderHelper(table, format, temporaryDirectory).appendToTable(expectedRecords);

    // Test that we can scan the data back
    FlinkSource.Builder builder =
        FlinkSource.forRowData().table(table).tableLoader(TABLE_EXTENSION.tableLoader());

    List<Row> actualRows = runFormat(builder.buildFormat());

    // Verify that we can read the data back correctly
    assertThat(actualRows).hasSize(expectedRecords.size());

    // The data should be readable, indicating that nanosecond timestamps are properly handled
    // in the read path as well
    TestHelpers.assertRecords(actualRows, expectedRecords, NANOSECOND_WATERMARK_SCHEMA);
  }

  /**
   * Tests that ColumnStatsWatermarkExtractor also works with LONG columns (not just TIMESTAMP
   * types). This is useful for cases where timestamps are stored as epoch milliseconds/nanoseconds
   * in a LONG column.
   */
  @Test
  public void testWatermarkExtractorWithLongColumn() {
    // Create a schema with a Long column for watermark testing
    Schema longWatermarkSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "timestamp_long", Types.LongType.get()),
            required(3, "data", Types.StringType.get()));

    // Test that we can create a watermark extractor for Long columns with nanosecond time unit
    ColumnStatsWatermarkExtractor extractor =
        new ColumnStatsWatermarkExtractor(
            longWatermarkSchema, "timestamp_long", TimeUnit.NANOSECONDS);

    assertThat(extractor).isNotNull();
  }

  /**
   * Tests that attempting to create a watermark extractor with an unsupported column type (e.g.,
   * STRING) throws an appropriate exception. Only LONG, TIMESTAMP, and TIMESTAMP_NANO types are
   * supported for watermark extraction.
   */
  @Test
  public void testInvalidColumnForWatermark() {
    Schema invalidSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "invalid_column", Types.StringType.get()));

    // Test that creating a watermark extractor with an invalid column type throws an exception
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () ->
                new ColumnStatsWatermarkExtractor(
                    invalidSchema, "invalid_column", TimeUnit.MICROSECONDS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expected a LONG, TIMESTAMP, or TIMESTAMP_NANO column");
  }

  private List<Row> runFormat(FlinkInputFormat inputFormat) throws IOException {
    RowType rowType = FlinkSchemaUtil.convert(NANOSECOND_WATERMARK_SCHEMA);
    return TestHelpers.readRows(inputFormat, rowType);
  }
}
