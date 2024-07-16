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

import static org.apache.iceberg.flink.TestFixtures.DATABASE;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.HadoopTableExtension;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestColumnStatsWatermarkExtractor {
  public static final Schema SCHEMA =
      new Schema(
          required(1, "timestamp_column", Types.TimestampType.withoutZone()),
          required(2, "timestamptz_column", Types.TimestampType.withZone()),
          required(3, "long_column", Types.LongType.get()),
          required(4, "string_column", Types.StringType.get()));

  private static final GenericAppenderFactory APPENDER_FACTORY = new GenericAppenderFactory(SCHEMA);

  private static final List<List<Record>> TEST_RECORDS =
      ImmutableList.of(
          RandomGenericData.generate(SCHEMA, 3, 2L), RandomGenericData.generate(SCHEMA, 3, 19L));

  private static final List<Map<String, Long>> MIN_VALUES =
      ImmutableList.of(Maps.newHashMapWithExpectedSize(3), Maps.newHashMapWithExpectedSize(3));

  @TempDir protected Path temporaryFolder;

  @RegisterExtension
  private static final HadoopTableExtension SOURCE_TABLE_RESOURCE =
      new HadoopTableExtension(DATABASE, TestFixtures.TABLE, SCHEMA);

  @Parameter(index = 0)
  private String columnName;

  @BeforeAll
  public static void updateMinValue() {
    for (int i = 0; i < TEST_RECORDS.size(); ++i) {
      for (Record r : TEST_RECORDS.get(i)) {
        Map<String, Long> minValues = MIN_VALUES.get(i);

        LocalDateTime localDateTime = (LocalDateTime) r.get(0);
        minValues.merge(
            "timestamp_column", localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli(), Math::min);

        OffsetDateTime offsetDateTime = (OffsetDateTime) r.get(1);
        minValues.merge("timestamptz_column", offsetDateTime.toInstant().toEpochMilli(), Math::min);

        minValues.merge("long_column", (Long) r.get(2), Math::min);
      }
    }
  }

  @Parameters(name = "columnName = {0}")
  public static Collection<Object[]> data() {
    return ImmutableList.of(
        new Object[] {"timestamp_column"},
        new Object[] {"timestamptz_column"},
        new Object[] {"long_column"});
  }

  @TestTemplate
  public void testSingle() throws IOException {
    ColumnStatsWatermarkExtractor extractor =
        new ColumnStatsWatermarkExtractor(SCHEMA, columnName, TimeUnit.MILLISECONDS);

    assertThat(extractor.extractWatermark(split(0)))
        .isEqualTo(MIN_VALUES.get(0).get(columnName).longValue());
  }

  @TestTemplate
  public void testTimeUnit() throws IOException {
    assumeThat(columnName).isEqualTo("long_column");
    ColumnStatsWatermarkExtractor extractor =
        new ColumnStatsWatermarkExtractor(SCHEMA, columnName, TimeUnit.MICROSECONDS);

    assertThat(extractor.extractWatermark(split(0)))
        .isEqualTo(MIN_VALUES.get(0).get(columnName) / 1000L);
  }

  @TestTemplate
  public void testMultipleFiles() throws IOException {
    assumeThat(columnName).isEqualTo("timestamp_column");
    IcebergSourceSplit combinedSplit =
        IcebergSourceSplit.fromCombinedScanTask(
            ReaderUtil.createCombinedScanTask(
                TEST_RECORDS, temporaryFolder, FileFormat.PARQUET, APPENDER_FACTORY));

    ColumnStatsWatermarkExtractor extractor =
        new ColumnStatsWatermarkExtractor(SCHEMA, columnName, null);

    assertThat(extractor.extractWatermark(split(0)))
        .isEqualTo(MIN_VALUES.get(0).get(columnName).longValue());
    assertThat(extractor.extractWatermark(split(1)))
        .isEqualTo(MIN_VALUES.get(1).get(columnName).longValue());
    assertThat(extractor.extractWatermark(combinedSplit))
        .isEqualTo(Math.min(MIN_VALUES.get(0).get(columnName), MIN_VALUES.get(1).get(columnName)));
  }

  @TestTemplate
  public void testWrongColumn() {
    assumeThat(columnName).isEqualTo("string_column");
    assertThatThrownBy(() -> new ColumnStatsWatermarkExtractor(SCHEMA, columnName, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Found STRING, expected a LONG or TIMESTAMP column for watermark generation.");
  }

  @TestTemplate
  public void testEmptyStatistics() throws IOException {
    assumeThat(columnName).isEqualTo("timestamp_column");

    // Create an extractor for a column we do not have statistics
    ColumnStatsWatermarkExtractor extractor =
        new ColumnStatsWatermarkExtractor(10, "missing_field");
    assertThatThrownBy(() -> extractor.extractWatermark(split(0)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Missing statistics for column");
  }

  private IcebergSourceSplit split(int id) throws IOException {
    return IcebergSourceSplit.fromCombinedScanTask(
        ReaderUtil.createCombinedScanTask(
            ImmutableList.of(TEST_RECORDS.get(id)),
            temporaryFolder,
            FileFormat.PARQUET,
            APPENDER_FACTORY));
  }
}
