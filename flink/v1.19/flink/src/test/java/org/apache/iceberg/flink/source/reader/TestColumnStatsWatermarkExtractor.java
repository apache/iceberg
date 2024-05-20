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

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.HadoopTableResource;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
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

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public final HadoopTableResource sourceTableResource =
      new HadoopTableResource(TEMPORARY_FOLDER, TestFixtures.DATABASE, TestFixtures.TABLE, SCHEMA);

  private final String columnName;

  @BeforeClass
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

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return ImmutableList.of(
        new Object[] {"timestamp_column"},
        new Object[] {"timestamptz_column"},
        new Object[] {"long_column"});
  }

  public TestColumnStatsWatermarkExtractor(String columnName) {
    this.columnName = columnName;
  }

  @Test
  public void testSingle() throws IOException {
    ColumnStatsWatermarkExtractor extractor =
        new ColumnStatsWatermarkExtractor(SCHEMA, columnName, TimeUnit.MILLISECONDS);

    Assert.assertEquals(
        MIN_VALUES.get(0).get(columnName).longValue(), extractor.extractWatermark(split(0)));
  }

  @Test
  public void testTimeUnit() throws IOException {
    Assume.assumeTrue("Run only for long column", columnName.equals("long_column"));
    ColumnStatsWatermarkExtractor extractor =
        new ColumnStatsWatermarkExtractor(SCHEMA, columnName, TimeUnit.MICROSECONDS);

    Assert.assertEquals(
        MIN_VALUES.get(0).get(columnName).longValue() / 1000L,
        extractor.extractWatermark(split(0)));
  }

  @Test
  public void testMultipleFiles() throws IOException {
    Assume.assumeTrue("Run only for the timestamp column", columnName.equals("timestamp_column"));
    IcebergSourceSplit combinedSplit =
        IcebergSourceSplit.fromCombinedScanTask(
            ReaderUtil.createCombinedScanTask(
                TEST_RECORDS, TEMPORARY_FOLDER, FileFormat.PARQUET, APPENDER_FACTORY));

    ColumnStatsWatermarkExtractor extractor =
        new ColumnStatsWatermarkExtractor(SCHEMA, columnName, null);

    Assert.assertEquals(
        MIN_VALUES.get(0).get(columnName).longValue(), extractor.extractWatermark(split(0)));
    Assert.assertEquals(
        MIN_VALUES.get(1).get(columnName).longValue(), extractor.extractWatermark(split(1)));
    Assert.assertEquals(
        Math.min(MIN_VALUES.get(0).get(columnName), MIN_VALUES.get(1).get(columnName)),
        extractor.extractWatermark(combinedSplit));
  }

  @Test
  public void testWrongColumn() {
    Assume.assumeTrue("Run only for string column", columnName.equals("string_column"));
    Assertions.assertThatThrownBy(() -> new ColumnStatsWatermarkExtractor(SCHEMA, columnName, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Found STRING, expected a LONG or TIMESTAMP column for watermark generation.");
  }

  @Test
  public void testEmptyStatistics() throws IOException {
    Assume.assumeTrue("Run only for timestamp column", columnName.equals("timestamp_column"));

    // Create an extractor for a column we do not have statistics
    ColumnStatsWatermarkExtractor extractor =
        new ColumnStatsWatermarkExtractor(10, "missing_field");
    Assertions.assertThatThrownBy(() -> extractor.extractWatermark(split(0)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Missing statistics for column");
  }

  private IcebergSourceSplit split(int id) throws IOException {
    return IcebergSourceSplit.fromCombinedScanTask(
        ReaderUtil.createCombinedScanTask(
            ImmutableList.of(TEST_RECORDS.get(id)),
            TEMPORARY_FOLDER,
            FileFormat.PARQUET,
            APPENDER_FACTORY));
  }
}
