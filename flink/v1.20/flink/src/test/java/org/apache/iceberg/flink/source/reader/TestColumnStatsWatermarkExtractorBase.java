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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.nio.file.Path;
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
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class TestColumnStatsWatermarkExtractorBase {
  public abstract Schema getSchema();

  public abstract List<List<Record>> getTestRecords();

  public abstract List<Map<String, Long>> getMinValues();

  private final GenericAppenderFactory genericAppenderFactory =
      new GenericAppenderFactory(getSchema());

  @TempDir protected Path temporaryFolder;

  @Parameter(index = 0)
  private String columnName;

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
        new ColumnStatsWatermarkExtractor(getSchema(), columnName, TimeUnit.MILLISECONDS);

    assertThat(extractor.extractWatermark(split(0)))
        .isEqualTo(getMinValues().get(0).get(columnName).longValue());
  }

  @TestTemplate
  public void testTimeUnit() throws IOException {
    assumeThat(columnName).isEqualTo("long_column");
    ColumnStatsWatermarkExtractor extractor =
        new ColumnStatsWatermarkExtractor(getSchema(), columnName, TimeUnit.MICROSECONDS);

    assertThat(extractor.extractWatermark(split(0)))
        .isEqualTo(getMinValues().get(0).get(columnName) / 1000L);
  }

  @TestTemplate
  public void testMultipleFiles() throws IOException {
    assumeThat(columnName).isEqualTo("timestamp_column");
    IcebergSourceSplit combinedSplit =
        IcebergSourceSplit.fromCombinedScanTask(
            ReaderUtil.createCombinedScanTask(
                getTestRecords(), temporaryFolder, FileFormat.PARQUET, genericAppenderFactory));

    ColumnStatsWatermarkExtractor extractor =
        new ColumnStatsWatermarkExtractor(getSchema(), columnName, null);

    assertThat(extractor.extractWatermark(split(0)))
        .isEqualTo(getMinValues().get(0).get(columnName).longValue());
    assertThat(extractor.extractWatermark(split(1)))
        .isEqualTo(getMinValues().get(1).get(columnName).longValue());
    assertThat(extractor.extractWatermark(combinedSplit))
        .isEqualTo(
            Math.min(getMinValues().get(0).get(columnName), getMinValues().get(1).get(columnName)));
  }

  @TestTemplate
  public void testWrongColumn() {
    assumeThat(columnName).isEqualTo("string_column");
    assertThatThrownBy(() -> new ColumnStatsWatermarkExtractor(getSchema(), columnName, null))
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
            ImmutableList.of(getTestRecords().get(id)),
            temporaryFolder,
            FileFormat.PARQUET,
            genericAppenderFactory));
  }
}
