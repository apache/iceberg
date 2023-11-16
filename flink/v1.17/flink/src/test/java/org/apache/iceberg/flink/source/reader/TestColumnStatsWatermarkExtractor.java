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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.HadoopTableResource;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestColumnStatsWatermarkExtractor {
  public static final Schema SCHEMA =
      new Schema(
          required(1, "ts", Types.TimestampType.withoutZone()),
          required(2, "tstz", Types.TimestampType.withZone()),
          required(3, "l", Types.LongType.get()),
          required(4, "s", Types.StringType.get()));

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public final HadoopTableResource sourceTableResource =
      new HadoopTableResource(TEMPORARY_FOLDER, TestFixtures.DATABASE, TestFixtures.TABLE, SCHEMA);

  private GenericAppenderHelper dataAppender;
  private long minTs = Long.MAX_VALUE;
  private long minTsTz = Long.MAX_VALUE;
  private long minL = Long.MAX_VALUE;

  @Before
  public void initTable() throws IOException {
    dataAppender =
        new GenericAppenderHelper(
            sourceTableResource.table(), FileFormat.PARQUET, TEMPORARY_FOLDER);

    List<Record> batch = RandomGenericData.generate(SCHEMA, 3, 2L);
    dataAppender.appendToTable(batch);

    for (Record r : batch) {
      LocalDateTime localDateTime = (LocalDateTime) r.get(0);
      minTs = Math.min(minTs, localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli());

      OffsetDateTime offsetDateTime = (OffsetDateTime) r.get(1);
      minTsTz = Math.min(minTsTz, offsetDateTime.toInstant().toEpochMilli());

      minL = Math.min(minL, (Long) r.get(2));
    }
  }

  @Test
  public void testTimestamp() {
    DataFile dataFile =
        sourceTableResource
            .table()
            .currentSnapshot()
            .addedDataFiles(sourceTableResource.table().io())
            .iterator()
            .next();
    ColumnStatsWatermarkExtractor tsExtractor =
        new ColumnStatsWatermarkExtractor(SCHEMA, "ts", null);

    Assert.assertEquals(
        minTs,
        tsExtractor.extractWatermark(
            IcebergSourceSplit.fromCombinedScanTask(new DummyTask(dataFile))));
  }

  @Test
  public void testTimestampWithTz() {
    DataFile dataFile =
        sourceTableResource
            .table()
            .currentSnapshot()
            .addedDataFiles(sourceTableResource.table().io())
            .iterator()
            .next();
    ColumnStatsWatermarkExtractor tsTzExtractor =
        new ColumnStatsWatermarkExtractor(SCHEMA, "tstz", null);

    Assert.assertEquals(
        minTsTz,
        tsTzExtractor.extractWatermark(
            IcebergSourceSplit.fromCombinedScanTask(new DummyTask(dataFile))));
  }

  @Test
  public void testLong() {
    DataFile dataFile =
        sourceTableResource
            .table()
            .currentSnapshot()
            .addedDataFiles(sourceTableResource.table().io())
            .iterator()
            .next();
    ColumnStatsWatermarkExtractor longExtractorMilliSeconds =
        new ColumnStatsWatermarkExtractor(SCHEMA, "l", TimeUnit.MILLISECONDS);
    ColumnStatsWatermarkExtractor longExtractorMicroSeconds =
        new ColumnStatsWatermarkExtractor(SCHEMA, "l", TimeUnit.MICROSECONDS);

    Assert.assertEquals(
        minL,
        longExtractorMilliSeconds.extractWatermark(
            IcebergSourceSplit.fromCombinedScanTask(new DummyTask(dataFile))));
    Assert.assertEquals(
        minL / 1000L,
        longExtractorMicroSeconds.extractWatermark(
            IcebergSourceSplit.fromCombinedScanTask(new DummyTask(dataFile))));
  }

  @Test
  public void testMultipleFiles() throws IOException {
    DataFile oldDataFile =
        sourceTableResource
            .table()
            .currentSnapshot()
            .addedDataFiles(sourceTableResource.table().io())
            .iterator()
            .next();
    List<Record> batch = RandomGenericData.generate(SCHEMA, 3, 19L);
    dataAppender.appendToTable(batch);

    long minTsNew = Long.MAX_VALUE;
    for (Record r : batch) {
      LocalDateTime localDateTime = (LocalDateTime) r.get(0);
      minTsNew = Math.min(minTsNew, localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli());
    }

    DataFile newDataFile =
        sourceTableResource
            .table()
            .currentSnapshot()
            .addedDataFiles(sourceTableResource.table().io())
            .iterator()
            .next();
    ColumnStatsWatermarkExtractor tsExtractor =
        new ColumnStatsWatermarkExtractor(SCHEMA, "ts", null);

    Assert.assertEquals(
        minTsNew,
        tsExtractor.extractWatermark(
            IcebergSourceSplit.fromCombinedScanTask(new DummyTask(newDataFile))));
    Assert.assertEquals(
        minTs,
        tsExtractor.extractWatermark(
            IcebergSourceSplit.fromCombinedScanTask(new DummyTask(oldDataFile))));
    Assert.assertEquals(
        Math.min(minTsNew, minTs),
        tsExtractor.extractWatermark(
            IcebergSourceSplit.fromCombinedScanTask(new DummyTask(newDataFile, oldDataFile))));
  }

  @Test
  public void testWrongColumn() {
    Assertions.assertThatThrownBy(() -> new ColumnStatsWatermarkExtractor(SCHEMA, "s", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Found STRING, expected a LONG or TIMESTAMP column for watermark generation.");
  }

  private static class DummyTask extends BaseCombinedScanTask {
    private Collection<FileScanTask> files;

    DummyTask(DataFile... dataFiles) {
      files =
          Arrays.stream(dataFiles).map(f -> new MockFileScanTask(f)).collect(Collectors.toList());
    }

    @Override
    public Collection<FileScanTask> files() {
      return files;
    }
  }
}
