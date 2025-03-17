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
package org.apache.iceberg.flink.source.assigner;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.reader.ColumnStatsWatermarkExtractor;
import org.apache.iceberg.flink.source.reader.ReaderUtil;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.SerializableComparator;
import org.apache.iceberg.flink.source.split.SplitComparators;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.jupiter.api.Test;

public class TestWatermarkBasedSplitAssigner extends SplitAssignerTestBase {
  public static final Schema SCHEMA =
      new Schema(required(1, "timestamp_column", Types.TimestampType.withoutZone()));
  private static final GenericAppenderFactory APPENDER_FACTORY = new GenericAppenderFactory(SCHEMA);

  @Override
  protected SplitAssigner splitAssigner() {
    return new OrderedSplitAssignerFactory(
            SplitComparators.watermark(
                new ColumnStatsWatermarkExtractor(SCHEMA, "timestamp_column", null)))
        .createAssigner();
  }

  /** Test the assigner when multiple files are in a single split */
  @Test
  public void testMultipleFilesInAnIcebergSplit() {
    SplitAssigner assigner = splitAssigner();
    assigner.onDiscoveredSplits(createSplits(4, 2, "2"));

    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE);
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);
  }

  /** Test sorted splits */
  @Test
  public void testSplitSort() {
    SplitAssigner assigner = splitAssigner();

    Instant now = Instant.now();
    List<IcebergSourceSplit> splits =
        IntStream.range(0, 5)
            .mapToObj(i -> splitFromInstant(now.plus(i, ChronoUnit.MINUTES)))
            .collect(Collectors.toList());

    assigner.onDiscoveredSplits(splits.subList(3, 5));
    assigner.onDiscoveredSplits(splits.subList(0, 1));
    assigner.onDiscoveredSplits(splits.subList(1, 3));

    assertGetNext(assigner, splits.get(0));
    assertGetNext(assigner, splits.get(1));
    assertGetNext(assigner, splits.get(2));
    assertGetNext(assigner, splits.get(3));
    assertGetNext(assigner, splits.get(4));

    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE);
  }

  @Test
  public void testSerializable() {
    byte[] bytes =
        SerializationUtil.serializeToBytes(
            SplitComparators.watermark(
                new ColumnStatsWatermarkExtractor(
                    TestFixtures.SCHEMA, "id", TimeUnit.MILLISECONDS)));
    SerializableComparator<IcebergSourceSplit> comparator =
        SerializationUtil.deserializeFromBytes(bytes);
    assertThat(comparator).isNotNull();
  }

  private void assertGetNext(SplitAssigner assigner, IcebergSourceSplit split) {
    GetSplitResult result = assigner.getNext(null);
    assertThat(split).isEqualTo(result.split());
  }

  @Override
  protected List<IcebergSourceSplit> createSplits(
      int fileCount, int filesPerSplit, String version) {
    return IntStream.range(0, fileCount / filesPerSplit)
        .mapToObj(
            splitNum ->
                splitFromRecords(
                    IntStream.range(0, filesPerSplit)
                        .mapToObj(
                            fileNum ->
                                RandomGenericData.generate(
                                    SCHEMA, 2, (long) splitNum * filesPerSplit + fileNum))
                        .collect(Collectors.toList())))
        .collect(Collectors.toList());
  }

  private IcebergSourceSplit splitFromInstant(Instant instant) {
    Record record = GenericRecord.create(SCHEMA);
    record.set(0, LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
    return splitFromRecords(ImmutableList.of(ImmutableList.of(record)));
  }

  private IcebergSourceSplit splitFromRecords(List<List<Record>> records) {
    try {
      return IcebergSourceSplit.fromCombinedScanTask(
          ReaderUtil.createCombinedScanTask(
              records, temporaryFolder, FileFormat.PARQUET, APPENDER_FACTORY));
    } catch (IOException e) {
      throw new RuntimeException("Split creation exception", e);
    }
  }
}
