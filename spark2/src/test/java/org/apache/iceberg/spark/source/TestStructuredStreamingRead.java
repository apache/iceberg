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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.MicroBatches.MicroBatch;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestStructuredStreamingRead {
  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );
  private static SparkSession spark = null;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @BeforeClass
  public static void startSpark() {
    TestStructuredStreamingRead.spark = SparkSession.builder()
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", 4)
        .getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestStructuredStreamingRead.spark;
    TestStructuredStreamingRead.spark = null;
    currentSpark.stop();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetChanges() throws IOException {
    File parent = temp.newFolder("test");
    File location = new File(parent, "table");
    File checkpoint = new File(parent, "checkpoint");
    Table table = createTable(location.toString());

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString(),
        "checkpointLocation", checkpoint.toString()));
    IcebergSource source = new IcebergSource();

    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);
    long initialSnapshotId = snapshotIds.get(0);

    // Getting all appends from initial snapshot.
    List<MicroBatch> pendingBatches = streamingReader.getChangesWithRateLimit(
        initialSnapshotId, 0, true, false, Long.MAX_VALUE);
    Assert.assertEquals(pendingBatches.size(), 4);

    List<Long> batchSnapshotIds = pendingBatches.stream()
        .map(MicroBatch::snapshotId)
        .collect(Collectors.toList());
    Assert.assertEquals(batchSnapshotIds, snapshotIds);

    // Getting appends from initial snapshot with index, 1st snapshot will be filtered out.
    List<MicroBatch> pendingBatches1 = streamingReader.getChangesWithRateLimit(
        initialSnapshotId, 1, true, false, Long.MAX_VALUE);

    Assert.assertEquals(pendingBatches1.size(), 4);
    MicroBatch batch = pendingBatches1.get(0);
    Assert.assertEquals(batch.sizeInBytes(), 0L);
    Assert.assertEquals(batch.endFileIndex(), 1);
    Assert.assertTrue(Iterables.isEmpty(batch.tasks()));

    // Getting appends from 2nd snapshot, 1st snapshot should be filtered out.
    long snapshotId2 = snapshotIds.get(1);
    List<MicroBatch> pendingBatches2 = streamingReader.getChangesWithRateLimit(
        snapshotId2, 0, false, false, Long.MAX_VALUE);

    Assert.assertEquals(pendingBatches2.size(), 3);
    List<Long> batchSnapshotIds1 = pendingBatches2.stream()
        .map(MicroBatch::snapshotId)
        .collect(Collectors.toList());
    Assert.assertEquals(batchSnapshotIds1.indexOf(initialSnapshotId), -1);

    // Getting appends from last snapshot with index, should have no task included.
    long lastSnapshotId = snapshotIds.get(3);
    List<MicroBatch> pendingBatches3 = streamingReader.getChangesWithRateLimit(
        lastSnapshotId, 1, false, false, Long.MAX_VALUE);

    Assert.assertEquals(pendingBatches3.size(), 1);
    MicroBatch batch1 = pendingBatches3.get(0);
    Assert.assertEquals(batch1.sizeInBytes(), 0L);
    Assert.assertEquals(batch1.endFileIndex(), 1);
    Assert.assertTrue(Iterables.isEmpty(batch.tasks()));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetChangesWithRateLimit() throws IOException {
    File parent = temp.newFolder("test");
    File location = new File(parent, "table");
    File checkpoint = new File(parent, "checkpoint");
    Table table = createTable(location.toString());

    IcebergSource source = new IcebergSource();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);
    long initialSnapshotId = snapshotIds.get(0);

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString(),
        "checkpointLocation", checkpoint.toString()));
    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    // the size of each data file is around 600 bytes.
    // max size set to 1000
    List<MicroBatch> rateLimitedBatches = streamingReader.getChangesWithRateLimit(
        initialSnapshotId, 0, true, false, 1000);

    Assert.assertEquals(rateLimitedBatches.size(), 2);
    MicroBatch batch = rateLimitedBatches.get(0);
    Assert.assertEquals(batch.endFileIndex(), 1);
    Assert.assertTrue(batch.lastIndexOfSnapshot());
    Assert.assertTrue(batch.tasks().size() == 1);
    Assert.assertTrue(batch.sizeInBytes() < 1000 && batch.sizeInBytes() > 0);

    MicroBatch batch1 = rateLimitedBatches.get(1);
    Assert.assertEquals(batch1.endFileIndex(), 1);
    Assert.assertTrue(batch1.lastIndexOfSnapshot());
    Assert.assertTrue(batch1.tasks().size() == 1);
    Assert.assertTrue(batch1.sizeInBytes() < 1000 && batch1.sizeInBytes() > 0);

    // max size less than file size
    List<MicroBatch> rateLimitedBatches1 = streamingReader.getChangesWithRateLimit(
        batch1.snapshotId(), batch1.endFileIndex(), false, batch1.lastIndexOfSnapshot(), 100);

    Assert.assertEquals(rateLimitedBatches1.size(), 1);
    MicroBatch batch2 = rateLimitedBatches1.get(0);
    Assert.assertEquals(batch2.endFileIndex(), 1);
    Assert.assertTrue(batch2.lastIndexOfSnapshot());
    Assert.assertTrue(batch2.tasks().size() == 1);
    Assert.assertTrue(batch2.sizeInBytes() < 1000 && batch2.sizeInBytes() > 0);

    // max size set to 10000
    List<MicroBatch> rateLimitedBatches2 = streamingReader.getChangesWithRateLimit(
        batch2.snapshotId(), batch2.endFileIndex(), false, batch2.lastIndexOfSnapshot(), 10000);

    Assert.assertEquals(rateLimitedBatches2.size(), 1);
    MicroBatch batch3 = rateLimitedBatches2.get(0);
    Assert.assertEquals(batch3.endFileIndex(), 1);
    Assert.assertTrue(batch3.tasks().size() == 1);
    Assert.assertTrue(batch3.lastIndexOfSnapshot());
    Assert.assertTrue(batch3.sizeInBytes() < 1000 && batch3.sizeInBytes() > 0);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetOffset() throws IOException {
    File parent = temp.newFolder("test");
    File location = new File(parent, "table");
    File checkpoint = new File(parent, "checkpoint");
    Table table = createTable(location.toString());

    IcebergSource source = new IcebergSource();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);

    // default max size per batch
    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString(),
        "checkpointLocation", checkpoint.toString()));
    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);
    streamingReader.setOffsetRange(Optional.empty(), Optional.empty());

    StreamingOffset start = (StreamingOffset) streamingReader.getStartOffset();
    Assert.assertEquals(start.snapshotId(), snapshotIds.get(0).longValue());
    Assert.assertEquals(start.index(), 0);
    Assert.assertTrue(start.isStartingSnapshotId());
    Assert.assertFalse(start.isLastIndexOfSnapshot());

    StreamingOffset end = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals(end.snapshotId(), snapshotIds.get(3).longValue());
    Assert.assertEquals(end.index(), 1);
    Assert.assertFalse(end.isStartingSnapshotId());
    Assert.assertTrue(end.isLastIndexOfSnapshot());

    streamingReader.setOffsetRange(Optional.of(end), Optional.empty());
    StreamingOffset end1 = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals(end, end1);

    // max size to 1000
    DataSourceOptions options1 = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString(),
        "checkpointLocation", checkpoint.toString(),
        "max-size-per-batch", "1000"));
    StreamingReader streamingReader1 = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options1);

    streamingReader1.setOffsetRange(Optional.empty(), Optional.empty());
    StreamingOffset start1 = (StreamingOffset) streamingReader1.getStartOffset();
    Assert.assertEquals(start1.snapshotId(), snapshotIds.get(0).longValue());
    Assert.assertEquals(start1.index(), 0);
    Assert.assertTrue(start1.isStartingSnapshotId());
    Assert.assertFalse(start1.isLastIndexOfSnapshot());

    StreamingOffset end2 = (StreamingOffset) streamingReader1.getEndOffset();
    Assert.assertEquals(end2.snapshotId(), snapshotIds.get(1).longValue());
    Assert.assertEquals(end2.index(), 1);
    Assert.assertFalse(end2.isStartingSnapshotId());
    Assert.assertTrue(end2.isLastIndexOfSnapshot());

    streamingReader1.setOffsetRange(Optional.of(end2), Optional.empty());
    StreamingOffset end3 = (StreamingOffset) streamingReader1.getEndOffset();
    Assert.assertEquals(end3.snapshotId(), snapshotIds.get(3).longValue());
    Assert.assertEquals(end3.index(), 1);
    Assert.assertFalse(end3.isStartingSnapshotId());
    Assert.assertTrue(end3.isLastIndexOfSnapshot());

    streamingReader1.setOffsetRange(Optional.of(end3), Optional.empty());
    StreamingOffset end4 = (StreamingOffset) streamingReader1.getEndOffset();
    Assert.assertEquals(end3, end4);

    // max size to 100
    DataSourceOptions options2 = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString(),
        "checkpointLocation", checkpoint.toString(),
        "max-size-per-batch", "100"));
    StreamingReader streamingReader2 = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options2);

    streamingReader2.setOffsetRange(Optional.empty(), Optional.empty());
    StreamingOffset start2 = (StreamingOffset) streamingReader2.getStartOffset();
    Assert.assertEquals(start2.snapshotId(), snapshotIds.get(0).longValue());
    Assert.assertEquals(start2.index(), 0);
    Assert.assertTrue(start2.isStartingSnapshotId());
    Assert.assertFalse(start2.isLastIndexOfSnapshot());

    StreamingOffset end6 = (StreamingOffset) streamingReader2.getEndOffset();
    Assert.assertEquals(end6.snapshotId(), snapshotIds.get(0).longValue());
    Assert.assertEquals(end6.index(), 1);
    Assert.assertTrue(end6.isStartingSnapshotId());
    Assert.assertTrue(end6.isLastIndexOfSnapshot());

    streamingReader2.setOffsetRange(Optional.of(end6), Optional.empty());
    StreamingOffset end7 = (StreamingOffset) streamingReader2.getEndOffset();
    Assert.assertEquals(end7.snapshotId(), snapshotIds.get(1).longValue());
    Assert.assertEquals(end7.index(), 1);
    Assert.assertFalse(end7.isStartingSnapshotId());
    Assert.assertTrue(end7.isLastIndexOfSnapshot());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testWithSnapshotId() throws IOException {
    File parent = temp.newFolder("test");
    File location = new File(parent, "table");
    File checkpoint = new File(parent, "checkpoint");
    Table table = createTable(location.toString());

    IcebergSource source = new IcebergSource();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);

    // test invalid snapshot id
    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString(),
        "checkpointLocation", checkpoint.toString(),
        "starting-snapshot-id", "-1"));
    AssertHelpers.assertThrows("Test invalid snapshot id",
        IllegalStateException.class, "The option starting-snapshot-id -1 is not an ancestor",
        () -> source.createMicroBatchReader(Optional.empty(), checkpoint.toString(), options));

    // test specify snapshot-id
    DataSourceOptions options1 = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString(),
        "checkpointLocation", checkpoint.toString(),
        "starting-snapshot-id", snapshotIds.get(1).toString(),
        "max-size-per-batch", "1000"));
    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options1);

    streamingReader.setOffsetRange(Optional.empty(), Optional.empty());
    StreamingOffset start = (StreamingOffset) streamingReader.getStartOffset();
    Assert.assertEquals(start.snapshotId(), snapshotIds.get(1).longValue());
    Assert.assertEquals(start.index(), 0);
    Assert.assertTrue(start.isStartingSnapshotId());
    Assert.assertFalse(start.isLastIndexOfSnapshot());

    StreamingOffset end = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals(end.snapshotId(), snapshotIds.get(1).longValue());
    Assert.assertEquals(end.index(), 1);
    Assert.assertTrue(end.isStartingSnapshotId());
    Assert.assertFalse(end.isLastIndexOfSnapshot());

    streamingReader.setOffsetRange(Optional.of(end), Optional.empty());
    StreamingOffset end1 = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals(end1.snapshotId(), snapshotIds.get(2).longValue());
    Assert.assertEquals(end1.index(), 1);
    Assert.assertFalse(end1.isStartingSnapshotId());
    Assert.assertTrue(end1.isLastIndexOfSnapshot());

    streamingReader.setOffsetRange(Optional.of(end1), Optional.empty());
    StreamingOffset end2 = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals(end2.snapshotId(), snapshotIds.get(3).longValue());
    Assert.assertEquals(end2.index(), 1);
    Assert.assertFalse(end2.isStartingSnapshotId());
    Assert.assertTrue(end2.isLastIndexOfSnapshot());

    streamingReader.setOffsetRange(Optional.of(end2), Optional.empty());
    StreamingOffset end3 = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals(end3, end2);
  }

  private Table createTable(String location) {
    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table table = tables.create(SCHEMA, spec, location);

    List<List<SimpleRecord>> expected = Lists.newArrayList(
        Lists.newArrayList(new SimpleRecord(1, "1")),
        Lists.newArrayList(new SimpleRecord(2, "2")),
        Lists.newArrayList(new SimpleRecord(3, "3")),
        Lists.newArrayList(new SimpleRecord(4, "4"))
    );

    // Write records one by one to generate 4 snapshots.
    for (List<SimpleRecord> l : expected) {
      Dataset<Row> df = spark.createDataFrame(l, SimpleRecord.class);
      df.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .save(location);
    }
    table.refresh();

    return table;
  }
}
