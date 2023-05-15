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
package org.apache.iceberg.flink.sink;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.flink.sink.IcebergCheckpointCommitter.MAX_CONTINUOUS_EMPTY_COMMITS;
import static org.apache.iceberg.flink.sink.ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION;
import static org.apache.iceberg.hadoop.HadoopOutputFile.fromPath;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.RowDataConverter;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.flink.TestTableLoader;
import org.apache.iceberg.flink.data.PartitionedWriteResult;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.ThreadPools;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIcebergPartitionTimeCommitter extends TableTestBase {
  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()),
          required(3, "d", Types.StringType.get()),
          required(4, "h", Types.StringType.get()),
          required(5, "zip", Types.StringType.get()));

  protected String tablePath;
  protected File flinkManifestFolder;

  protected final FileFormat format;
  protected final boolean nonTimeField;

  @Parameterized.Parameters(name = "FileFormat = {0}, FormatVersion={1}, NonTimeField={2}")
  public static Object[][] parameters() {
    return new Object[][] {
      new Object[] {"avro", 1, true},
      new Object[] {"avro", 1, false},
      new Object[] {"avro", 2, true},
      new Object[] {"avro", 2, false}
    };
  }

  @Override
  @Before
  public void setupTable() throws IOException {
    flinkManifestFolder = temp.newFolder();

    this.tableDir = temp.newFolder();
    this.metadataDir = new File(tableDir, "metadata");
    Assert.assertTrue(tableDir.delete());

    tablePath = tableDir.getAbsolutePath();

    PartitionSpec spec;
    if (nonTimeField) {
      spec = PartitionSpec.builderFor(SCHEMA).identity("d").identity("h").identity("zip").build();
    } else {
      spec = PartitionSpec.builderFor(SCHEMA).identity("d").identity("h").build();
    }

    table = create(SCHEMA, spec);

    table
        .updateProperties()
        .set(DEFAULT_FILE_FORMAT, format.name())
        .set(FLINK_MANIFEST_LOCATION, flinkManifestFolder.getAbsolutePath())
        .set(MAX_CONTINUOUS_EMPTY_COMMITS, "1")
        .set(FlinkWriteOptions.SINK_PARTITION_COMMIT_ENABLED.key(), "true")
        .set(FlinkWriteOptions.SINK_PARTITION_COMMIT_DELAY.key(), "1h")
        .set(FlinkWriteOptions.SINK_PARTITION_COMMIT_POLICY_KIND.key(), "success-file")
        .set(FlinkWriteOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN.key(), "$d $h:00:00")
        .commit();
  }

  public TestIcebergPartitionTimeCommitter(String format, int formatVersion, boolean nonTimeField) {
    super(formatVersion);
    this.format = FileFormat.fromString(format);
    this.nonTimeField = nonTimeField;
  }

  private WriteResult of(DataFile dataFile, PartitionKey partitionKey) {
    return PartitionedWriteResult.partitionWriteResultBuilder()
        .addDataFiles(dataFile)
        .partitionKey(partitionKey)
        .build();
  }

  @Test
  public void testCommitTxn() throws Exception {
    long timestamp = 0;
    long checkpoint = 0;
    JobID jobID = new JobID();
    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());

    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobID)) {
      harness.setup();
      harness.open();
      OperatorID operatorId = harness.getOneInputOperator().getOperatorID();
      assertSnapshotSize(0);

      Record record1 = createRecord(table, 1, "data" + 1, "2022-02-02", "10", "1"); // 10h data
      partitionKey.partition(record1);
      harness.processElement(
          of(
              writeDataFile(
                  "data-1",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record1)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);

      Record record1Zip = createRecord(table, 11, "data" + 11, "2022-02-02", "10", "2"); // 10h data
      partitionKey.partition(record1Zip);
      harness.processElement(
          of(
              writeDataFile(
                  "data-1_2",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record1Zip)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);

      // At 10h watermark , 10h data uncommitted
      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 10, 5).toInstant(ZoneOffset.UTC).toEpochMilli());

      harness.snapshot(++checkpoint, ++timestamp);
      assertFlinkManifests(nonTimeField ? 2 : 1);

      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertTableRecords(table, ImmutableList.of());
      assertSnapshotSize(0);
      assertFlinkManifests(nonTimeField ? 2 : 1);

      Record record2 = createRecord(table, 2, "data" + 2, "2022-02-02", "11", "1"); // 11h data
      partitionKey.partition(record2);
      harness.processElement(
          of(
              writeDataFile(
                  "data-2",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record2)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);

      // At 11h watermark , 10h data committed, 11h data uncommitted
      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 11, 5).toInstant(ZoneOffset.UTC).toEpochMilli());

      harness.snapshot(++checkpoint, ++timestamp);
      assertFlinkManifests(nonTimeField ? 3 : 2);

      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertTableRecords(table, Lists.newArrayList(record1, record1Zip));
      assertSnapshotSize(1);
      assertFlinkManifests(1);

      if (nonTimeField) {
        Assert.assertTrue(
            new File(table.locationProvider().newDataLocation("d=2022-02-02/h=10/zip=1/_SUCCESS"))
                .exists());
      } else {
        Assert.assertTrue(
            new File(table.locationProvider().newDataLocation("d=2022-02-02/h=10/_SUCCESS"))
                .exists());
      }

      Record record3 = createRecord(table, 3, "data" + 3, "2022-02-02", "12", "1"); // 12h data
      partitionKey.partition(record3);
      harness.processElement(
          of(
              writeDataFile(
                  "data-3",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record3)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);

      Record record3Zip = createRecord(table, 32, "data" + 32, "2022-02-02", "12", "2"); // 12h data
      partitionKey.partition(record3Zip);
      harness.processElement(
          of(
              writeDataFile(
                  "data-3_2",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record3Zip)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);

      Record record4 = createRecord(table, 4, "data" + 4, "2022-02-02", "13", "1"); // 13h data
      partitionKey.partition(record4);
      harness.processElement(
          of(
              writeDataFile(
                  "data-4",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record4)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);

      Record record5 = createRecord(table, 5, "data" + 5, "2022-02-02", "14", "1"); // 14h data
      partitionKey.partition(record5);
      harness.processElement(
          of(
              writeDataFile(
                  "data-5",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record5)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);

      // At 15h watermark , 11h-14h data committed
      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 15, 5).toInstant(ZoneOffset.UTC).toEpochMilli());

      harness.snapshot(++checkpoint, ++timestamp);
      assertFlinkManifests(nonTimeField ? 5 : 4);

      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertTableRecords(
          table,
          Lists.newArrayList(record1, record1Zip, record2, record3, record3Zip, record4, record5));
      assertFlinkManifests(0);
      assertSnapshotSize(2);
      assertMaxCommittedCheckpointId(jobID, operatorId, checkpoint);

      Assert.assertEquals(
          TestIcebergPartitionTimeCommitter.class.getName(),
          table.currentSnapshot().summary().get("flink.test"));
    }
  }

  private static void assertTableRecords(Table table, List<Record> expected) throws IOException {
    table.refresh();

    Types.StructType type = table.schema().asStruct();
    StructLikeSet expectedSet = StructLikeSet.create(type);
    expectedSet.addAll(expected);

    try (CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
      StructLikeSet actualSet = StructLikeSet.create(type);

      for (Record record : iterable) {
        actualSet.add(record);
      }

      Assert.assertEquals("Should produce the expected record", expectedSet, actualSet);
    }
  }

  private Record createRecord(
      Table table, Integer id, String data, String day, String hour, String zip) {
    Record record = GenericRecord.create(table.schema());
    record.setField("id", id);
    record.setField("data", data);
    record.setField("d", day);
    record.setField("h", hour);
    record.setField("zip", zip);
    return record;
  }

  @Test
  public void testOrderedEventsBetweenCheckpoints() throws Exception {
    long timestamp = 0;
    JobID jobId = new JobID();
    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());

    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.open();

      OperatorID operatorId = harness.getOneInputOperator().getOperatorID();
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      Record record1 = createRecord(table, 1, "data" + 1, "2022-02-02", "10", "1");
      partitionKey.partition(record1);
      RowData rowData1 = RowDataConverter.convert(table.schema(), record1);
      harness.processElement(
          of(
              writeDataFile("data-1", ImmutableList.of(rowData1), partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);

      Record record1Zip = createRecord(table, 1, "data" + 1, "2022-02-02", "10", "2");
      partitionKey.partition(record1Zip);

      harness.processElement(
          of(
              writeDataFile(
                  "data-1_2",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record1Zip)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);
      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 11, 0).toInstant(ZoneOffset.UTC).toEpochMilli());
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      // 1. snapshotState for checkpoint#1
      long firstCheckpointId = 1;
      harness.snapshot(firstCheckpointId, ++timestamp);
      assertFlinkManifests(nonTimeField ? 2 : 1);

      Record record2 = createRecord(table, 2, "data" + 2, "2022-02-02", "11", "1");
      partitionKey.partition(record2);
      DataFile dataFile2 =
          writeDataFile(
              "data-2",
              ImmutableList.of(RowDataConverter.convert(table.schema(), record2)),
              partitionKey.copy());

      harness.processElement(of(dataFile2, partitionKey.copy()), ++timestamp);
      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 12, 5).toInstant(ZoneOffset.UTC).toEpochMilli());
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      // 2. snapshotState for checkpoint#2
      long secondCheckpointId = 2;
      harness.snapshot(secondCheckpointId, ++timestamp);
      assertFlinkManifests(nonTimeField ? 3 : 2);

      // 3. notifyCheckpointComplete for checkpoint#1
      harness.notifyOfCompletedCheckpoint(firstCheckpointId);
      assertTableRecords(table, Lists.newArrayList(record1, record1Zip));
      assertMaxCommittedCheckpointId(jobId, operatorId, firstCheckpointId);
      assertFlinkManifests(1);

      // 4. notifyCheckpointComplete for checkpoint#2
      harness.notifyOfCompletedCheckpoint(secondCheckpointId);
      assertTableRecords(table, Lists.newArrayList(record1, record1Zip, record2));
      assertMaxCommittedCheckpointId(jobId, operatorId, secondCheckpointId);
      assertFlinkManifests(0);
    }
  }

  @Test
  public void testDisorderedEventsBetweenCheckpoints() throws Exception {
    long timestamp = 0;
    JobID jobId = new JobID();
    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());

    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.open();

      OperatorID operatorId = harness.getOneInputOperator().getOperatorID();
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      Record record1 = createRecord(table, 1, "data" + 1, "2022-02-02", "10", "1");
      partitionKey.partition(record1);
      DataFile dataFile1 =
          writeDataFile(
              "data-1",
              ImmutableList.of(RowDataConverter.convert(table.schema(), record1)),
              partitionKey.copy());
      harness.processElement(of(dataFile1, partitionKey.copy()), ++timestamp);

      Record record1Zip = createRecord(table, 2, "data" + 1, "2022-02-02", "10", "2");
      partitionKey.partition(record1Zip);
      harness.processElement(
          of(
              writeDataFile(
                  "data-1_2",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record1Zip)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);

      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 11, 0).toInstant(ZoneOffset.UTC).toEpochMilli());
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      // 1. snapshotState for checkpoint#1
      long firstCheckpointId = 1;
      harness.snapshot(firstCheckpointId, ++timestamp);
      assertFlinkManifests(nonTimeField ? 2 : 1);

      Record record3 = createRecord(table, 3, "data" + 2, "2022-02-02", "11", "1");
      partitionKey.partition(record3);
      harness.processElement(
          of(
              writeDataFile(
                  "data-3",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record3)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);

      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 12, 5).toInstant(ZoneOffset.UTC).toEpochMilli());
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      // 2. snapshotState for checkpoint#2
      long secondCheckpointId = 2;
      harness.snapshot(secondCheckpointId, ++timestamp);
      assertFlinkManifests(nonTimeField ? 3 : 2);

      // 3. notifyCheckpointComplete for checkpoint#2
      harness.notifyOfCompletedCheckpoint(secondCheckpointId);
      assertTableRecords(table, Lists.newArrayList(record1, record1Zip, record3));
      assertMaxCommittedCheckpointId(jobId, operatorId, secondCheckpointId);
      assertFlinkManifests(0);

      // 4. notifyCheckpointComplete for checkpoint#1
      harness.notifyOfCompletedCheckpoint(firstCheckpointId);
      assertTableRecords(table, Lists.newArrayList(record1, record1Zip, record3));
      assertMaxCommittedCheckpointId(jobId, operatorId, secondCheckpointId);
      assertFlinkManifests(0);
    }
  }

  @Test
  public void testRecoveryFromValidSnapshot() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;
    List<Record> expectedRecords = Lists.newArrayList();
    List<Record> oldRecords = Lists.newArrayList();
    OperatorSubtaskState snapshot;

    JobID jobId = new JobID();
    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());

    OperatorID operatorId;
    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.open();

      assertSnapshotSize(0);
      operatorId = harness.getOneInputOperator().getOperatorID();
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      Record record1 = createRecord(table, 1, "data" + 1, "2022-02-02", "10", "1");
      expectedRecords.add(record1);
      oldRecords.add(record1);
      partitionKey.partition(record1);
      harness.processElement(
          of(
              writeDataFile(
                  "data-1",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record1)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);

      Record record1Zip = createRecord(table, 2, "data" + 2, "2022-02-02", "10", "2");
      expectedRecords.add(record1Zip);
      oldRecords.add(record1Zip);
      partitionKey.partition(record1Zip);
      harness.processElement(
          of(
              writeDataFile(
                  "data-1_2",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record1Zip)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);

      Record record3 = createRecord(table, 3, "data" + 3, "2022-02-02", "11", "1");
      expectedRecords.add(record3);
      partitionKey.partition(record3);
      harness.processElement(
          of(
              writeDataFile(
                  "data-3",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record3)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);

      // At 11h watermark , 10h data committed, 11h data uncommitted
      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 11, 5).toInstant(ZoneOffset.UTC).toEpochMilli());

      snapshot = harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(nonTimeField ? 3 : 2);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertSnapshot(1, oldRecords, 1, jobId, checkpointId, operatorId);
    }

    // Restore from the given snapshot
    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.getStreamConfig().setOperatorID(operatorId);
      harness.setup();
      harness.initializeState(snapshot);
      harness.open();

      assertTableRecords(table, oldRecords);
      assertFlinkManifests(1);
      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpointId);

      Record record4 = createRecord(table, 4, "data" + 4, "2022-02-02", "11", "1"); // 11h  data2
      expectedRecords.add(record4);
      partitionKey.partition(record4);
      harness.processElement(
          of(
              writeDataFile(
                  "data-4",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record4)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);

      Record record5 = createRecord(table, 5, "data" + 5, "2022-02-02", "11", "2"); // 11h  data2
      expectedRecords.add(record5);
      partitionKey.partition(record5);
      harness.processElement(
          of(
              writeDataFile(
                  "data-5",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record5)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);

      // At 12h watermark , 11h data committed
      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 12, 5).toInstant(ZoneOffset.UTC).toEpochMilli());

      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(nonTimeField ? 3 : 2);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertSnapshot(0, expectedRecords, 2, jobId, checkpointId, operatorId);
    }
  }

  @Test
  public void testRecoveryFromSnapshotWithoutCompletedNotification() throws Exception {
    // We've two steps in checkpoint: 1. snapshotState(ckp); 2. notifyCheckpointComplete(ckp). It's
    // possible that we flink job will restore from a checkpoint with only step#1 finished.
    long checkpointId = 0;
    long timestamp = 0;
    OperatorSubtaskState snapshot;
    List<Record> expectedRecords = Lists.newArrayList();
    List<Record> oldRecords = Lists.newArrayList();
    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
    Record record1 = createRecord(table, 1, "data" + 1, "2022-02-02", "10", "1");
    Record record1Zip = createRecord(table, 2, "data" + 1, "2022-02-02", "10", "2");
    Record record3 = createRecord(table, 3, "data" + 2, "2022-02-02", "11", "1");
    Record record4 = createRecord(table, 4, "data" + 3, "2022-02-02", "12", "2");
    Record record5 = createRecord(table, 5, "data" + 4, "2022-02-02", "13", "1");
    Record record6 = createRecord(table, 6, "data" + 4, "2022-02-02", "11", "1");
    Record record7 = createRecord(table, 7, "data" + 4, "2022-02-02", "11", "2");

    JobID jobId = new JobID();
    OperatorID operatorId;
    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.open();
      assertSnapshotSize(0);
      operatorId = harness.getOneInputOperator().getOperatorID();
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);
      expectedRecords.add(record1);
      oldRecords.add(record1);
      partitionKey.partition(record1);
      harness.processElement(
          of(
              writeDataFile(
                  "data-1",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record1)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);
      expectedRecords.add(record1Zip);
      oldRecords.add(record1Zip);
      partitionKey.partition(record1Zip);
      harness.processElement(
          of(
              writeDataFile(
                  "data-1_2",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record1Zip)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);
      partitionKey.partition(record3);
      harness.processElement(
          of(
              writeDataFile(
                  "data-3",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record3)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);
      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 11, 5).toInstant(ZoneOffset.UTC).toEpochMilli());
      snapshot = harness.snapshot(++checkpointId, ++timestamp);
      assertSnapshot(nonTimeField ? 3 : 2, ImmutableList.of(), 0, jobId, -1L, operatorId);
    }

    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.getStreamConfig().setOperatorID(operatorId);
      harness.setup();
      harness.initializeState(snapshot);
      harness.open();
      assertSnapshot(nonTimeField ? 3 : 2, Lists.newArrayList(), 0, jobId, -1, operatorId);
      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 11, 5).toInstant(ZoneOffset.UTC).toEpochMilli());
      harness.snapshot(++checkpointId, ++timestamp); // shot2
      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertSnapshot(1, expectedRecords, 1, jobId, checkpointId, operatorId);
      partitionKey.partition(record4);
      harness.processElement(
          of(
              writeDataFile(
                  "data-4",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record4)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);
      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 12, 5).toInstant(ZoneOffset.UTC).toEpochMilli());
      snapshot = harness.snapshot(checkpointId, ++timestamp); // shot3
      assertFlinkManifests(2);
    }

    JobID newJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness =
        createStreamSink(newJobId)) {
      harness.getStreamConfig().setOperatorID(operatorId);
      harness.setup();
      harness.initializeState(snapshot);
      harness.open();
      assertFlinkManifests(2);
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpointId);
      assertMaxCommittedCheckpointId(newJobId, operatorId, -1);
      assertTableRecords(table, expectedRecords);
      expectedRecords.add(record3);
      expectedRecords.add(record4);
      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 13, 15).toInstant(ZoneOffset.UTC).toEpochMilli());
      harness.snapshot(++checkpointId, ++timestamp); // shot4
      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertSnapshot(0, expectedRecords, 2, newJobId, checkpointId, operatorId);
      expectedRecords.add(record5);
      partitionKey.partition(record5);
      harness.processElement(
          of(
              writeDataFile(
                  "data-5",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record5)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);
      expectedRecords.add(record6);
      partitionKey.partition(record6);
      harness.processElement(
          of(
              writeDataFile(
                  "data-6",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record6)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);
      expectedRecords.add(record7);
      partitionKey.partition(record7);
      harness.processElement(
          of(
              writeDataFile(
                  "data-7",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record7)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);
      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 14, 5).toInstant(ZoneOffset.UTC).toEpochMilli());
      harness.snapshot(++checkpointId, ++timestamp); // shot4
      assertFlinkManifests(nonTimeField ? 3 : 2);
      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertSnapshot(0, expectedRecords, 3, newJobId, checkpointId, operatorId);
    }
  }

  private void assertSnapshot(
      int expectedFlinkManifests,
      List<Record> expectedRecords,
      int expectedSnapshotSize,
      JobID newJobId,
      long checkpointId,
      OperatorID operatorId)
      throws IOException {
    assertFlinkManifests(expectedFlinkManifests);
    assertTableRecords(table, expectedRecords);
    assertSnapshotSize(expectedSnapshotSize);
    assertMaxCommittedCheckpointId(newJobId, operatorId, checkpointId);
  }

  @Test
  public void testStartAnotherJobToWriteSameTable() throws Exception {
    long checkpointId = 0;
    long timestamp = 0;
    List<Record> tableRecords = Lists.newArrayList();
    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());

    JobID oldJobId = new JobID();
    OperatorID operatorId;
    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness =
        createStreamSink(oldJobId)) {
      harness.setup();
      harness.open();

      assertSnapshotSize(0);
      operatorId = harness.getOneInputOperator().getOperatorID();
      assertMaxCommittedCheckpointId(oldJobId, operatorId, -1L);

      Record record = createRecord(table, 1, "data" + 1, "2022-02-02", "10", "1");
      tableRecords.add(record);
      partitionKey.partition(record);
      harness.processElement(
          of(
              writeDataFile(
                  "data-1",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);
      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 11, 5).toInstant(ZoneOffset.UTC).toEpochMilli());

      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(1);
      assertSnapshotSize(0);

      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertSnapshot(0, tableRecords, 1, oldJobId, checkpointId, operatorId);
    }

    // The new started job will start with checkpoint = 1 again.
    checkpointId = 0;
    timestamp = 0;
    JobID newJobId = new JobID();
    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness =
        createStreamSink(newJobId)) {
      harness.getStreamConfig().setOperatorID(operatorId);
      harness.setup();
      harness.open();

      assertSnapshotSize(1);
      assertMaxCommittedCheckpointId(oldJobId, operatorId, 1);
      assertMaxCommittedCheckpointId(newJobId, operatorId, -1);

      Record record = createRecord(table, 1, "data1-new", "2022-02-02", "11", "1");
      tableRecords.add(record);
      partitionKey.partition(record);

      harness.processElement(
          of(
              writeDataFile(
                  "data-new-1",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);
      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 12, 5).toInstant(ZoneOffset.UTC).toEpochMilli());
      harness.snapshot(++checkpointId, ++timestamp);
      assertFlinkManifests(1);

      harness.notifyOfCompletedCheckpoint(checkpointId);
      assertSnapshot(0, tableRecords, 2, newJobId, checkpointId, operatorId);
    }
  }

  @Test
  public void testMultipleJobsWriteSameTable() throws Exception {
    long timestamp = 0;

    List<Record> tableRecords = Lists.newArrayList();
    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());

    JobID[] jobs = new JobID[] {new JobID(), new JobID(), new JobID()};
    OperatorID[] operatorIds =
        new OperatorID[] {new OperatorID(), new OperatorID(), new OperatorID()};
    for (int i = 0; i < 20; i++) {
      int jobIndex = i % 3;
      int checkpointId = i / 3;
      JobID jobId = jobs[jobIndex];
      OperatorID operatorId = operatorIds[jobIndex];
      try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
        harness.getStreamConfig().setOperatorID(operatorId);
        harness.setup();
        harness.open();

        assertSnapshotSize(i);
        assertMaxCommittedCheckpointId(jobId, operatorId, checkpointId == 0 ? -1 : checkpointId);

        Record record =
            createRecord(table, i, "data" + i, "2022-02-02", String.format("1%d", i / 3), "1");
        partitionKey.partition(record);
        tableRecords.add(record);
        DataFile dataFile =
            writeDataFile(
                String.format("data-%d", i),
                ImmutableList.of(RowDataConverter.convert(table.schema(), record)),
                partitionKey.copy());
        harness.processElement(of(dataFile, partitionKey.copy()), ++timestamp);
        harness.processWatermark(
            LocalDateTime.of(2022, 2, 2, 11 + i / 3, 5).toInstant(ZoneOffset.UTC).toEpochMilli());
        harness.snapshot(checkpointId + 1, ++timestamp);
        assertFlinkManifests(1);

        harness.notifyOfCompletedCheckpoint(checkpointId + 1);
        assertSnapshot(0, tableRecords, i + 1, jobId, checkpointId + 1, operatorId);
      }
    }
  }

  @Test
  public void testBoundedStream() throws Exception {
    JobID jobId = new JobID();
    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());

    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.open();

      assertFlinkManifests(0);
      assertSnapshotSize(0);
      OperatorID operatorId = harness.getOneInputOperator().getOperatorID();
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      Record record1 = createRecord(table, 1, "data" + 1, "2022-02-02", "10", "1");
      partitionKey.partition(record1);
      harness.processElement(
          of(
              writeDataFile(
                  "data-1",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record1)),
                  partitionKey.copy()),
              partitionKey.copy()),
          1);

      Record record1Zip = createRecord(table, 2, "data" + 2, "2022-02-02", "10", "2");
      partitionKey.partition(record1Zip);
      harness.processElement(
          of(
              writeDataFile(
                  "data-2",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record1Zip)),
                  partitionKey.copy()),
              partitionKey.copy()),
          1);

      ((BoundedOneInput) harness.getOneInputOperator()).endInput(); // commitUpToCheckpoint

      assertSnapshot(
          0, Lists.newArrayList(record1, record1Zip), 1, jobId, Long.MAX_VALUE, operatorId);
      Assert.assertEquals(
          TestIcebergPartitionTimeCommitter.class.getName(),
          table.currentSnapshot().summary().get("flink.test"));
    }
  }

  @Test
  public void testFlinkManifests() throws Exception {
    long timestamp = 0;
    final long checkpoint = 10;

    JobID jobId = new JobID();
    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());

    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.open();

      OperatorID operatorId = harness.getOneInputOperator().getOperatorID();
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      Record record = createRecord(table, 1, "data" + 1, "2022-02-02", "10", "1");
      partitionKey.partition(record);

      harness.processElement(
          of(
              writeDataFile(
                  "data-1",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);
      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 11, 5).toInstant(ZoneOffset.UTC).toEpochMilli());

      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      // 1. snapshotState for checkpoint#1
      harness.snapshot(checkpoint, ++timestamp);
      List<Path> manifestPaths = assertFlinkManifests(1);
      Path manifestPath = manifestPaths.get(0);
      Assert.assertEquals(
          "File name should have the expected pattern.",
          String.format("%s-%s-%05d-%d-%d-%05d.avro", jobId, operatorId, 0, 0, checkpoint, 1),
          manifestPath.getFileName().toString());

      // 2. Read the data files from manifests and assert.
      List<DataFile> dataFiles =
          FlinkManifestUtil.readDataFiles(createTestingManifestFile(manifestPath), table.io());
      Assert.assertEquals(1, dataFiles.size());
      TestHelpers.assertEquals(
          writeDataFile(
              "data-1",
              ImmutableList.of(RowDataConverter.convert(table.schema(), record)),
              partitionKey.copy()),
          dataFiles.get(0));

      // 3. notifyCheckpointComplete for checkpoint#1
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertTableRecords(table, Lists.newArrayList(record));
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpoint);
      assertFlinkManifests(0);
    }
  }

  @Test
  public void testDeleteFiles() throws Exception {
    Assume.assumeFalse("Only support equality-delete in format v2.", formatVersion < 2);

    long timestamp = 0;
    long checkpoint = 10;

    JobID jobId = new JobID();
    FileAppenderFactory<RowData> appenderFactory = createDeletableAppenderFactory();

    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());

    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.open();

      OperatorID operatorId = harness.getOneInputOperator().getOperatorID();
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      Record record1 = createRecord(table, 1, "data" + 1, "2022-02-02", "10", "1");
      partitionKey.partition(record1);
      harness.processElement(
          of(
              writeDataFile(
                  "data-file-1",
                  ImmutableList.of(RowDataConverter.convert(table.schema(), record1)),
                  partitionKey.copy()),
              partitionKey.copy()),
          ++timestamp);
      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 11, 5).toInstant(ZoneOffset.UTC).toEpochMilli());
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      // 1. snapshotState for checkpoint#1
      harness.snapshot(checkpoint, ++timestamp);
      List<Path> manifestPaths = assertFlinkManifests(1);
      Path manifestPath = manifestPaths.get(0);
      Assert.assertEquals(
          "File name should have the expected pattern.",
          String.format("%s-%s-%05d-%d-%d-%05d.avro", jobId, operatorId, 0, 0, checkpoint, 1),
          manifestPath.getFileName().toString());

      // 2. Read the data files from manifests and assert.
      List<DataFile> dataFiles =
          FlinkManifestUtil.readDataFiles(createTestingManifestFile(manifestPath), table.io());
      Assert.assertEquals(1, dataFiles.size());
      TestHelpers.assertEquals(
          writeDataFile(
              "data-file-1",
              ImmutableList.of(RowDataConverter.convert(table.schema(), record1)),
              partitionKey.copy()),
          dataFiles.get(0));

      // 3. notifyCheckpointComplete for checkpoint#1
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertTableRecords(table, Lists.newArrayList(record1));
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpoint);
      assertFlinkManifests(0);

      // 4. process both data files and delete files.
      RowData delete1 = RowDataConverter.convert(table.schema(), record1);
      delete1.setRowKind(RowKind.DELETE);
      DeleteFile deleteFile1 =
          writeEqDeleteFile(
              appenderFactory, "delete-file-1", ImmutableList.of(delete1), partitionKey.copy());

      Record record2 = createRecord(table, 2, "data" + 2, "2022-02-02", "11", "1");
      partitionKey.partition(record2);
      DataFile dataFile2 =
          writeDataFile(
              "data-file-2",
              ImmutableList.of(RowDataConverter.convert(table.schema(), record2)),
              partitionKey.copy());

      harness.processElement(
          PartitionedWriteResult.partitionWriteResultBuilder()
              .addDataFiles(dataFile2)
              .addDeleteFiles(deleteFile1)
              .partitionKey(partitionKey)
              .build(),
          ++timestamp);
      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 12, 5).toInstant(ZoneOffset.UTC).toEpochMilli());
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpoint);

      // 5. snapshotState for checkpoint#2
      harness.snapshot(++checkpoint, ++timestamp);
      assertFlinkManifests(2);

      // 6. notifyCheckpointComplete for checkpoint#2
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertTableRecords(table, Lists.newArrayList(record2));
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpoint);
      assertFlinkManifests(0);
    }
  }

  @Test
  public void testCommitTwoCheckpointsInSingleTxn() throws Exception {
    Assume.assumeFalse("Only support equality-delete in format v2.", formatVersion < 2);

    long timestamp = 0;
    long checkpoint = 10;

    JobID jobId = new JobID();
    FileAppenderFactory<RowData> appenderFactory = createDeletableAppenderFactory();

    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());

    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobId)) {
      harness.setup();
      harness.open();

      OperatorID operatorId = harness.getOneInputOperator().getOperatorID();
      assertMaxCommittedCheckpointId(jobId, operatorId, -1L);

      Record record1 = createRecord(table, 1, "data" + 1, "2022-02-02", "10", "1");
      Record record2 = createRecord(table, 2, "data" + 2, "2022-02-02", "10", "1");
      Record record3 = createRecord(table, 3, "data" + 3, "2022-02-02", "10", "1");
      partitionKey.partition(record1);
      RowData insert1 = RowDataConverter.convert(table.schema(), record1);
      RowData insert2 = RowDataConverter.convert(table.schema(), record2);
      RowData delete3 = RowDataConverter.convert(table.schema(), record3);
      delete3.setRowKind(RowKind.DELETE);

      DataFile dataFile1 =
          writeDataFile("data-file-1", ImmutableList.of(insert1, insert2), partitionKey.copy());
      DeleteFile deleteFile1 =
          writeEqDeleteFile(
              appenderFactory, "delete-file-1", ImmutableList.of(delete3), partitionKey.copy());
      harness.processElement(
          PartitionedWriteResult.partitionWriteResultBuilder()
              .addDataFiles(dataFile1)
              .addDeleteFiles(deleteFile1)
              .partitionKey(partitionKey)
              .build(),
          ++timestamp);

      // The 1th snapshotState.
      harness.snapshot(checkpoint, ++timestamp);

      Record record4 = createRecord(table, 4, "data" + 4, "2022-02-02", "10", "1");
      partitionKey.partition(record4);
      RowData insert4 = RowDataConverter.convert(table.schema(), record4);
      RowData delete2 = RowDataConverter.convert(table.schema(), record2);
      delete2.setRowKind(RowKind.DELETE);

      DataFile dataFile2 =
          writeDataFile("data-file-2", ImmutableList.of(insert4), partitionKey.copy());
      DeleteFile deleteFile2 =
          writeEqDeleteFile(
              appenderFactory, "delete-file-2", ImmutableList.of(delete2), partitionKey.copy());
      harness.processElement(
          PartitionedWriteResult.partitionWriteResultBuilder()
              .addDataFiles(dataFile2)
              .addDeleteFiles(deleteFile2)
              .partitionKey(partitionKey)
              .build(),
          ++timestamp);
      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 11, 5).toInstant(ZoneOffset.UTC).toEpochMilli());

      // The 2nd snapshotState.
      harness.snapshot(++checkpoint, ++timestamp);
      assertSnapshotSize(0);

      // Notify the 2nd snapshot to complete.
      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertTableRecords(table, Lists.newArrayList(record1, record4));
      assertMaxCommittedCheckpointId(jobId, operatorId, checkpoint);
      assertFlinkManifests(0);
      Assert.assertEquals(
          "Should have committed 2 txn.", 2, ImmutableList.copyOf(table.snapshots()).size());
    }
  }

  @Test
  public void testEscapingSpecialCharacters() throws Exception {
    table
        .updateProperties()
        .set(FlinkWriteOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN.key(), "$d $h")
        .set(FlinkWriteOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_FORMATTER.key(), "yyyy MM dd HH")
        .commit();

    long timestamp = 0;
    long checkpoint = 0;
    JobID jobID = new JobID();
    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());

    try (OneInputStreamOperatorTestHarness<WriteResult, Void> harness = createStreamSink(jobID)) {
      harness.setup();
      harness.open();
      assertSnapshotSize(0);

      Record record1 = createRecord(table, 1, "data" + 1, "2022 02 02", "10", "1"); // 10h data
      partitionKey.partition(record1);
      RowData rowData1 = RowDataConverter.convert(table.schema(), record1);
      DataFile dataFile1 =
          writeDataFile("data-" + 1, ImmutableList.of(rowData1), partitionKey.copy());
      harness.processElement(of(dataFile1, partitionKey.copy()), ++timestamp);

      // At 10h watermark , 10h data uncommitted
      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 10, 5).toInstant(ZoneOffset.UTC).toEpochMilli());

      harness.snapshot(++checkpoint, ++timestamp);
      assertFlinkManifests(1);

      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertTableRecords(table, ImmutableList.of());
      assertSnapshotSize(0);
      assertFlinkManifests(1);

      Record record2 = createRecord(table, 2, "data" + 2, "2022 02 02", "10", "1"); // 11h data
      partitionKey.partition(record2);
      RowData rowData2 = RowDataConverter.convert(table.schema(), record2);
      DataFile dataFile2 =
          writeDataFile("data-" + 2, ImmutableList.of(rowData2), partitionKey.copy());
      harness.processElement(of(dataFile2, partitionKey.copy()), ++timestamp);

      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 11, 5).toInstant(ZoneOffset.UTC).toEpochMilli());

      harness.snapshot(++checkpoint, ++timestamp);
      assertFlinkManifests(2);

      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertTableRecords(table, Lists.newArrayList(record1, record2));
      assertSnapshotSize(1);
      assertFlinkManifests(0);

      Record record3 = createRecord(table, 2, "data" + 2, "2022 02 02", "12", "1"); // 12h data
      partitionKey.partition(record3);
      RowData rowData3 = RowDataConverter.convert(table.schema(), record3);
      DataFile dataFile3 =
          writeDataFile("data-" + 2, ImmutableList.of(rowData3), partitionKey.copy());
      harness.processElement(of(dataFile3, partitionKey.copy()), ++timestamp);

      Record record4 = createRecord(table, 2, "data" + 2, "2022 02 02", "13", "1"); // 13h data
      partitionKey.partition(record4);
      RowData rowData4 = RowDataConverter.convert(table.schema(), record4);
      DataFile dataFile4 =
          writeDataFile("data-" + 2, ImmutableList.of(rowData4), partitionKey.copy());
      harness.processElement(of(dataFile4, partitionKey.copy()), ++timestamp);

      Record record5 = createRecord(table, 2, "data" + 2, "2022 02 02", "14", "1"); // 14h data
      partitionKey.partition(record5);
      RowData rowData5 = RowDataConverter.convert(table.schema(), record5);
      DataFile dataFile5 =
          writeDataFile("data-" + 2, ImmutableList.of(rowData5), partitionKey.copy());
      harness.processElement(of(dataFile5, partitionKey.copy()), ++timestamp);

      // At 15h watermark , 11h-14h data committed
      harness.processWatermark(
          LocalDateTime.of(2022, 2, 2, 15, 5).toInstant(ZoneOffset.UTC).toEpochMilli());

      harness.snapshot(++checkpoint, ++timestamp);
      assertFlinkManifests(3);

      harness.notifyOfCompletedCheckpoint(checkpoint);
      assertTableRecords(table, Lists.newArrayList(record1, record2, record3, record4, record5));
      assertFlinkManifests(0);
      assertSnapshotSize(2);
      assertMaxCommittedCheckpointId(
          jobID, harness.getOneInputOperator().getOperatorID(), checkpoint);

      Assert.assertEquals(
          TestIcebergPartitionTimeCommitter.class.getName(),
          table.currentSnapshot().summary().get("flink.test"));
    }
  }

  private DeleteFile writeEqDeleteFile(
      FileAppenderFactory<RowData> appenderFactory,
      String filename,
      List<RowData> deletes,
      PartitionKey partitionKey)
      throws IOException {
    return writeEqDeleteFile(
        table, FileFormat.PARQUET, tablePath, filename, appenderFactory, deletes, partitionKey);
  }

  public static DeleteFile writeEqDeleteFile(
      Table table,
      FileFormat format,
      String tablePath,
      String filename,
      FileAppenderFactory<RowData> appenderFactory,
      List<RowData> deletes,
      PartitionKey partitionKey)
      throws IOException {
    EncryptedOutputFile outputFile =
        table
            .encryption()
            .encrypt(
                fromPath(new org.apache.hadoop.fs.Path(tablePath, filename), new Configuration()));

    EqualityDeleteWriter<RowData> eqWriter =
        appenderFactory.newEqDeleteWriter(outputFile, format, partitionKey);
    try (EqualityDeleteWriter<RowData> writer = eqWriter) {
      writer.write(deletes);
    }
    return eqWriter.toDeleteFile();
  }

  private FileAppenderFactory<RowData> createDeletableAppenderFactory() {
    int[] equalityFieldIds =
        new int[] {
          table.schema().findField("id").fieldId(), table.schema().findField("data").fieldId()
        };
    return new FlinkAppenderFactory(
        table,
        table.schema(),
        FlinkSchemaUtil.convert(table.schema()),
        table.properties(),
        table.spec(),
        equalityFieldIds,
        table.schema(),
        null);
  }

  private ManifestFile createTestingManifestFile(Path manifestPath) {
    return new GenericManifestFile(
        manifestPath.toAbsolutePath().toString(),
        manifestPath.toFile().length(),
        0,
        ManifestContent.DATA,
        0,
        0,
        0L,
        0,
        0,
        0,
        0,
        0,
        0,
        null,
        null);
  }

  private List<Path> assertFlinkManifests(int expectedCount) throws IOException {
    List<Path> manifests =
        Files.list(flinkManifestFolder.toPath())
            .filter(p -> !p.toString().endsWith(".crc"))
            .collect(Collectors.toList());
    Assert.assertEquals(
        String.format("Expected %s flink manifests, but the list is: %s", expectedCount, manifests),
        expectedCount,
        manifests.size());
    return manifests;
  }

  private DataFile writeDataFile(String filename, List<RowData> rows, PartitionKey partitionKey)
      throws IOException {
    return writeFile(
        table,
        table.schema(),
        table.spec(),
        CONF,
        tablePath,
        format.addExtension(filename),
        rows,
        partitionKey);
  }

  public static DataFile writeFile(
      Table table,
      Schema schema,
      PartitionSpec spec,
      Configuration conf,
      String location,
      String filename,
      List<RowData> rows,
      PartitionKey partitionKey)
      throws IOException {
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(location, filename);
    FileFormat fileFormat = FileFormat.fromFileName(filename);
    Preconditions.checkNotNull(fileFormat, "Cannot determine format for file: %s", filename);

    RowType flinkSchema = FlinkSchemaUtil.convert(schema);
    FileAppenderFactory<RowData> appenderFactory =
        new FlinkAppenderFactory(
            table, schema, flinkSchema, ImmutableMap.of(), spec, null, null, null);

    FileAppender<RowData> appender = appenderFactory.newAppender(fromPath(path, conf), fileFormat);
    try (FileAppender<RowData> closeableAppender = appender) {
      closeableAppender.addAll(rows);
    }

    return DataFiles.builder(spec)
        .withInputFile(HadoopInputFile.fromPath(path, conf))
        .withPartition(partitionKey)
        .withMetrics(appender.metrics())
        .build();
  }

  private void assertMaxCommittedCheckpointId(JobID jobID, OperatorID operatorId, long expectedId) {
    table.refresh();
    long actualId =
        IcebergCheckpointCommitter.getMaxCommittedCheckpointId(
            table, jobID.toString(), operatorId.toHexString(), SnapshotRef.MAIN_BRANCH);
    Assert.assertEquals(expectedId, actualId);
  }

  private void assertSnapshotSize(int expectedSnapshotSize) {
    table.refresh();
    Assert.assertEquals(expectedSnapshotSize, Lists.newArrayList(table.snapshots()).size());
  }

  private OneInputStreamOperatorTestHarness<WriteResult, Void> createStreamSink(JobID jobID)
      throws Exception {
    TestOperatorFactory factory = TestOperatorFactory.of(tablePath, table);
    return new OneInputStreamOperatorTestHarness<>(factory, createEnvironment(jobID));
  }

  private static MockEnvironment createEnvironment(JobID jobID) {
    return new MockEnvironmentBuilder()
        .setTaskName("test task")
        .setManagedMemorySize(32 * 1024)
        .setInputSplitProvider(new MockInputSplitProvider())
        .setBufferSize(256)
        .setTaskConfiguration(new org.apache.flink.configuration.Configuration())
        .setExecutionConfig(new ExecutionConfig())
        .setMaxParallelism(16)
        .setJobID(jobID)
        .build();
  }

  private static class TestOperatorFactory extends AbstractStreamOperatorFactory<Void>
      implements OneInputStreamOperatorFactory<WriteResult, Void> {
    private final String tablePath;
    private final Table table;

    private TestOperatorFactory(String tablePath, Table table) {
      this.tablePath = tablePath;
      this.table = table;
    }

    private static TestOperatorFactory of(String tablePath, Table table) {
      return new TestOperatorFactory(tablePath, table);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<Void>> T createStreamOperator(
        StreamOperatorParameters<Void> param) {

      FlinkWriteConf flinkWriteConf =
          new FlinkWriteConf(
              table, ImmutableMap.of(), new org.apache.flink.configuration.Configuration());

      IcebergPartitionTimeCommitter committer =
          new IcebergPartitionTimeCommitter(
              new TestTableLoader(tablePath),
              false,
              Collections.singletonMap(
                  "flink.test", TestIcebergPartitionTimeCommitter.class.getName()),
              ThreadPools.WORKER_THREAD_POOL_SIZE,
              SnapshotRef.MAIN_BRANCH,
              flinkWriteConf.partitionCommitDelay(),
              flinkWriteConf.partitionCommitWatermarkTimeZone(),
              flinkWriteConf.partitionTimeExtractorTimestampPattern(),
              flinkWriteConf.partitionTimeExtractorTimestampFormatter(),
              flinkWriteConf.partitionCommitPolicyKind(),
              flinkWriteConf.partitionCommitPolicyClass(),
              flinkWriteConf.partitionCommitSuccessFileName());
      committer.setup(param.getContainingTask(), param.getStreamConfig(), param.getOutput());
      return (T) committer;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return IcebergPartitionTimeCommitter.class;
    }
  }
}
