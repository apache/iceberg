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

import static org.apache.iceberg.flink.sink.ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION;
import static org.apache.iceberg.flink.sink.SinkTestUtil.extractAndAssertCommittableWithLineage;
import static org.apache.iceberg.flink.sink.SinkTestUtil.transformsToStreamElement;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestIcebergWriteAggregatorCommitGate {

  @TempDir private File temporaryFolder;
  @TempDir private File flinkManifestFolder;

  private Table table;
  private TableLoader tableLoader;

  private final DataFile dataFile =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data.parquet")
          .withFileSizeInBytes(100)
          .withRecordCount(5)
          .build();

  @BeforeEach
  void before() {
    String tablePath = temporaryFolder.getAbsolutePath() + "/test";
    assertThat(new File(tablePath).mkdir()).isTrue();
    table =
        SimpleDataUtil.createTable(
            tablePath,
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION,
                "2",
                FLINK_MANIFEST_LOCATION,
                flinkManifestFolder.getAbsolutePath()),
            false);
    tableLoader = TableLoader.fromHadoopTable(tablePath);
  }

  private OneInputStreamOperatorTestHarness<
          CommittableMessage<WriteResult>, CommittableMessage<IcebergCommittable>>
      createHarness(IcebergWriteAggregator aggregator) throws Exception {
    OneInputStreamOperatorTestHarness<
            CommittableMessage<WriteResult>, CommittableMessage<IcebergCommittable>>
        harness = new OneInputStreamOperatorTestHarness<>(aggregator, 1, 1, 0);
    harness.setup();
    return harness;
  }

  private void processWriteResult(
      OneInputStreamOperatorTestHarness<
              CommittableMessage<WriteResult>, CommittableMessage<IcebergCommittable>>
          harness,
      long checkpointId)
      throws Exception {
    WriteResult wr = WriteResult.builder().addDataFiles(dataFile).build();
    harness.processElement(new StreamRecord<>(new CommittableWithLineage<>(wr, checkpointId, 0)));
  }

  private static void assertCommittableCheckpointId(StreamElement element, long expectedId) {
    CommittableWithLineage<IcebergCommittable> committable =
        extractAndAssertCommittableWithLineage(element);
    assertThat(committable.getCommittable().checkpointId()).isEqualTo(expectedId);
  }

  @Test
  void testNoGateEmitsNormally() throws Exception {
    IcebergWriteAggregator aggregator = new IcebergWriteAggregator(tableLoader, null);
    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<WriteResult>, CommittableMessage<IcebergCommittable>>
        harness = createHarness(aggregator)) {
      harness.open();

      processWriteResult(harness, 1L);
      aggregator.prepareSnapshotPreBarrier(1L);

      List<StreamElement> output = transformsToStreamElement(harness.getOutput());
      assertThat(output).hasSize(2);
      assertCommittableCheckpointId(output.get(1), 1L);
    }
  }

  @Test
  void testGateClosedBuffersCommittable() throws Exception {
    IcebergWriteAggregator aggregator =
        new IcebergWriteAggregator(tableLoader, checkpointId -> false);
    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<WriteResult>, CommittableMessage<IcebergCommittable>>
        harness = createHarness(aggregator)) {
      harness.open();

      processWriteResult(harness, 1L);
      aggregator.prepareSnapshotPreBarrier(1L);

      assertThat(harness.getOutput()).isEmpty();
    }
  }

  @Test
  void testGateClosedThenOpenedFlushesBuffered() throws Exception {
    AtomicBoolean allowed = new AtomicBoolean(false);
    IcebergWriteAggregator aggregator =
        new IcebergWriteAggregator(tableLoader, checkpointId -> allowed.get());
    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<WriteResult>, CommittableMessage<IcebergCommittable>>
        harness = createHarness(aggregator)) {
      harness.open();

      processWriteResult(harness, 1L);
      aggregator.prepareSnapshotPreBarrier(1L);
      harness.snapshot(1L, 1L);
      assertThat(harness.getOutput()).isEmpty();

      processWriteResult(harness, 2L);
      aggregator.prepareSnapshotPreBarrier(2L);
      harness.snapshot(2L, 2L);
      assertThat(harness.getOutput()).isEmpty();

      allowed.set(true);
      processWriteResult(harness, 3L);
      aggregator.prepareSnapshotPreBarrier(3L);

      List<StreamElement> output = transformsToStreamElement(harness.getOutput());
      assertThat(output).hasSize(6);
      assertCommittableCheckpointId(output.get(1), 1L);
      assertCommittableCheckpointId(output.get(3), 2L);
      assertCommittableCheckpointId(output.get(5), 3L);
    }
  }

  @Test
  void testFinishBypassesGate() throws Exception {
    IcebergWriteAggregator aggregator =
        new IcebergWriteAggregator(tableLoader, checkpointId -> false);
    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<WriteResult>, CommittableMessage<IcebergCommittable>>
        harness = createHarness(aggregator)) {
      harness.open();

      processWriteResult(harness, 1L);
      aggregator.prepareSnapshotPreBarrier(1L);
      harness.snapshot(1L, 1L);
      assertThat(harness.getOutput()).isEmpty();

      processWriteResult(harness, 2L);
      aggregator.finish();

      List<StreamElement> output = transformsToStreamElement(harness.getOutput());
      assertThat(output).hasSize(4);
      assertCommittableCheckpointId(output.get(1), 1L);
      assertCommittableCheckpointId(output.get(3), 2L);
    }
  }

  @Test
  void testStateRestoreWithBufferedCommittables() throws Exception {
    OperatorSubtaskState snapshot;

    IcebergWriteAggregator aggregator1 =
        new IcebergWriteAggregator(tableLoader, checkpointId -> false);
    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<WriteResult>, CommittableMessage<IcebergCommittable>>
        harness = createHarness(aggregator1)) {
      harness.open();

      processWriteResult(harness, 1L);
      aggregator1.prepareSnapshotPreBarrier(1L);
      snapshot = harness.snapshot(1L, 1L);
      assertThat(harness.getOutput()).isEmpty();
    }

    IcebergWriteAggregator aggregator2 =
        new IcebergWriteAggregator(tableLoader, checkpointId -> true);
    try (OneInputStreamOperatorTestHarness<
            CommittableMessage<WriteResult>, CommittableMessage<IcebergCommittable>>
        harness = createHarness(aggregator2)) {
      harness.initializeState(snapshot);
      harness.open();

      processWriteResult(harness, 2L);
      aggregator2.prepareSnapshotPreBarrier(2L);

      List<StreamElement> output = transformsToStreamElement(harness.getOutput());
      assertThat(output).hasSize(4);
      assertCommittableCheckpointId(output.get(1), 1L);
      assertCommittableCheckpointId(output.get(3), 2L);
    }
  }
}
