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
package org.apache.iceberg.flink.maintenance.operator;

import static org.apache.iceberg.actions.RewriteDataFiles.TARGET_FILE_SIZE_BYTES;
import static org.apache.iceberg.actions.SizeBasedFileRewritePlanner.MIN_INPUT_FILES;
import static org.apache.iceberg.flink.maintenance.operator.RewriteUtil.executeRewrite;
import static org.apache.iceberg.flink.maintenance.operator.RewriteUtil.planDataFileRewrite;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.maintenance.api.Trigger;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestDataFileRewriteRunner extends OperatorTestBase {
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testExecute(boolean partitioned) throws Exception {
    Table table;
    PartitionData partition;
    if (partitioned) {
      table = createPartitionedTable();
      partition = new PartitionData(table.spec().partitionType());
      partition.set(0, "p1");
      insertPartitioned(table, 1, "p1");
      insertPartitioned(table, 2, "p1");
      insertPartitioned(table, 3, "p1");
    } else {
      table = createTable();
      partition = new PartitionData(PartitionSpec.unpartitioned().partitionType());
      insert(table, 1, "p1");
      insert(table, 2, "p1");
      insert(table, 3, "p1");
    }

    List<DataFileRewritePlanner.PlannedGroup> planned = planDataFileRewrite(tableLoader());
    assertThat(planned).hasSize(1);
    List<DataFileRewriteRunner.ExecutedGroup> actual = executeRewrite(planned);
    assertThat(actual).hasSize(1);

    assertRewriteFileGroup(
        actual.get(0),
        table,
        records(
            table.schema(),
            ImmutableSet.of(
                ImmutableList.of(1, "p1"), ImmutableList.of(2, "p1"), ImmutableList.of(3, "p1"))),
        1,
        ImmutableSet.of(partition));
  }

  @Test
  void testPartitionSpecChange() throws Exception {
    Table table = createPartitionedTable();
    insertPartitioned(table, 1, "p1");
    insertPartitioned(table, 2, "p1");
    PartitionData oldPartition = new PartitionData(table.spec().partitionType());
    oldPartition.set(0, "p1");

    try (OneInputStreamOperatorTestHarness<
            DataFileRewritePlanner.PlannedGroup, DataFileRewriteRunner.ExecutedGroup>
        testHarness =
            ProcessFunctionTestHarnesses.forProcessFunction(
                new DataFileRewriteRunner(
                    OperatorTestBase.DUMMY_TABLE_NAME, OperatorTestBase.DUMMY_TABLE_NAME, 0))) {
      testHarness.open();

      List<DataFileRewritePlanner.PlannedGroup> planned = planDataFileRewrite(tableLoader());
      assertThat(planned).hasSize(1);

      testHarness.processElement(planned.get(0), System.currentTimeMillis());
      List<DataFileRewriteRunner.ExecutedGroup> actual = testHarness.extractOutputValues();
      assertThat(actual).hasSize(1);
      assertRewriteFileGroup(
          actual.get(0),
          table,
          records(
              table.schema(),
              ImmutableSet.of(ImmutableList.of(1, "p1"), ImmutableList.of(2, "p1"))),
          1,
          ImmutableSet.of(oldPartition));

      insertPartitioned(table, 3, "p1");

      planned = planDataFileRewrite(tableLoader());
      assertThat(planned).hasSize(1);

      testHarness.processElement(planned.get(0), System.currentTimeMillis());
      actual = testHarness.extractOutputValues();
      assertThat(actual).hasSize(2);
      assertRewriteFileGroup(
          actual.get(1),
          table,
          records(
              table.schema(),
              ImmutableSet.of(
                  ImmutableList.of(1, "p1"), ImmutableList.of(2, "p1"), ImmutableList.of(3, "p1"))),
          1,
          ImmutableSet.of(oldPartition));

      // Alter the table schema
      table.updateSpec().addField("id").commit();
      // Insert some now data
      insertFullPartitioned(table, 4, "p1");
      insertFullPartitioned(table, 4, "p1");
      PartitionData newPartition = new PartitionData(table.spec().partitionType());
      newPartition.set(0, "p1");
      newPartition.set(1, 4);
      table.refresh();

      planned = planDataFileRewrite(tableLoader());
      assertThat(planned).hasSize(2);
      DataFileRewritePlanner.PlannedGroup oldCompact = planned.get(0);
      DataFileRewritePlanner.PlannedGroup newCompact = planned.get(1);
      if (oldCompact.group().inputFileNum() == 2) {
        newCompact = planned.get(0);
        oldCompact = planned.get(1);
      }

      testHarness.processElement(newCompact, System.currentTimeMillis());
      actual = testHarness.extractOutputValues();
      assertThat(actual).hasSize(3);
      assertRewriteFileGroup(
          actual.get(2),
          table,
          records(
              table.schema(),
              ImmutableList.of(ImmutableList.of(4, "p1"), ImmutableList.of(4, "p1"))),
          1,
          ImmutableSet.of(newPartition));

      testHarness.processElement(oldCompact, System.currentTimeMillis());
      actual = testHarness.extractOutputValues();
      assertThat(actual).hasSize(4);
      PartitionData[] transformedPartitions = {
        newPartition.copy(), newPartition.copy(), newPartition.copy()
      };
      transformedPartitions[0].set(1, 1);
      transformedPartitions[1].set(1, 2);
      transformedPartitions[2].set(1, 3);
      assertRewriteFileGroup(
          actual.get(3),
          table,
          records(
              table.schema(),
              ImmutableSet.of(
                  ImmutableList.of(1, "p1"), ImmutableList.of(2, "p1"), ImmutableList.of(3, "p1"))),
          3,
          Sets.newHashSet(transformedPartitions));
    }
  }

  @Test
  void testError() throws Exception {
    Table table = createTable();
    insert(table, 1, "a");
    insert(table, 2, "b");

    try (OneInputStreamOperatorTestHarness<
            DataFileRewritePlanner.PlannedGroup, DataFileRewriteRunner.ExecutedGroup>
        testHarness =
            ProcessFunctionTestHarnesses.forProcessFunction(
                new DataFileRewriteRunner(
                    OperatorTestBase.DUMMY_TABLE_NAME, OperatorTestBase.DUMMY_TABLE_NAME, 0))) {
      testHarness.open();

      List<DataFileRewritePlanner.PlannedGroup> planned = planDataFileRewrite(tableLoader());
      assertThat(planned).hasSize(1);
      // Cause an exception
      dropTable();

      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
      testHarness.processElement(planned.get(0), System.currentTimeMillis());
      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).hasSize(1);
      assertThat(
              testHarness
                  .getSideOutput(TaskResultAggregator.ERROR_STREAM)
                  .poll()
                  .getValue()
                  .getMessage())
          .contains("File does not exist: ");
    }
  }

  @Test
  void testV2Table() throws Exception {
    Table table = createTableWithDelete();
    update(table, 1, null, "a", "b");
    update(table, 1, "b", "c");

    List<DataFileRewritePlanner.PlannedGroup> planned = planDataFileRewrite(tableLoader());
    assertThat(planned).hasSize(1);

    List<DataFileRewriteRunner.ExecutedGroup> actual = executeRewrite(planned);
    assertThat(actual).hasSize(1);

    assertRewriteFileGroup(
        actual.get(0),
        table,
        records(table.schema(), ImmutableSet.of(ImmutableList.of(1, "c"))),
        1,
        ImmutableSet.of(new PartitionData(PartitionSpec.unpartitioned().partitionType())));
  }

  @Test
  void testSplitSize() throws Exception {
    Table table = createTable();

    File dataDir = new File(new Path(table.location(), "data").toUri().getPath());
    dataDir.mkdir();
    GenericAppenderHelper dataAppender =
        new GenericAppenderHelper(table, FileFormat.PARQUET, dataDir.toPath());
    List<Record> expected = Lists.newArrayListWithExpectedSize(4000);
    for (int i = 0; i < 4; ++i) {
      List<Record> batch = RandomGenericData.generate(table.schema(), 1000, 10 + i);
      dataAppender.appendToTable(batch);
      expected.addAll(batch);
    }

    // First run with high target file size
    List<DataFileRewritePlanner.PlannedGroup> planWithNoTargetFileSize =
        planDataFileRewrite(tableLoader());
    assertThat(planWithNoTargetFileSize).hasSize(1);

    // Second run with low target file size
    long targetFileSize =
        planWithNoTargetFileSize.get(0).group().fileScanTasks().get(0).sizeBytes()
            + planWithNoTargetFileSize.get(0).group().fileScanTasks().get(1).sizeBytes();
    List<DataFileRewritePlanner.PlannedGroup> planned;
    try (OneInputStreamOperatorTestHarness<Trigger, DataFileRewritePlanner.PlannedGroup>
        testHarness =
            ProcessFunctionTestHarnesses.forProcessFunction(
                new DataFileRewritePlanner(
                    OperatorTestBase.DUMMY_TABLE_NAME,
                    OperatorTestBase.DUMMY_TABLE_NAME,
                    0,
                    tableLoader(),
                    11,
                    10_000_000,
                    ImmutableMap.of(
                        MIN_INPUT_FILES,
                        "2",
                        TARGET_FILE_SIZE_BYTES,
                        String.valueOf(targetFileSize)),
                    Expressions.alwaysTrue()))) {
      testHarness.open();

      OperatorTestBase.trigger(testHarness);

      assertThat(testHarness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNull();
      planned = testHarness.extractOutputValues();
      assertThat(planned).hasSize(1);
    }

    List<DataFileRewriteRunner.ExecutedGroup> actual = executeRewrite(planned);
    assertThat(actual).hasSize(1);

    assertRewriteFileGroup(
        actual.get(0),
        table,
        expected,
        2,
        ImmutableSet.of(new PartitionData(PartitionSpec.unpartitioned().partitionType())));
  }

  void assertRewriteFileGroup(
      DataFileRewriteRunner.ExecutedGroup actual,
      Table table,
      Collection<Record> expectedRecords,
      int expectedFileNum,
      Set<StructLike> expectedPartitions)
      throws IOException {
    assertThat(actual.snapshotId()).isEqualTo(table.currentSnapshot().snapshotId());
    assertThat(actual.groupsPerCommit()).isEqualTo(1);
    assertThat(actual.group().addedFiles()).hasSize(expectedFileNum);
    Collection<Record> writtenRecords = Lists.newArrayListWithExpectedSize(expectedRecords.size());
    Set<StructLike> writtenPartitions = Sets.newHashSetWithExpectedSize(expectedPartitions.size());
    for (DataFile newDataFile : actual.group().addedFiles()) {
      assertThat(newDataFile.format()).isEqualTo(FileFormat.PARQUET);
      assertThat(newDataFile.content()).isEqualTo(FileContent.DATA);
      assertThat(newDataFile.keyMetadata()).isNull();
      writtenPartitions.add(newDataFile.partition());

      try (CloseableIterable<Record> reader =
          Parquet.read(table.io().newInputFile(newDataFile.location()))
              .project(table.schema())
              .createReaderFunc(
                  fileSchema -> GenericParquetReaders.buildReader(table.schema(), fileSchema))
              .build()) {
        List<Record> newRecords = Lists.newArrayList(reader);
        assertThat(newRecords).hasSize((int) newDataFile.recordCount());
        writtenRecords.addAll(newRecords);
      }
    }

    assertThat(writtenRecords).containsExactlyInAnyOrderElementsOf(expectedRecords);
    assertThat(writtenPartitions).isEqualTo(expectedPartitions);
  }

  private List<Record> records(Schema schema, Collection<List<Object>> data) {
    GenericRecord record = GenericRecord.create(schema);

    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    data.forEach(
        recordData ->
            builder.add(
                record.copy(ImmutableMap.of("id", recordData.get(0), "data", recordData.get(1)))));

    return builder.build();
  }
}
