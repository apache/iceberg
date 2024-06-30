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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.iceberg.BaseScanTaskGroup;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.Mockito;

public class TestSparkPlanningUtil extends TestBaseWithCatalog {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()),
          required(3, "category", Types.StringType.get()));
  private static final PartitionSpec SPEC_1 =
      PartitionSpec.builderFor(SCHEMA).withSpecId(1).bucket("id", 16).identity("data").build();
  private static final PartitionSpec SPEC_2 =
      PartitionSpec.builderFor(SCHEMA).withSpecId(2).identity("data").build();
  private static final List<String> EXECUTOR_LOCATIONS =
      ImmutableList.of("host1_exec1", "host1_exec2", "host1_exec3", "host2_exec1", "host2_exec2");

  @TestTemplate
  public void testFileScanTaskWithoutDeletes() {
    List<ScanTask> tasks =
        ImmutableList.of(
            new MockFileScanTask(mockDataFile(Row.of(1, "a")), SCHEMA, SPEC_1),
            new MockFileScanTask(mockDataFile(Row.of(2, "b")), SCHEMA, SPEC_1),
            new MockFileScanTask(mockDataFile(Row.of(3, "c")), SCHEMA, SPEC_1));
    ScanTaskGroup<ScanTask> taskGroup = new BaseScanTaskGroup<>(tasks);
    List<ScanTaskGroup<ScanTask>> taskGroups = ImmutableList.of(taskGroup);

    String[][] locations = SparkPlanningUtil.assignExecutors(taskGroups, EXECUTOR_LOCATIONS);

    // should not assign executors if there are no deletes
    assertThat(locations.length).isEqualTo(1);
    assertThat(locations[0]).isEmpty();
  }

  @TestTemplate
  public void testFileScanTaskWithDeletes() {
    StructLike partition1 = Row.of("k2", null);
    StructLike partition2 = Row.of("k1");
    List<ScanTask> tasks =
        ImmutableList.of(
            new MockFileScanTask(
                mockDataFile(partition1), mockDeleteFiles(1, partition1), SCHEMA, SPEC_1),
            new MockFileScanTask(
                mockDataFile(partition2), mockDeleteFiles(3, partition2), SCHEMA, SPEC_2),
            new MockFileScanTask(
                mockDataFile(partition1), mockDeleteFiles(2, partition1), SCHEMA, SPEC_1));
    ScanTaskGroup<ScanTask> taskGroup = new BaseScanTaskGroup<>(tasks);
    List<ScanTaskGroup<ScanTask>> taskGroups = ImmutableList.of(taskGroup);

    String[][] locations = SparkPlanningUtil.assignExecutors(taskGroups, EXECUTOR_LOCATIONS);

    // should assign executors and handle different size of partitions
    assertThat(locations.length).isEqualTo(1);
    assertThat(locations[0].length).isGreaterThanOrEqualTo(1);
  }

  @TestTemplate
  public void testFileScanTaskWithUnpartitionedDeletes() {
    List<ScanTask> tasks1 =
        ImmutableList.of(
            new MockFileScanTask(
                mockDataFile(Row.of()),
                mockDeleteFiles(2, Row.of()),
                SCHEMA,
                PartitionSpec.unpartitioned()),
            new MockFileScanTask(
                mockDataFile(Row.of()),
                mockDeleteFiles(2, Row.of()),
                SCHEMA,
                PartitionSpec.unpartitioned()),
            new MockFileScanTask(
                mockDataFile(Row.of()),
                mockDeleteFiles(2, Row.of()),
                SCHEMA,
                PartitionSpec.unpartitioned()));
    ScanTaskGroup<ScanTask> taskGroup1 = new BaseScanTaskGroup<>(tasks1);
    List<ScanTask> tasks2 =
        ImmutableList.of(
            new MockFileScanTask(
                mockDataFile(null),
                mockDeleteFiles(2, null),
                SCHEMA,
                PartitionSpec.unpartitioned()),
            new MockFileScanTask(
                mockDataFile(null),
                mockDeleteFiles(2, null),
                SCHEMA,
                PartitionSpec.unpartitioned()),
            new MockFileScanTask(
                mockDataFile(null),
                mockDeleteFiles(2, null),
                SCHEMA,
                PartitionSpec.unpartitioned()));
    ScanTaskGroup<ScanTask> taskGroup2 = new BaseScanTaskGroup<>(tasks2);
    List<ScanTaskGroup<ScanTask>> taskGroups = ImmutableList.of(taskGroup1, taskGroup2);

    String[][] locations = SparkPlanningUtil.assignExecutors(taskGroups, EXECUTOR_LOCATIONS);

    // should not assign executors if the table is unpartitioned
    assertThat(locations.length).isEqualTo(2);
    assertThat(locations[0]).isEmpty();
    assertThat(locations[1]).isEmpty();
  }

  @TestTemplate
  public void testDataTasks() {
    List<ScanTask> tasks =
        ImmutableList.of(
            new MockDataTask(mockDataFile(Row.of(1, "a"))),
            new MockDataTask(mockDataFile(Row.of(2, "b"))),
            new MockDataTask(mockDataFile(Row.of(3, "c"))));
    ScanTaskGroup<ScanTask> taskGroup = new BaseScanTaskGroup<>(tasks);
    List<ScanTaskGroup<ScanTask>> taskGroups = ImmutableList.of(taskGroup);

    String[][] locations = SparkPlanningUtil.assignExecutors(taskGroups, EXECUTOR_LOCATIONS);

    // should not assign executors for data tasks
    assertThat(locations.length).isEqualTo(1);
    assertThat(locations[0]).isEmpty();
  }

  @TestTemplate
  public void testUnknownTasks() {
    List<ScanTask> tasks = ImmutableList.of(new UnknownScanTask(), new UnknownScanTask());
    ScanTaskGroup<ScanTask> taskGroup = new BaseScanTaskGroup<>(tasks);
    List<ScanTaskGroup<ScanTask>> taskGroups = ImmutableList.of(taskGroup);

    String[][] locations = SparkPlanningUtil.assignExecutors(taskGroups, EXECUTOR_LOCATIONS);

    // should not assign executors for unknown tasks
    assertThat(locations.length).isEqualTo(1);
    assertThat(locations[0]).isEmpty();
  }

  private static DataFile mockDataFile(StructLike partition) {
    DataFile file = Mockito.mock(DataFile.class);
    when(file.partition()).thenReturn(partition);
    return file;
  }

  private static DeleteFile[] mockDeleteFiles(int count, StructLike partition) {
    DeleteFile[] files = new DeleteFile[count];
    for (int index = 0; index < count; index++) {
      files[index] = mockDeleteFile(partition);
    }
    return files;
  }

  private static DeleteFile mockDeleteFile(StructLike partition) {
    DeleteFile file = Mockito.mock(DeleteFile.class);
    when(file.partition()).thenReturn(partition);
    return file;
  }

  private static class MockDataTask extends MockFileScanTask implements DataTask {

    MockDataTask(DataFile file) {
      super(file);
    }

    @Override
    public PartitionSpec spec() {
      return PartitionSpec.unpartitioned();
    }

    @Override
    public CloseableIterable<StructLike> rows() {
      throw new UnsupportedOperationException();
    }
  }

  private static class UnknownScanTask implements ScanTask {}
}
