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

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.FlinkSplitPlanner;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitState;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitStatus;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;
import org.apache.iceberg.util.ThreadPools;
import org.junit.Assert;
import org.junit.Test;

// TODO -- in long term, we should consolidate some of the Partitioned Split Generation
// to SplitHelpers like done in the other enumeration tests
public class TestPartitionAwareSplitAssigner extends SplitAssignerTestBase {

  // Simple schema for testing: id, data
  private static final Schema TEST_SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));

  // Partition spec with bucket partitioning on id (4 buckets)
  private static final PartitionSpec TEST_PARTITION_SPEC =
      PartitionSpec.builderFor(TEST_SCHEMA).bucket("id", 4).build();

  @Override
  protected SplitAssigner splitAssigner() {
    return new PartitionAwareSplitAssigner();
  }

  /** Test 4 buckets assigned to 4 subtasks - each subtask gets exactly one bucket. */
  @Test
  public void testFourBucketsToFourSubtasks() throws Exception {
    SplitAssigner assigner = splitAssigner();
    assigner.onDiscoveredSplits(createBucketPartitionedSplits());

    Map<StructLike, Integer> assignments = assignSplitsAndTrackPartitions(assigner, 4);

    // Verify each subtask got exactly one partition (4 buckets to 4 subtasks)
    Assert.assertEquals("Should have 4 partitions", 4, assignments.size());
    Assert.assertEquals(
        "Each subtask should get one partition", 4, Sets.newHashSet(assignments.values()).size());
    Assert.assertEquals("All splits should be consumed", 0, assigner.pendingSplitCount());
  }

  /** Test 4 buckets assigned to 2 subtasks - each subtask gets multiple buckets. */
  @Test
  public void testFourBucketsToTwoSubtasks() throws Exception {
    SplitAssigner assigner = splitAssigner();
    assigner.onDiscoveredSplits(createBucketPartitionedSplits());

    Map<StructLike, Integer> assignments = assignSplitsAndTrackPartitions(assigner, 2);

    // Verify both subtasks got partitions (4 buckets distributed to 2 subtasks)
    Assert.assertEquals("Should have 4 partitions", 4, assignments.size());
    Assert.assertEquals(
        "Both subtasks should get partitions", 2, Sets.newHashSet(assignments.values()).size());
    Assert.assertEquals("All splits should be consumed", 0, assigner.pendingSplitCount());
  }

  /** Test 4 buckets assigned to 6 subtasks - 2 subtasks won't have data. */
  @Test
  public void testFourBucketsToSixSubtasks() throws Exception {
    SplitAssigner assigner = splitAssigner();
    assigner.onDiscoveredSplits(createBucketPartitionedSplits());

    Map<StructLike, Integer> assignments = assignSplitsAndTrackPartitions(assigner, 6);

    // Verify exactly 4 subtasks got partitions (only 4 buckets available) and 2 got none
    Assert.assertEquals("Should have 4 partitions", 4, assignments.size());
    Assert.assertEquals(
        "Only 4 subtasks should get partitions", 4, Sets.newHashSet(assignments.values()).size());
    Assert.assertEquals("All splits should be consumed", 0, assigner.pendingSplitCount());
  }

  /** Test mixed dt (identity) and bucket partitioning with 3 files assigned to 2 tasks. */
  @Test
  public void testMixedPartitioningThreeFilesToTwoTasks() throws Exception {
    SplitAssigner assigner = splitAssigner();
    assigner.onDiscoveredSplits(createMixedPartitionedSplits());

    Map<StructLike, Integer> assignments = assignSplitsAndTrackPartitions(assigner, 2);

    // Verify partitions distributed to both subtasks (3 mixed partitions to 2 subtasks)
    Assert.assertEquals("Should have 3 partitions", 3, assignments.size());
    Assert.assertEquals(
        "Both subtasks should get partitions", 2, Sets.newHashSet(assignments.values()).size());
    Assert.assertEquals("All splits should be consumed", 0, assigner.pendingSplitCount());
  }

  @Test
  public void testStateFailedTask() throws Exception {
    // 1. Create assigner and add partitioned splits
    SplitAssigner assigner = splitAssigner();
    List<IcebergSourceSplit> splits = createBucketPartitionedSplits(); // 4 bucket splits
    assigner.onDiscoveredSplits(splits);

    // 2. Take a snapshot of the state (before any splits are assigned)
    Collection<IcebergSourceSplitState> state = assigner.state();
    Assert.assertEquals("State should contain all splits", splits.size(), state.size());

    // Verify all splits are in UNASSIGNED status
    for (IcebergSourceSplitState splitState : state) {
      Assert.assertEquals(
          "Splits should be unassigned in state",
          IcebergSourceSplitStatus.UNASSIGNED,
          splitState.status());
      Assert.assertNotNull("Split should not be null in state", splitState.split());
    }

    // 3. Create a new assigner from the saved state (simulating task restart)
    SplitAssigner restored = new PartitionAwareSplitAssigner(state);

    // 4. Verify the restored assigner has the same state
    Assert.assertEquals(
        "Restored assigner should have same split count",
        splits.size(),
        restored.pendingSplitCount());
    Collection<IcebergSourceSplitState> restoredState = restored.state();
    Assert.assertEquals("Restored state should have same size", state.size(), restoredState.size());

    // 5. Test that partition-aware assignment works correctly after restoration
    Map<StructLike, Integer> restoredAssignments = assignSplitsAndTrackPartitions(restored, 4);

    // 6. Verify all splits were retrieved and partition affinity is maintained
    Assert.assertEquals(
        "All partitions should be assignable after restore",
        splits.size(),
        restoredAssignments.size());
    Assert.assertEquals(
        "Each subtask should get exactly one partition after restore",
        restoredAssignments.size(),
        Sets.newHashSet(restoredAssignments.values()).size());
    Assert.assertEquals(
        "No splits should remain after assignment", 0, restored.pendingSplitCount());
  }

  @Test
  public void testConsistentAssignmentPartitionAcrossTables() throws Exception {
    int[] partitionCounts = {2, 4, 6, 8};
    int totalTasks = 4;

    for (int partitionCount : partitionCounts) {
      testConsistentAssignmentWithPartitionCount(partitionCount, totalTasks);
    }
  }

  private void testConsistentAssignmentWithPartitionCount(int partitionCount, int totalTasks)
      throws Exception {
    // Create two tables with identical partitioning but different data
    List<IcebergSourceSplit> table1Splits =
        createIdentityPartitionedSplits("table1", partitionCount);
    List<IcebergSourceSplit> table2Splits =
        createIdentityPartitionedSplits("table2", partitionCount);

    // Create separate assigners for each table
    SplitAssigner assigner1 = new PartitionAwareSplitAssigner();
    SplitAssigner assigner2 = new PartitionAwareSplitAssigner();

    assigner1.onDiscoveredSplits(table1Splits);
    assigner2.onDiscoveredSplits(table2Splits);

    // Assign splits and track partition assignments for both tables
    Map<StructLike, Integer> table1Assignments =
        assignSplitsAndTrackPartitions(assigner1, totalTasks);
    Map<StructLike, Integer> table2Assignments =
        assignSplitsAndTrackPartitions(assigner2, totalTasks);

    // Assert consistent assignment across tables
    Assert.assertEquals(
        "Both tables should have same number of partitions assigned",
        table1Assignments.size(),
        table2Assignments.size());

    for (StructLike partition : table1Assignments.keySet()) {
      Assert.assertTrue(
          "Table2 should have assignment for partition: " + partition,
          table2Assignments.containsKey(partition));
      Assert.assertEquals(
          "Partition " + partition + " should be assigned to same task in both tables",
          table1Assignments.get(partition),
          table2Assignments.get(partition));
    }

    // Verify all splits were assigned
    Assert.assertEquals("All table1 splits should be consumed", 0, assigner1.pendingSplitCount());
    Assert.assertEquals("All table2 splits should be consumed", 0, assigner2.pendingSplitCount());
  }

  /** Assigns splits using round-robin Returns a map of partition -> subtask assignments. */
  private Map<StructLike, Integer> assignSplitsAndTrackPartitions(
      SplitAssigner assigner, int totalTasks) {
    Map<StructLike, Integer> assignments = Maps.newHashMap();
    int currentSubtask = 0;
    while (assigner.pendingSplitCount() > 0) {
      GetSplitResult result = assigner.getNext(null, currentSubtask, totalTasks);
      if (result.status() == GetSplitResult.Status.AVAILABLE) {
        StructLike partitionKey = extractPartitionKey(result.split());
        // same partition must always go to same subtask
        Integer existingSubtask = assignments.put(partitionKey, currentSubtask);
        if (existingSubtask != null) {
          Assert.assertEquals(
              "Partition " + partitionKey + " must consistently go to same subtask",
              existingSubtask,
              Integer.valueOf(currentSubtask));
        }
      }
      currentSubtask = (currentSubtask + 1) % totalTasks;
    }

    return assignments;
  }

  // Same logic as seen in PartitionAwareSplitAssigner
  private StructLike extractPartitionKey(IcebergSourceSplit split) {
    // Reuse the same approach as PartitionAwareSplitAssigner for consistent partition key
    // extraction
    FileScanTask firstTask = split.task().files().iterator().next();
    PartitionSpec spec = firstTask.spec();
    StructLike partition = firstTask.partition();

    // Compute consistent grouping key type for this split's partition specs
    Set<PartitionSpec> specs =
        split.task().files().stream().map(FileScanTask::spec).collect(Collectors.toSet());
    Types.StructType groupingKeyType = Partitioning.groupingKeyType(null, specs);

    // Create projection to convert partition to consistent grouping key
    StructProjection projection = StructProjection.create(spec.partitionType(), groupingKeyType);
    PartitionData groupingKeyTemplate = new PartitionData(groupingKeyType);
    return groupingKeyTemplate.copyFor(projection.wrap(partition));
  }

  @Override
  @Test
  public void testEmptyInitialization() {
    SplitAssigner assigner = splitAssigner();
    // Partition-aware assigner requires subtaskId and registeredTasks
    GetSplitResult result = assigner.getNext(null, 0, 1);
    Assert.assertEquals(GetSplitResult.Status.UNAVAILABLE, result.status());
  }

  @Override
  @Test
  // Right now PartitionAwareSplitAssigner is only designed for Batch Execution mode
  public void testContinuousEnumeratorSequence() {}

  @Override
  @Test
  public void testStaticEnumeratorSequence() throws Exception {
    SplitAssigner assigner = splitAssigner();
    List<IcebergSourceSplit> splits = createBucketPartitionedSplits();
    assigner.onDiscoveredSplits(splits);

    // We override this function from SplitAssignerTestBase, so no need to overcomplicated, all
    // splits
    // are assigned to the same taskId
    int registeredTasks = 1;

    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE, 0, registeredTasks);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE, 0, registeredTasks);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE, 0, registeredTasks);
    assertSnapshot(assigner, 1);
    assigner.onUnassignedSplits(createSplits(1, 1, "1"));
    assertSnapshot(assigner, 2);

    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE, 0, registeredTasks);
    assertGetNext(assigner, GetSplitResult.Status.AVAILABLE, 0, registeredTasks);
    assertGetNext(assigner, GetSplitResult.Status.UNAVAILABLE, 0, registeredTasks);
    assertSnapshot(assigner, 0);
  }

  /**
   * Create splits with bucket partitioning - one file per bucket (4 buckets total). TODO: Move this
   * logic to SplitHelpers once we have a robust implementation.
   */
  private List<IcebergSourceSplit> createBucketPartitionedSplits() throws Exception {
    final File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    final String warehouse = "file:" + warehouseFile;
    Configuration hadoopConf = new Configuration();
    final HadoopCatalog catalog = new HadoopCatalog(hadoopConf, warehouse);

    ImmutableMap<String, String> properties = ImmutableMap.of(TableProperties.FORMAT_VERSION, "1");

    try {
      final Table table =
          catalog.createTable(
              TestFixtures.TABLE_IDENTIFIER, TEST_SCHEMA, TEST_PARTITION_SPEC, null, properties);
      final GenericAppenderHelper dataAppender =
          new GenericAppenderHelper(table, FileFormat.PARQUET, TEMPORARY_FOLDER);

      // Create records for each bucket (0, 1, 2, 3)
      // bucket(id, 4) = id % 4, so we use ids: 100, 101, 102, 103
      List<Record> bucket0Records = RandomGenericData.generate(TEST_SCHEMA, 2, 0L);
      bucket0Records.get(0).setField("id", 100L); // bucket(100, 4) = 0
      bucket0Records.get(1).setField("id", 104L); // bucket(104, 4) = 0

      List<Record> bucket1Records = RandomGenericData.generate(TEST_SCHEMA, 2, 1L);
      bucket1Records.get(0).setField("id", 101L); // bucket(101, 4) = 1
      bucket1Records.get(1).setField("id", 105L); // bucket(105, 4) = 1

      List<Record> bucket2Records = RandomGenericData.generate(TEST_SCHEMA, 2, 2L);
      bucket2Records.get(0).setField("id", 102L); // bucket(102, 4) = 2
      bucket2Records.get(1).setField("id", 106L); // bucket(106, 4) = 2

      List<Record> bucket3Records = RandomGenericData.generate(TEST_SCHEMA, 2, 3L);
      bucket3Records.get(0).setField("id", 103L); // bucket(103, 4) = 3
      bucket3Records.get(1).setField("id", 107L); // bucket(107, 4) = 3

      // Write files with explicit bucket values
      dataAppender.appendToTable(
          org.apache.iceberg.TestHelpers.Row.of(0), bucket0Records); // bucket 0
      dataAppender.appendToTable(
          org.apache.iceberg.TestHelpers.Row.of(1), bucket1Records); // bucket 1
      dataAppender.appendToTable(
          org.apache.iceberg.TestHelpers.Row.of(2), bucket2Records); // bucket 2
      dataAppender.appendToTable(
          org.apache.iceberg.TestHelpers.Row.of(3), bucket3Records); // bucket 3

      final ScanContext scanContext = ScanContext.builder().build();
      final List<IcebergSourceSplit> splits =
          FlinkSplitPlanner.planIcebergSourceSplits(
              table, scanContext, ThreadPools.getWorkerPool());

      return splits.stream()
          .flatMap(
              split -> {
                List<List<FileScanTask>> filesList =
                    Lists.partition(Lists.newArrayList(split.task().files()), 1);
                return filesList.stream()
                    .map(files -> new BaseCombinedScanTask(files))
                    .map(
                        combinedScanTask ->
                            IcebergSourceSplit.fromCombinedScanTask(combinedScanTask));
              })
          .collect(Collectors.toList());
    } finally {
      catalog.dropTable(TestFixtures.TABLE_IDENTIFIER);
      catalog.close();
    }
  }

  /**
   * Create splits with mixed dt (identity) and bucket partitioning - 3 files total. TODO: Move this
   * logic to SplitHelpers once we have a robust implementation.
   */
  private List<IcebergSourceSplit> createMixedPartitionedSplits() throws Exception {
    // Schema with both dt and id fields
    Schema mixedSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "dt", Types.StringType.get()),
            required(3, "data", Types.StringType.get()));

    // Partition spec with both identity and bucket partitioning
    PartitionSpec mixedSpec =
        PartitionSpec.builderFor(mixedSchema).identity("dt").bucket("id", 4).build();

    final File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    final String warehouse = "file:" + warehouseFile;
    Configuration hadoopConf = new Configuration();
    final HadoopCatalog catalog = new HadoopCatalog(hadoopConf, warehouse);

    ImmutableMap<String, String> properties = ImmutableMap.of(TableProperties.FORMAT_VERSION, "1");

    try {
      final Table table =
          catalog.createTable(
              TestFixtures.TABLE_IDENTIFIER, mixedSchema, mixedSpec, null, properties);
      final GenericAppenderHelper dataAppender =
          new GenericAppenderHelper(table, FileFormat.PARQUET, TEMPORARY_FOLDER);

      // File 1: part(dt=2020-06-12/bucket=0)
      List<Record> file1Records = RandomGenericData.generate(mixedSchema, 2, 0L);
      file1Records.get(0).setField("id", 100L); // bucket(100, 4) = 0
      file1Records.get(0).setField("dt", "2020-06-12");
      file1Records.get(1).setField("id", 104L); // bucket(104, 4) = 0
      file1Records.get(1).setField("dt", "2020-06-12");

      // File 2: part(dt=2025-06-11/bucket=1)
      List<Record> file2Records = RandomGenericData.generate(mixedSchema, 2, 1L);
      file2Records.get(0).setField("id", 101L); // bucket(101, 4) = 1
      file2Records.get(0).setField("dt", "2025-06-11");
      file2Records.get(1).setField("id", 105L); // bucket(105, 4) = 1
      file2Records.get(1).setField("dt", "2025-06-11");

      // File 3: part(dt=2025-06-10/bucket=2)
      List<Record> file3Records = RandomGenericData.generate(mixedSchema, 2, 2L);
      file3Records.get(0).setField("id", 102L); // bucket(102, 4) = 2
      file3Records.get(0).setField("dt", "2025-06-10");
      file3Records.get(1).setField("id", 106L); // bucket(106, 4) = 2
      file3Records.get(1).setField("dt", "2025-06-10");

      // Write files with explicit partition values (dt, bucket)
      dataAppender.appendToTable(
          org.apache.iceberg.TestHelpers.Row.of("2020-06-12", 0), file1Records);
      dataAppender.appendToTable(
          org.apache.iceberg.TestHelpers.Row.of("2025-06-11", 1), file2Records);
      dataAppender.appendToTable(
          org.apache.iceberg.TestHelpers.Row.of("2025-06-10", 2), file3Records);

      final ScanContext scanContext = ScanContext.builder().build();
      final List<IcebergSourceSplit> splits =
          FlinkSplitPlanner.planIcebergSourceSplits(
              table, scanContext, ThreadPools.getWorkerPool());

      return splits.stream()
          .flatMap(
              split -> {
                List<List<FileScanTask>> filesList =
                    Lists.partition(Lists.newArrayList(split.task().files()), 1);
                return filesList.stream()
                    .map(files -> new BaseCombinedScanTask(files))
                    .map(
                        combinedScanTask ->
                            IcebergSourceSplit.fromCombinedScanTask(combinedScanTask));
              })
          .collect(Collectors.toList());
    } finally {
      catalog.dropTable(TestFixtures.TABLE_IDENTIFIER);
      catalog.close();
    }
  }

  /**
   * Create splits with identity partitioning on dt and id fields. Used for testing consistent
   * assignment across tables.
   */
  private List<IcebergSourceSplit> createIdentityPartitionedSplits(
      String tablePrefix, int partitionCount) throws Exception {
    // Schema with dt and id fields for identity partitioning
    Schema identitySchema =
        new Schema(
            required(1, "dt", Types.StringType.get()),
            required(2, "id", Types.LongType.get()),
            required(3, "data", Types.StringType.get()));

    // Partition spec with identity partitioning on both dt and id
    PartitionSpec identitySpec =
        PartitionSpec.builderFor(identitySchema).identity("dt").identity("id").build();

    final File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    final String warehouse = "file:" + warehouseFile;
    Configuration hadoopConf = new Configuration();
    final HadoopCatalog catalog = new HadoopCatalog(hadoopConf, warehouse);

    ImmutableMap<String, String> properties = ImmutableMap.of(TableProperties.FORMAT_VERSION, "1");

    try {
      final Table table =
          catalog.createTable(
              TestFixtures.TABLE_IDENTIFIER, identitySchema, identitySpec, null, properties);
      final GenericAppenderHelper dataAppender =
          new GenericAppenderHelper(table, FileFormat.PARQUET, TEMPORARY_FOLDER);

      // Create partitions with predictable dt and id values
      for (int i = 0; i < partitionCount; i++) {
        String dt = "2024-01-" + String.format("%02d", (i % 30) + 1);
        Long id = (long) (i + 1000); // Start IDs from 1000

        List<Record> records = RandomGenericData.generate(identitySchema, 2, i);
        // Set the partition field values
        records.get(0).setField("dt", dt);
        records.get(0).setField("id", id);
        records.get(1).setField("dt", dt);
        records.get(1).setField("id", id);

        // Write file for this partition
        dataAppender.appendToTable(org.apache.iceberg.TestHelpers.Row.of(dt, id), records);
      }

      final ScanContext scanContext = ScanContext.builder().build();
      final List<IcebergSourceSplit> splits =
          FlinkSplitPlanner.planIcebergSourceSplits(
              table, scanContext, ThreadPools.getWorkerPool());

      return splits.stream()
          .flatMap(
              split -> {
                List<List<FileScanTask>> filesList =
                    Lists.partition(Lists.newArrayList(split.task().files()), 1);
                return filesList.stream()
                    .map(files -> new BaseCombinedScanTask(files))
                    .map(
                        combinedScanTask ->
                            IcebergSourceSplit.fromCombinedScanTask(combinedScanTask));
              })
          .collect(Collectors.toList());
    } finally {
      catalog.dropTable(TestFixtures.TABLE_IDENTIFIER);
      catalog.close();
    }
  }

  protected void assertGetNext(
      SplitAssigner assigner,
      GetSplitResult.Status expectedStatus,
      int taskId,
      int registeredTasks) {
    GetSplitResult result = assigner.getNext(null, taskId, registeredTasks);
    Assert.assertEquals(expectedStatus, result.status());
    switch (expectedStatus) {
      case AVAILABLE:
        Assert.assertNotNull(result.split());
        break;
      case CONSTRAINED:
      case UNAVAILABLE:
        Assert.assertNull(result.split());
        break;
      default:
        Assert.fail("Unknown status: " + expectedStatus);
    }
  }
}
