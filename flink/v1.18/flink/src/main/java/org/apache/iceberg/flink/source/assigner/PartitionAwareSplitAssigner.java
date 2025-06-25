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

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitState;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitStatus;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;

/**
 * A partition-aware split assigner that distributes splits to tasks based on consistent hashing of
 * partition grouping keys, ensuring better data locality and more balanced workload distribution.
 *
 * <p>This assigner groups splits by their partition information and uses Iceberg's partition
 * grouping key computation along with consistent hashing to assign splits to specific task IDs.
 *
 * <h3>Lifecycle and Initialization</h3>
 *
 * <p>The assigner uses lazy initialization because the number of registered tasks is only known at
 * runtime during the first {@link #getNext(String, int, int)} call:
 *
 * <h3>Partition Grouping</h3>
 *
 * <p>Uses {@link org.apache.iceberg.Partitioning#groupingKeyType(String, java.util.Set)} to compute
 * partition grouping keys that work across different partition specifications. This handles:
 *
 * <ul>
 *   <li>Identity partitions
 *   <li>Transform partitions (bucket, truncate, year, month, day, hour)
 *   <li>Mixed partition schemes in the same table
 * </ul>
 *
 * <h3>Thread Safety</h3>
 *
 * <p>All public methods are synchronized as required by the Flink split enumerator contract. Since
 * all methods are called from the source coordinator thread, synchronization provides consistency
 * without performance concerns.
 *
 * <h3>State Management</h3>
 *
 * <p>Supports checkpoint/restore through the {@link #state()} method. The assigner tracks all
 * pending splits regardless of their assignment state, allowing for complete state recovery.
 *
 * <p>Compared to {@link DefaultSplitAssigner}, this implementation provides better data locality
 * for partitioned tables at the cost of slightly more complex initialization logic.
 *
 * @see DefaultSplitAssigner for simple round-robin assignment
 * @see org.apache.iceberg.Partitioning for partition grouping key computation
 */
public class PartitionAwareSplitAssigner implements SplitAssigner {
  /**
   * Temporary task ID used to store splits before the number of registered tasks is known. This
   * enables lazy initialization where splits can be added before the job is fully initialized, and
   * then properly distributed using consistent hashing once the number of tasks is known.
   *
   * <p>addSplits() is called before getNext(), which is when the initialization happens. At this
   * stage we know the number of registeredTasks, but not before
   */
  private static final int UNINITIALIZED_TASK_ID = -1;

  private final Map<Integer, Queue<IcebergSourceSplit>> pendingSplitsByTaskId;
  private Optional<CompletableFuture<Void>> availableFuture;

  private final Map<Integer, StructProjection> groupingKeyProjectionsBySpec;
  // TODO -- no utilClass found for write-once, we can later on determine more robust way for
  // write-once
  private int registeredTasks;
  private Optional<Types.StructType> groupingKeyType;

  // TODO -- fail PartitionAwareSplitAssigner if execution mode is streaming
  public PartitionAwareSplitAssigner() {
    this.pendingSplitsByTaskId = Maps.newHashMap();
    this.registeredTasks = -1;
    this.groupingKeyProjectionsBySpec = Maps.newHashMap();
    this.availableFuture = Optional.empty();
    this.groupingKeyType = Optional.empty();
  }

  public PartitionAwareSplitAssigner(Collection<IcebergSourceSplitState> assignerState) {
    this();
    List<IcebergSourceSplit> restoredSplits = Lists.newArrayList();
    assignerState.stream().map(IcebergSourceSplitState::split).forEach(restoredSplits::add);
    this.addSplits(restoredSplits);
  }

  @Override
  public synchronized GetSplitResult getNext(
      @Nullable String hostname, int subtaskId, int numRegisteredTasks) {
    if (hasSplitsAvailable()) {
      // Initialize if we have uninitialized splits and now know the registered tasks
      if (isUninitialized()) {
        initialize(numRegisteredTasks);
      }
      // After initialization, registeredTasks must remain consistent
      Preconditions.checkArgument(
          numRegisteredTasks == this.registeredTasks,
          "registeredTasks must remain consistent after initialization. Expected: %s, but got: %s",
          this.registeredTasks,
          numRegisteredTasks);
      Preconditions.checkState(
          !pendingSplitsByTaskId.containsKey(UNINITIALIZED_TASK_ID),
          "After initialization, unregistered_task_id must be removed from pending splits mapping");
      Queue<IcebergSourceSplit> taskSplits = pendingSplitsByTaskId.get(subtaskId);
      if (taskSplits != null && !taskSplits.isEmpty()) {
        IcebergSourceSplit split = taskSplits.poll();
        return GetSplitResult.forSplit(split);
      }
    }
    return GetSplitResult.unavailable();
  }

  @Override
  public synchronized void onDiscoveredSplits(Collection<IcebergSourceSplit> splits) {
    addSplits(splits);
  }

  @Override
  public synchronized void onUnassignedSplits(Collection<IcebergSourceSplit> splits) {
    addSplits(splits);
  }

  @Override
  public synchronized Collection<IcebergSourceSplitState> state() {
    return pendingSplitsByTaskId.values().stream()
        .flatMap(Queue::stream)
        .map(split -> new IcebergSourceSplitState(split, IcebergSourceSplitStatus.UNASSIGNED))
        .collect(Collectors.toList());
  }

  @Override
  public synchronized CompletableFuture<Void> isAvailable() {
    if (!availableFuture.isPresent()) {
      availableFuture = Optional.of(new CompletableFuture<>());
    }
    return availableFuture.get();
  }

  @Override
  public synchronized int pendingSplitCount() {
    return pendingSplitsByTaskId.values().stream().mapToInt(Queue::size).sum();
  }

  @Override
  public synchronized long pendingRecords() {
    return pendingSplitsByTaskId.values().stream()
        .flatMap(Queue::stream)
        .mapToLong(split -> split.task().estimatedRowsCount())
        .sum();
  }

  private synchronized void addSplits(Collection<IcebergSourceSplit> splits) {
    if (!splits.isEmpty()) {
      // registeredTasks are defined upon initialization
      if (registeredTasks > 0) {
        distributeNewSplitsToTasks(splits);
      } else {
        pendingSplitsByTaskId
            .computeIfAbsent(UNINITIALIZED_TASK_ID, k -> new ArrayDeque<>())
            .addAll(splits);
      }
      completeAvailableFuturesIfNeeded();
    }
  }

  private synchronized void completeAvailableFuturesIfNeeded() {
    if (availableFuture.isPresent() && hasSplitsAvailable()) {
      availableFuture.get().complete(null);
    }
    availableFuture = Optional.empty();
  }

  private synchronized void initialize(int numRegisteredTasks) {
    // Validate all state is in unset/initial state before initialization
    Preconditions.checkState(
        this.registeredTasks == -1,
        "registeredTasks must be unset, but was: %s",
        this.registeredTasks);
    Preconditions.checkState(
        !this.groupingKeyType.isPresent(),
        "groupingKeyType must be unset, but was: %s",
        this.groupingKeyType);
    // Validate that only uninitialized splits exist (no task-specific queues)
    long taskSpecificSplitCount =
        pendingSplitsByTaskId.entrySet().stream()
            .filter(entry -> entry.getKey() != UNINITIALIZED_TASK_ID)
            .mapToLong(entry -> entry.getValue().size())
            .sum();
    Preconditions.checkState(
        taskSpecificSplitCount == 0,
        "Expected no task-specific splits before initialization, but found %s splits across task-specific queues",
        taskSpecificSplitCount);
    // initialize() should only be called when uninitialized splits are present
    Queue<IcebergSourceSplit> uninitializedSplits =
        pendingSplitsByTaskId.get(UNINITIALIZED_TASK_ID);
    Preconditions.checkState(
        uninitializedSplits != null && !uninitializedSplits.isEmpty(),
        "Expected uninitialized splits to be present when initialize() is called, but uninitializedSplits was: %s",
        uninitializedSplits);

    // TODO -- in future if need to deal with Schema evolution of partitioning, should revisit this
    // function call
    Types.StructType computedGroupingKeyType =
        Partitioning.groupingKeyType(null, getPartitionSpecs(uninitializedSplits));
    // Compute the grouping key type using Iceberg's Partitioning utility
    // This automatically handles all transform types (Month, bucket, Identity, etc.)
    this.registeredTasks = numRegisteredTasks;
    // Store the grouping key type for consistent extraction across all splits
    this.groupingKeyType = Optional.of(computedGroupingKeyType);
    // Distribute uninitialized splits to actual task IDs
    distributeNewSplitsToTasks(uninitializedSplits);
    // Remove the uninitialized queue
    pendingSplitsByTaskId.remove(UNINITIALIZED_TASK_ID);
  }

  /**
   * Distributes splits to tasks using a two-level partition-aware approach that ensures both
   * partition locality and balanced task utilization.
   *
   * <p>This method follows Spark's proven pattern by first grouping splits by their logical
   * partition, then distributing partition groups deterministically across available tasks using
   * round-robin assignment.
   *
   * <h3>Algorithm Overview:</h3>
   *
   * <pre>
   * Step 1: Group splits by partition key
   *   Splits: [s1, s2, s3, s4, s5, s6]
   *   ↓ (group by partition)
   *   Partition A: [s1, s2]
   *   Partition B: [s3, s4]
   *   Partition C: [s5, s6]
   *
   * Step 2: Sort partitions deterministically
   *   [Partition A, Partition B, Partition C]
   *   ↓ (sort by string representation)
   *   [Partition A, Partition B, Partition C]
   *
   * Step 3: Distribute partitions round-robin to tasks
   *   Task 0: Partition A → [s1, s2]
   *   Task 1: Partition B → [s3, s4]
   *   Task 2: Partition C → [s5, s6]
   *   Task 0: (next partition would go here)
   * </pre>
   *
   * <h3>Benefits:</h3>
   *
   * <ul>
   *   <li><b>Partition Locality:</b> All splits from the same logical partition are assigned to the
   *       same task, enabling storage-level optimizations and efficient joins.
   *   <li><b>Balanced Distribution:</b> Round-robin assignment ensures even task utilization across
   *       all available tasks.
   *   <li><b>Deterministic Assignment:</b> Sorting partition keys ensures consistent assignment
   *       across tables, enabling storage partition joins.
   *   <li><b>No Hash Collisions:</b> Avoids hash-based assignment issues that could cause uneven
   *       distribution or co-location problems.
   * </ul>
   *
   * @param splits the splits to distribute across tasks
   */
  private synchronized void distributeNewSplitsToTasks(Collection<IcebergSourceSplit> splits) {
    Preconditions.checkState(
        registeredTasks > 0, "registeredTasks must be positive, but was: %s", registeredTasks);
    Map<StructLike, List<IcebergSourceSplit>> splitsByPartition = Maps.newHashMap();
    for (IcebergSourceSplit split : splits) {
      StructLike groupingKey = extractGroupingKey(split);
      splitsByPartition.computeIfAbsent(groupingKey, k -> Lists.newArrayList()).add(split);
    }

    // Sort partition keys deterministically for cross-table consistency
    List<StructLike> sortedPartitionKeys =
        Lists.newArrayList(splitsByPartition.keySet().iterator());
    sortedPartitionKeys.sort(Comparator.comparing(Object::toString));

    int assignedTaskId = -1;
    for (int i = 0; i < sortedPartitionKeys.size(); i++) {
      StructLike partitionKey = sortedPartitionKeys.get(i);
      List<IcebergSourceSplit> partitionSplits = splitsByPartition.get(partitionKey);
      assignedTaskId = i % registeredTasks;
      pendingSplitsByTaskId
          .computeIfAbsent(assignedTaskId, k -> new ArrayDeque<>())
          .addAll(partitionSplits);
    }
  }

  /**
   * Extracts the shared partition key from a split's file tasks.
   *
   * <p>Since each {@code IcebergSourceSplit} comes from {@code TableScanUtil#planTaskGroups()}, all
   * file tasks within the split already share the same partition values. This method extracts that
   * common partition key for task assignment.
   *
   * <p>For example, if a split contains files from partition {@code (dt=2024-01-01, bucket=5)},
   * this method returns a {@code StructLike} representing that partition key, which will be used to
   * assign the entire split to a specific task.
   *
   * @param split the split to extract partition key from (all files have same partition)
   * @return the shared partition key for all files in this split
   */
  private synchronized StructLike extractGroupingKey(IcebergSourceSplit split) {
    // motivation from `TableScanUtil#lanTaskGroups`
    // Use the consistent grouping key type computed during initialization
    Preconditions.checkState(
        groupingKeyType.isPresent(),
        "groupingKeyType must be initialized before extracting grouping keys");

    FileScanTask firstTask = split.task().files().iterator().next();
    PartitionSpec spec = firstTask.spec();
    StructLike partition = firstTask.partition();
    Types.StructType currentGroupingKeyType = this.groupingKeyType.get();
    StructProjection projection =
        groupingKeyProjectionsBySpec.computeIfAbsent(
            spec.specId(),
            specId -> StructProjection.create(spec.partitionType(), currentGroupingKeyType));
    PartitionData groupingKeyTemplate = new PartitionData(currentGroupingKeyType);
    return groupingKeyTemplate.copyFor(projection.wrap(partition));
  }

  /**
   * Extracts all unique partition specifications from the given splits.
   *
   * <p>Returns the set of {@code PartitionSpec} objects used by the file tasks within the splits. A
   * {@code PartitionSpec} defines how a table is partitioned (e.g., by date, by bucket, etc.). This
   * is used to compute the grouping key type that works across different partition specifications.
   *
   * <p>For example, may return specs that produce grouping keys like: {@code struct<1000: dt:
   * optional string, 1001: user_id_bucket: optional int>}
   *
   * @param splits the splits to extract partition specs from
   * @return set of unique partition specifications
   */
  private synchronized Set<PartitionSpec> getPartitionSpecs(Collection<IcebergSourceSplit> splits) {
    return splits.stream()
        .flatMap(split -> split.task().files().stream())
        .map(FileScanTask::spec)
        .collect(Collectors.toSet());
  }

  private synchronized boolean hasSplitsAvailable() {
    return pendingSplitsByTaskId.values().stream().anyMatch(queue -> !queue.isEmpty());
  }

  private synchronized boolean isUninitialized() {
    return this.registeredTasks == -1;
  }
}
