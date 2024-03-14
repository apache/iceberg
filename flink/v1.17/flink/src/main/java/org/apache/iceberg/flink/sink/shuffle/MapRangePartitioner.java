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
package org.apache.iceberg.flink.sink.shuffle;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderComparators;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal partitioner implementation that supports MapDataStatistics, which is typically used for
 * low-cardinality use cases. While MapDataStatistics can keep accurate counters, it can't be used
 * for high-cardinality use cases. Otherwise, the memory footprint is too high.
 *
 * <p>It is a greedy algorithm for bin packing. With close file cost, the calculation isn't always
 * precise when calculating close cost for every file, target weight per subtask, padding residual
 * weight, assigned weight without close cost.
 *
 * <p>All actions should be executed in a single Flink mailbox thread. So there is no need to make
 * it thread safe.
 */
class MapRangePartitioner implements Partitioner<RowData> {
  private static final Logger LOG = LoggerFactory.getLogger(MapRangePartitioner.class);

  private final RowDataWrapper rowDataWrapper;
  private final SortKey sortKey;
  private final Comparator<StructLike> comparator;
  private final Map<SortKey, Long> mapStatistics;
  private final double closeFileCostInWeightPercentage;

  // Counter that tracks how many times a new key encountered
  // where there is no traffic statistics learned about it.
  private long newSortKeyCounter;
  private long lastNewSortKeyLogTimeMilli;

  // lazily computed due to the need of numPartitions
  private Map<SortKey, KeyAssignment> assignment;
  private NavigableMap<SortKey, Long> sortedStatsWithCloseFileCost;

  MapRangePartitioner(
      Schema schema,
      SortOrder sortOrder,
      MapDataStatistics dataStatistics,
      double closeFileCostInWeightPercentage) {
    dataStatistics
        .statistics()
        .entrySet()
        .forEach(
            entry ->
                Preconditions.checkArgument(
                    entry.getValue() > 0,
                    "Invalid statistics: weight is 0 for key %s",
                    entry.getKey()));

    this.rowDataWrapper = new RowDataWrapper(FlinkSchemaUtil.convert(schema), schema.asStruct());
    this.sortKey = new SortKey(schema, sortOrder);
    this.comparator = SortOrderComparators.forSchema(schema, sortOrder);
    this.mapStatistics = dataStatistics.statistics();
    this.closeFileCostInWeightPercentage = closeFileCostInWeightPercentage;
    this.newSortKeyCounter = 0;
    this.lastNewSortKeyLogTimeMilli = System.currentTimeMillis();
  }

  @Override
  public int partition(RowData row, int numPartitions) {
    // assignment table can only be built lazily when first referenced here,
    // because number of partitions (downstream subtasks) is needed.
    // the numPartitions is not available in the constructor.
    Map<SortKey, KeyAssignment> assignmentMap = assignment(numPartitions);
    // reuse the sortKey and rowDataWrapper
    sortKey.wrap(rowDataWrapper.wrap(row));
    KeyAssignment keyAssignment = assignmentMap.get(sortKey);
    if (keyAssignment == null) {
      LOG.trace(
          "Encountered new sort key: {}. Fall back to round robin as statistics not learned yet.",
          sortKey);
      // Ideally unknownKeyCounter should be published as a counter metric.
      // It seems difficult to pass in MetricGroup into the partitioner.
      // Just log an INFO message every minute.
      newSortKeyCounter += 1;
      long now = System.currentTimeMillis();
      if (now - lastNewSortKeyLogTimeMilli > TimeUnit.MINUTES.toMillis(1)) {
        LOG.info("Encounter new sort keys in total {} times", newSortKeyCounter);
        lastNewSortKeyLogTimeMilli = now;
      }
      return (int) (newSortKeyCounter % numPartitions);
    }

    return keyAssignment.select();
  }

  @VisibleForTesting
  Map<SortKey, KeyAssignment> assignment(int numPartitions) {
    if (assignment == null) {
      long totalWeight = mapStatistics.values().stream().mapToLong(l -> l).sum();
      double targetWeightPerSubtask = ((double) totalWeight) / numPartitions;
      long closeFileCostInWeight =
          (long) Math.ceil(targetWeightPerSubtask * closeFileCostInWeightPercentage / 100);

      this.sortedStatsWithCloseFileCost = Maps.newTreeMap(comparator);
      mapStatistics.forEach(
          (k, v) -> {
            int estimatedSplits = (int) Math.ceil(v / targetWeightPerSubtask);
            long estimatedCloseFileCost = closeFileCostInWeight * estimatedSplits;
            sortedStatsWithCloseFileCost.put(k, v + estimatedCloseFileCost);
          });

      long totalWeightWithCloseFileCost =
          sortedStatsWithCloseFileCost.values().stream().mapToLong(l -> l).sum();
      long targetWeightPerSubtaskWithCloseFileCost =
          (long) Math.ceil(((double) totalWeightWithCloseFileCost) / numPartitions);
      this.assignment =
          buildAssignment(
              numPartitions,
              sortedStatsWithCloseFileCost,
              targetWeightPerSubtaskWithCloseFileCost,
              closeFileCostInWeight);
    }

    return assignment;
  }

  @VisibleForTesting
  Map<SortKey, Long> mapStatistics() {
    return mapStatistics;
  }

  /**
   * @return assignment summary for every subtask. Key is subtaskId. Value pair is (weight assigned
   *     to the subtask, number of keys assigned to the subtask)
   */
  Map<Integer, Pair<Long, Integer>> assignmentInfo() {
    Map<Integer, Pair<Long, Integer>> assignmentInfo = Maps.newTreeMap();
    assignment.forEach(
        (key, keyAssignment) -> {
          for (int i = 0; i < keyAssignment.assignedSubtasks.length; ++i) {
            int subtaskId = keyAssignment.assignedSubtasks[i];
            long subtaskWeight = keyAssignment.subtaskWeightsExcludingCloseCost[i];
            Pair<Long, Integer> oldValue = assignmentInfo.getOrDefault(subtaskId, Pair.of(0L, 0));
            assignmentInfo.put(
                subtaskId, Pair.of(oldValue.first() + subtaskWeight, oldValue.second() + 1));
          }
        });

    return assignmentInfo;
  }

  private Map<SortKey, KeyAssignment> buildAssignment(
      int numPartitions,
      NavigableMap<SortKey, Long> sortedStatistics,
      long targetWeightPerSubtask,
      long closeFileCostInWeight) {
    Map<SortKey, KeyAssignment> assignmentMap =
        Maps.newHashMapWithExpectedSize(sortedStatistics.size());
    Iterator<SortKey> mapKeyIterator = sortedStatistics.keySet().iterator();
    int subtaskId = 0;
    SortKey currentKey = null;
    long keyRemainingWeight = 0L;
    long subtaskRemainingWeight = targetWeightPerSubtask;
    List<Integer> assignedSubtasks = Lists.newArrayList();
    List<Long> subtaskWeights = Lists.newArrayList();
    while (mapKeyIterator.hasNext() || currentKey != null) {
      // This should never happen because target weight is calculated using ceil function.
      if (subtaskId >= numPartitions) {
        LOG.error(
            "Internal algorithm error: exhausted subtasks with unassigned keys left. number of partitions: {}, "
                + "target weight per subtask: {}, close file cost in weight: {}, data statistics: {}",
            numPartitions,
            targetWeightPerSubtask,
            closeFileCostInWeight,
            sortedStatistics);
        throw new IllegalStateException(
            "Internal algorithm error: exhausted subtasks with unassigned keys left");
      }

      if (currentKey == null) {
        currentKey = mapKeyIterator.next();
        keyRemainingWeight = sortedStatistics.get(currentKey);
      }

      assignedSubtasks.add(subtaskId);
      if (keyRemainingWeight < subtaskRemainingWeight) {
        // assign the remaining weight of the key to the current subtask
        subtaskWeights.add(keyRemainingWeight);
        subtaskRemainingWeight -= keyRemainingWeight;
        keyRemainingWeight = 0L;
      } else {
        // filled up the current subtask
        long assignedWeight = subtaskRemainingWeight;
        keyRemainingWeight -= subtaskRemainingWeight;

        // If assigned weight is less than close file cost, pad it up with close file cost.
        // This might cause the subtask assigned weight over the target weight.
        // But it should be no more than one close file cost. Small skew is acceptable.
        if (assignedWeight <= closeFileCostInWeight) {
          long paddingWeight = Math.min(keyRemainingWeight, closeFileCostInWeight);
          keyRemainingWeight -= paddingWeight;
          assignedWeight += paddingWeight;
        }

        subtaskWeights.add(assignedWeight);
        // move on to the next subtask
        subtaskId += 1;
        subtaskRemainingWeight = targetWeightPerSubtask;
      }

      Preconditions.checkState(
          assignedSubtasks.size() == subtaskWeights.size(),
          "List size mismatch: assigned subtasks = %s, subtask weights = %s",
          assignedSubtasks,
          subtaskWeights);

      // If the remaining key weight is smaller than the close file cost, simply skip the residual
      // as it doesn't make sense to assign a weight smaller than close file cost to a new subtask.
      // this might lead to some inaccuracy in weight calculation. E.g., assuming the key weight is 2
      // and close file cost is 2. key weight with close cost is 4. Let's assume the previous task
      // has a weight of 3 available. So weight of 3 for this key is assigned to the task and the
      // residual weight of 1 is dropped. Then the routing weight for this key is 1 (minus the close
      // file cost), which is inaccurate as the true key weight should be 2.
      // Again, this greedy algorithm is not intended to be perfect. Some small inaccuracy is
      // expected and acceptable. Traffic distribution should still be balanced.
      if (keyRemainingWeight > 0 && keyRemainingWeight <= closeFileCostInWeight) {
        keyRemainingWeight = 0;
      }

      if (keyRemainingWeight == 0) {
        // finishing up the assignment for the current key
        KeyAssignment keyAssignment =
            new KeyAssignment(assignedSubtasks, subtaskWeights, closeFileCostInWeight);
        assignmentMap.put(currentKey, keyAssignment);
        assignedSubtasks.clear();
        subtaskWeights.clear();
        currentKey = null;
      }
    }

    return assignmentMap;
  }

  /** Subtask assignment for a key */
  @VisibleForTesting
  static class KeyAssignment {
    private final int[] assignedSubtasks;
    private final long[] subtaskWeightsExcludingCloseCost;
    private final long keyWeight;
    private final long[] cumulativeWeights;

    /**
     * @param assignedSubtasks assigned subtasks for this key. It could be a single subtask. It
     *     could also be multiple subtasks if the key has heavy weight that should be handled by
     *     multiple subtasks.
     * @param subtaskWeightsWithCloseFileCost assigned weight for each subtask. E.g., if the
     *     keyWeight is 27 and the key is assigned to 3 subtasks, subtaskWeights could contain
     *     values as [10, 10, 7] for target weight of 10 per subtask.
     */
    KeyAssignment(
        List<Integer> assignedSubtasks,
        List<Long> subtaskWeightsWithCloseFileCost,
        long closeFileCostInWeight) {
      Preconditions.checkArgument(
          assignedSubtasks != null && !assignedSubtasks.isEmpty(),
          "Invalid assigned subtasks: null or empty");
      Preconditions.checkArgument(
          subtaskWeightsWithCloseFileCost != null && !subtaskWeightsWithCloseFileCost.isEmpty(),
          "Invalid assigned subtasks weights: null or empty");
      Preconditions.checkArgument(
          assignedSubtasks.size() == subtaskWeightsWithCloseFileCost.size(),
          "Invalid assignment: size mismatch (tasks length = %s, weights length = %s)",
          assignedSubtasks.size(),
          subtaskWeightsWithCloseFileCost.size());
      subtaskWeightsWithCloseFileCost.forEach(
          weight ->
              Preconditions.checkArgument(
                  weight > closeFileCostInWeight,
                  "Invalid weight: should be larger than close file cost: weight = %s, close file cost = %s",
                  weight,
                  closeFileCostInWeight));

      this.assignedSubtasks = assignedSubtasks.stream().mapToInt(i -> i).toArray();
      // Exclude the close file cost for key routing
      this.subtaskWeightsExcludingCloseCost =
          subtaskWeightsWithCloseFileCost.stream()
              .mapToLong(weightWithCloseFileCost -> weightWithCloseFileCost - closeFileCostInWeight)
              .toArray();
      this.keyWeight = Arrays.stream(subtaskWeightsExcludingCloseCost).sum();
      this.cumulativeWeights = new long[subtaskWeightsExcludingCloseCost.length];
      long cumulativeWeight = 0;
      for (int i = 0; i < subtaskWeightsExcludingCloseCost.length; ++i) {
        cumulativeWeight += subtaskWeightsExcludingCloseCost[i];
        cumulativeWeights[i] = cumulativeWeight;
      }
    }

    /** @return subtask id */
    int select() {
      if (assignedSubtasks.length == 1) {
        // only choice. no need to run random number generator.
        return assignedSubtasks[0];
      } else {
        long randomNumber = ThreadLocalRandom.current().nextLong(keyWeight);
        int index = Arrays.binarySearch(cumulativeWeights, randomNumber);
        // choose the subtask where randomNumber < cumulativeWeights[pos].
        // this works regardless whether index is negative or not.
        int position = Math.abs(index + 1);
        Preconditions.checkState(
            position < assignedSubtasks.length,
            "Invalid selected position: out of range. key weight = %s, random number = %s, cumulative weights array = %s",
            keyWeight,
            randomNumber,
            cumulativeWeights);
        return assignedSubtasks[position];
      }
    }

    @Override
    public int hashCode() {
      return 31 * Arrays.hashCode(assignedSubtasks)
          + Arrays.hashCode(subtaskWeightsExcludingCloseCost);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      KeyAssignment that = (KeyAssignment) o;
      return Arrays.equals(assignedSubtasks, that.assignedSubtasks)
          && Arrays.equals(subtaskWeightsExcludingCloseCost, that.subtaskWeightsExcludingCloseCost);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("assignedSubtasks", assignedSubtasks)
          .add("subtaskWeightsExcludingCloseCost", subtaskWeightsExcludingCloseCost)
          .toString();
    }
  }
}
