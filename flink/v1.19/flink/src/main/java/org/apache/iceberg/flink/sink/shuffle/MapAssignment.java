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

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Key assignment to subtasks for Map statistics. */
class MapAssignment {
  private static final Logger LOG = LoggerFactory.getLogger(MapAssignment.class);

  private final int numPartitions;
  private final Map<SortKey, KeyAssignment> keyAssignments;

  MapAssignment(int numPartitions, Map<SortKey, KeyAssignment> keyAssignments) {
    Preconditions.checkArgument(keyAssignments != null, "Invalid key assignments: null");
    this.numPartitions = numPartitions;
    this.keyAssignments = keyAssignments;
  }

  static MapAssignment fromKeyFrequency(
      int numPartitions,
      Map<SortKey, Long> mapStatistics,
      double closeFileCostWeightPercentage,
      Comparator<StructLike> comparator) {
    return new MapAssignment(
        numPartitions,
        assignment(numPartitions, mapStatistics, closeFileCostWeightPercentage, comparator));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(numPartitions, keyAssignments);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MapAssignment that = (MapAssignment) o;
    return numPartitions == that.numPartitions && keyAssignments.equals(that.keyAssignments);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("numPartitions", numPartitions)
        .add("keyAssignments", keyAssignments)
        .toString();
  }

  int numPartitions() {
    return numPartitions;
  }

  Map<SortKey, KeyAssignment> keyAssignments() {
    return keyAssignments;
  }

  /**
   * @return assignment summary for every subtask. Key is subtaskId. Value pair is (weight assigned
   *     to the subtask, number of keys assigned to the subtask)
   */
  Map<Integer, Pair<Long, Integer>> assignmentInfo() {
    Map<Integer, Pair<Long, Integer>> assignmentInfo = Maps.newTreeMap();
    keyAssignments.forEach(
        (key, keyAssignment) -> {
          for (int i = 0; i < keyAssignment.assignedSubtasks().size(); ++i) {
            int subtaskId = keyAssignment.assignedSubtasks().get(i);
            long subtaskWeight = keyAssignment.subtaskWeightsExcludingCloseCost()[i];
            Pair<Long, Integer> oldValue = assignmentInfo.getOrDefault(subtaskId, Pair.of(0L, 0));
            assignmentInfo.put(
                subtaskId, Pair.of(oldValue.first() + subtaskWeight, oldValue.second() + 1));
          }
        });

    return assignmentInfo;
  }

  static Map<SortKey, KeyAssignment> assignment(
      int numPartitions,
      Map<SortKey, Long> mapStatistics,
      double closeFileCostWeightPercentage,
      Comparator<StructLike> comparator) {
    mapStatistics.forEach(
        (key, value) ->
            Preconditions.checkArgument(
                value > 0, "Invalid statistics: weight is 0 for key %s", key));

    long totalWeight = mapStatistics.values().stream().mapToLong(l -> l).sum();
    double targetWeightPerSubtask = ((double) totalWeight) / numPartitions;
    long closeFileCostWeight =
        (long) Math.ceil(targetWeightPerSubtask * closeFileCostWeightPercentage / 100);

    NavigableMap<SortKey, Long> sortedStatsWithCloseFileCost = Maps.newTreeMap(comparator);
    mapStatistics.forEach(
        (k, v) -> {
          int estimatedSplits = (int) Math.ceil(v / targetWeightPerSubtask);
          long estimatedCloseFileCost = closeFileCostWeight * estimatedSplits;
          sortedStatsWithCloseFileCost.put(k, v + estimatedCloseFileCost);
        });

    long totalWeightWithCloseFileCost =
        sortedStatsWithCloseFileCost.values().stream().mapToLong(l -> l).sum();
    long targetWeightPerSubtaskWithCloseFileCost =
        (long) Math.ceil(((double) totalWeightWithCloseFileCost) / numPartitions);
    return buildAssignment(
        numPartitions,
        sortedStatsWithCloseFileCost,
        targetWeightPerSubtaskWithCloseFileCost,
        closeFileCostWeight);
  }

  private static Map<SortKey, KeyAssignment> buildAssignment(
      int numPartitions,
      NavigableMap<SortKey, Long> sortedStatistics,
      long targetWeightPerSubtask,
      long closeFileCostWeight) {
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
            closeFileCostWeight,
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
        if (assignedWeight <= closeFileCostWeight) {
          long paddingWeight = Math.min(keyRemainingWeight, closeFileCostWeight);
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
      // this might lead to some inaccuracy in weight calculation. E.g., assuming the key weight is
      // 2 and close file cost is 2. key weight with close cost is 4. Let's assume the previous
      // task has a weight of 3 available. So weight of 3 for this key is assigned to the task and
      // the residual weight of 1 is dropped. Then the routing weight for this key is 1 (minus the
      // close file cost), which is inaccurate as the true key weight should be 2.
      // Again, this greedy algorithm is not intended to be perfect. Some small inaccuracy is
      // expected and acceptable. Traffic distribution should still be balanced.
      if (keyRemainingWeight > 0 && keyRemainingWeight <= closeFileCostWeight) {
        keyRemainingWeight = 0;
      }

      if (keyRemainingWeight == 0) {
        // finishing up the assignment for the current key
        KeyAssignment keyAssignment =
            new KeyAssignment(assignedSubtasks, subtaskWeights, closeFileCostWeight);
        assignmentMap.put(currentKey, keyAssignment);
        assignedSubtasks = Lists.newArrayList();
        subtaskWeights = Lists.newArrayList();
        currentKey = null;
      }
    }

    return assignmentMap;
  }
}
