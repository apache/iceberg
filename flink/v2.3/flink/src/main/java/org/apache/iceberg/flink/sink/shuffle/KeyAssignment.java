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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Subtask assignment for a key for Map statistics based */
class KeyAssignment {
  private final List<Integer> assignedSubtasks;
  private final List<Long> subtaskWeightsWithCloseFileCost;
  private final long closeFileCostWeight;
  private final long[] subtaskWeightsExcludingCloseCost;
  private final long keyWeight;
  private final long[] cumulativeWeights;

  /**
   * @param assignedSubtasks assigned subtasks for this key. It could be a single subtask. It could
   *     also be multiple subtasks if the key has heavy weight that should be handled by multiple
   *     subtasks.
   * @param subtaskWeightsWithCloseFileCost assigned weight for each subtask. E.g., if the keyWeight
   *     is 27 and the key is assigned to 3 subtasks, subtaskWeights could contain values as [10,
   *     10, 7] for target weight of 10 per subtask.
   */
  KeyAssignment(
      List<Integer> assignedSubtasks,
      List<Long> subtaskWeightsWithCloseFileCost,
      long closeFileCostWeight) {
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
                weight > closeFileCostWeight,
                "Invalid weight: should be larger than close file cost: weight = %s, close file cost = %s",
                weight,
                closeFileCostWeight));

    this.assignedSubtasks = assignedSubtasks;
    this.subtaskWeightsWithCloseFileCost = subtaskWeightsWithCloseFileCost;
    this.closeFileCostWeight = closeFileCostWeight;
    // Exclude the close file cost for key routing
    this.subtaskWeightsExcludingCloseCost =
        subtaskWeightsWithCloseFileCost.stream()
            .mapToLong(weightWithCloseFileCost -> weightWithCloseFileCost - closeFileCostWeight)
            .toArray();
    this.keyWeight = Arrays.stream(subtaskWeightsExcludingCloseCost).sum();
    this.cumulativeWeights = new long[subtaskWeightsExcludingCloseCost.length];
    long cumulativeWeight = 0;
    for (int i = 0; i < subtaskWeightsExcludingCloseCost.length; ++i) {
      cumulativeWeight += subtaskWeightsExcludingCloseCost[i];
      cumulativeWeights[i] = cumulativeWeight;
    }
  }

  List<Integer> assignedSubtasks() {
    return assignedSubtasks;
  }

  List<Long> subtaskWeightsWithCloseFileCost() {
    return subtaskWeightsWithCloseFileCost;
  }

  long closeFileCostWeight() {
    return closeFileCostWeight;
  }

  long[] subtaskWeightsExcludingCloseCost() {
    return subtaskWeightsExcludingCloseCost;
  }

  /**
   * Select a subtask for the key.
   *
   * @return subtask id
   */
  int select() {
    if (assignedSubtasks.size() == 1) {
      // only choice. no need to run random number generator.
      return assignedSubtasks.get(0);
    } else {
      long randomNumber = ThreadLocalRandom.current().nextLong(keyWeight);
      int index = Arrays.binarySearch(cumulativeWeights, randomNumber);
      // choose the subtask where randomNumber < cumulativeWeights[pos].
      // this works regardless whether index is negative or not.
      int position = Math.abs(index + 1);
      Preconditions.checkState(
          position < assignedSubtasks.size(),
          "Invalid selected position: out of range. key weight = %s, random number = %s, cumulative weights array = %s",
          keyWeight,
          randomNumber,
          cumulativeWeights);
      return assignedSubtasks.get(position);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(assignedSubtasks, subtaskWeightsWithCloseFileCost, closeFileCostWeight);
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
    return Objects.equals(assignedSubtasks, that.assignedSubtasks)
        && Objects.equals(subtaskWeightsWithCloseFileCost, that.subtaskWeightsWithCloseFileCost)
        && closeFileCostWeight == that.closeFileCostWeight;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("assignedSubtasks", assignedSubtasks)
        .add("subtaskWeightsWithCloseFileCost", subtaskWeightsWithCloseFileCost)
        .add("closeFileCostWeight", closeFileCostWeight)
        .toString();
  }
}
