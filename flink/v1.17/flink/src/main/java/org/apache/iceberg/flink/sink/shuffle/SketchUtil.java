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
import java.util.Map;
import java.util.function.Consumer;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.StructLike;

class SketchUtil {
  static final int COORDINATOR_MIN_RESERVOIR_SIZE = 10_000;
  static final int COORDINATOR_MAX_RESERVOIR_SIZE = 1_000_000;
  static final int COORDINATOR_TARGET_PARTITIONS_MULTIPLIER = 100;
  static final int OPERATOR_OVER_SAMPLE_RATIO = 10;

  // switch the statistics tracking from map to sketch if the cardinality of the sort key is over
  // this threshold. It is hardcoded for now, we can revisit in the future if config is needed.
  static final int OPERATOR_SKETCH_SWITCH_THRESHOLD = 10_000;
  static final int COORDINATOR_SKETCH_SWITCH_THRESHOLD = 100_000;

  private SketchUtil() {}

  /**
   * The larger the reservoir size, the more accurate for range bounds calculation and the more
   * balanced range distribution.
   *
   * <p>Here are the heuristic rules
   * <li>Target size: numPartitions x 100 to achieve good accuracy and is easier to calculate the
   *     range bounds
   * <li>Min is 10K to achieve good accuracy while memory footprint is still relatively small
   * <li>Max is 1M to cap the memory footprint on coordinator
   *
   * @param numPartitions number of range partitions which equals to downstream operator parallelism
   * @return reservoir size
   */
  static int determineCoordinatorReservoirSize(int numPartitions) {
    int reservoirSize = numPartitions * COORDINATOR_TARGET_PARTITIONS_MULTIPLIER;

    if (reservoirSize < COORDINATOR_MIN_RESERVOIR_SIZE) {
      // adjust it up and still make reservoirSize divisible by numPartitions
      int remainder = COORDINATOR_MIN_RESERVOIR_SIZE % numPartitions;
      reservoirSize = COORDINATOR_MIN_RESERVOIR_SIZE + (numPartitions - remainder);
    } else if (reservoirSize > COORDINATOR_MAX_RESERVOIR_SIZE) {
      // adjust it down and still make reservoirSize divisible by numPartitions
      int remainder = COORDINATOR_MAX_RESERVOIR_SIZE % numPartitions;
      reservoirSize = COORDINATOR_MAX_RESERVOIR_SIZE - remainder;
    }

    return reservoirSize;
  }

  /**
   * Determine the sampling reservoir size where operator subtasks collect data statistics.
   *
   * <p>Here are the heuristic rules
   * <li>Target size is "coordinator reservoir size * over sampling ration (10) / operator
   *     parallelism"
   * <li>Min is 1K to achieve good accuracy while memory footprint is still relatively small
   * <li>Max is 100K to cap the memory footprint on coordinator
   *
   * @param numPartitions number of range partitions which equals to downstream operator parallelism
   * @param operatorParallelism data statistics operator parallelism
   * @return reservoir size
   */
  static int determineOperatorReservoirSize(int operatorParallelism, int numPartitions) {
    int coordinatorReservoirSize = determineCoordinatorReservoirSize(numPartitions);
    int totalOperatorSamples = coordinatorReservoirSize * OPERATOR_OVER_SAMPLE_RATIO;
    return (int) Math.ceil((double) totalOperatorSamples / operatorParallelism);
  }

  /**
   * To understand how range bounds are used in range partitioning, here is an example for human
   * ages with 4 partitions: [15, 32, 60]. The 4 ranges would be
   *
   * <ul>
   *   <li>age <= 15
   *   <li>age > 15 && age <= 32
   *   <li>age >32 && age <= 60
   *   <li>age > 60
   * </ul>
   *
   * <p>Assumption is that a single key is not dominant enough to span multiple subtasks.
   *
   * @param numPartitions number of partitions which maps to downstream operator parallelism
   * @param samples sampled keys
   * @return array of range partition bounds. It should be a sorted list (ascending). Number of
   *     items should be {@code numPartitions - 1}. if numPartitions is 1, return an empty list
   */
  static SortKey[] rangeBounds(
      int numPartitions, Comparator<StructLike> comparator, SortKey[] samples) {
    // sort the keys first
    Arrays.sort(samples, comparator);
    int numCandidates = numPartitions - 1;
    SortKey[] candidates = new SortKey[numCandidates];
    int step = (int) Math.ceil((double) samples.length / numPartitions);
    int position = step - 1;
    int numChosen = 0;
    while (position < samples.length && numChosen < numCandidates) {
      SortKey candidate = samples[position];
      // skip duplicate values
      if (numChosen > 0 && candidate.equals(candidates[numChosen - 1])) {
        // linear probe for the next distinct value
        position += 1;
      } else {
        candidates[numChosen] = candidate;
        position += step;
        numChosen += 1;
      }
    }

    return candidates;
  }

  /** This can be a bit expensive since it is quadratic. */
  static void convertMapToSketch(
      Map<SortKey, Long> taskMapStats, Consumer<SortKey> sketchConsumer) {
    taskMapStats.forEach(
        (sortKey, count) -> {
          for (int i = 0; i < count; ++i) {
            sketchConsumer.accept(sortKey);
          }
        });
  }
}
