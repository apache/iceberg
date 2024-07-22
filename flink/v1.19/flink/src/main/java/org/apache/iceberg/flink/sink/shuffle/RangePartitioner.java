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

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The wrapper class */
@Internal
public class RangePartitioner implements Partitioner<StatisticsOrRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(RangePartitioner.class);

  private final Schema schema;
  private final SortOrder sortOrder;

  private transient AtomicLong roundRobinCounter;
  private transient Partitioner<RowData> delegatePartitioner;

  public RangePartitioner(Schema schema, SortOrder sortOrder) {
    this.schema = schema;
    this.sortOrder = sortOrder;
  }

  @Override
  public int partition(StatisticsOrRecord wrapper, int numPartitions) {
    if (wrapper.hasStatistics()) {
      this.delegatePartitioner = delegatePartitioner(wrapper.statistics());
      return (int) (roundRobinCounter(numPartitions).getAndIncrement() % numPartitions);
    } else {
      if (delegatePartitioner != null) {
        return delegatePartitioner.partition(wrapper.record(), numPartitions);
      } else {
        int partition = (int) (roundRobinCounter(numPartitions).getAndIncrement() % numPartitions);
        LOG.trace("Statistics not available. Round robin to partition {}", partition);
        return partition;
      }
    }
  }

  private AtomicLong roundRobinCounter(int numPartitions) {
    if (roundRobinCounter == null) {
      // randomize the starting point to avoid synchronization across subtasks
      this.roundRobinCounter = new AtomicLong(new Random().nextInt(numPartitions));
    }

    return roundRobinCounter;
  }

  private Partitioner<RowData> delegatePartitioner(GlobalStatistics statistics) {
    if (statistics.type() == StatisticsType.Map) {
      return new MapRangePartitioner(schema, sortOrder, statistics.mapAssignment());
    } else if (statistics.type() == StatisticsType.Sketch) {
      return new SketchRangePartitioner(schema, sortOrder, statistics.rangeBounds());
    } else {
      throw new IllegalArgumentException(
          String.format("Invalid statistics type: %s. Should be Map or Sketch", statistics.type()));
    }
  }

  /**
   * Util method that handles rescale (write parallelism / numPartitions change).
   *
   * @param partition partition caculated based on the existing statistics
   * @param numPartitionsStatsCalculation number of partitions when the assignment was calculated
   *     based on
   * @param numPartitions current number of partitions
   * @return adjusted partition if necessary.
   */
  static int adjustPartitionWithRescale(
      int partition, int numPartitionsStatsCalculation, int numPartitions) {
    if (numPartitionsStatsCalculation <= numPartitions) {
      // no rescale or scale-up case.
      // new subtasks are ignored and not assigned any keys, which is sub-optimal and only
      // transient.
      // when rescale is detected, operator requests new statistics from coordinator upon
      // initialization.
      return partition;
    } else {
      // scale-down case.
      // Use mod % operation to distribution the over-range partitions.
      // It can cause skew among subtasks. but the behavior is still better than
      // discarding the statistics and falling back to round-robin (no clustering).
      // Again, this is transient and stats refresh is requested when rescale is detected.
      return partition % numPartitions;
    }
  }
}
