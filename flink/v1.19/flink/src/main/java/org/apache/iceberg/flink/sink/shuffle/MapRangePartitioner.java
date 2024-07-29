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

import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
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
  private final MapAssignment mapAssignment;

  // Counter that tracks how many times a new key encountered
  // where there is no traffic statistics learned about it.
  private long newSortKeyCounter;
  private long lastNewSortKeyLogTimeMilli;

  MapRangePartitioner(Schema schema, SortOrder sortOrder, MapAssignment mapAssignment) {
    this.rowDataWrapper = new RowDataWrapper(FlinkSchemaUtil.convert(schema), schema.asStruct());
    this.sortKey = new SortKey(schema, sortOrder);
    this.mapAssignment = mapAssignment;
    this.newSortKeyCounter = 0;
    this.lastNewSortKeyLogTimeMilli = System.currentTimeMillis();
  }

  @Override
  public int partition(RowData row, int numPartitions) {
    // reuse the sortKey and rowDataWrapper
    sortKey.wrap(rowDataWrapper.wrap(row));
    KeyAssignment keyAssignment = mapAssignment.keyAssignments().get(sortKey);

    int partition;
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
        LOG.info(
            "Encounter new sort keys {} times. Fall back to round robin as statistics not learned yet",
            newSortKeyCounter);
        lastNewSortKeyLogTimeMilli = now;
        newSortKeyCounter = 0;
      }
      partition = (int) (newSortKeyCounter % numPartitions);
    } else {
      partition = keyAssignment.select();
    }

    return RangePartitioner.adjustPartitionWithRescale(
        partition, mapAssignment.numPartitions(), numPartitions);
  }
}
