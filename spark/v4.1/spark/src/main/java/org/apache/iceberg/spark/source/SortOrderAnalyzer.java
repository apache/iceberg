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

import java.util.List;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analyzes whether sort ordering can be reported for a table's task groups.
 *
 * <p>For sort ordering to be reported, ALL of these conditions must hold:
 *
 * <ul>
 *   <li>The table has a defined sort order (non-null and {@code sortOrder.isSorted() == true})
 *   <li>Each partition key maps to exactly ONE task group (Spark drops the ordering guarantee when
 *       multiple {@code InputPartition}s share the same partition key)
 *   <li>Every {@link FileScanTask} in every task group carries the current sort order ID
 * </ul>
 */
class SortOrderAnalyzer {

  private static final Logger LOG = LoggerFactory.getLogger(SortOrderAnalyzer.class);

  private SortOrderAnalyzer() {}

  /**
   * Returns {@code true} only when sort ordering can be safely reported to Spark for the given
   * table and task groups.
   */
  static boolean canReportOrdering(
      Table table, List<? extends ScanTaskGroup<?>> taskGroups, Types.StructType groupingKeyType) {

    SortOrder sortOrder = table.sortOrder();

    if (sortOrder == null || sortOrder.isUnsorted()) {
      LOG.debug("Cannot report ordering: table {} has no sort order defined", table.name());
      return false;
    }

    if (taskGroups == null || taskGroups.isEmpty()) {
      LOG.debug("Cannot report ordering: no task groups for table {}", table.name());
      return false;
    }

    if (!hasUniquePartitionKeys(taskGroups, groupingKeyType)) {
      LOG.debug(
          "Cannot report ordering: table {} has multiple task groups sharing the same partition"
              + " key.",
          table.name());
      return false;
    }

    for (ScanTaskGroup<?> taskGroup : taskGroups) {
      if (!allFilesHaveSortOrder(taskGroup, sortOrder.orderId())) {
        LOG.debug(
            "Cannot report ordering: table {} has files whose sort order ID does not match the"
                + " current table sort order {}",
            table.name(),
            sortOrder.orderId());
        return false;
      }
    }

    return true;
  }

  /**
   * Checks that each partition key appears in at most one task group.
   *
   * <p>When multiple {@code InputPartition}s share the same partition key, Spark's {@code
   * EnsureRequirements} coalesces them into a single task at join/aggregate time. Because this
   * coalescing simply concatenates the partitions rather than merge-sorting them, it destroys the
   * within-partition ordering guarantee. Reporting ordering in this situation would cause incorrect
   * query results.
   */
  private static boolean hasUniquePartitionKeys(
      List<? extends ScanTaskGroup<?>> taskGroups, Types.StructType groupingKeyType) {

    if (groupingKeyType == null || groupingKeyType.fields().isEmpty()) {
      return true;
    }

    StructLikeSet seenKeys = StructLikeSet.create(groupingKeyType);
    for (ScanTaskGroup<?> taskGroup : taskGroups) {
      StructLike key = taskGroup.groupingKey();
      if (key != null && !seenKeys.add(key)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Checks that every {@link FileScanTask} in the task group carries a sort order ID that matches
   * the table's current sort order.
   *
   * <p>Non-{@code FileScanTask} entries (e.g. changelog tasks) are skipped.
   */
  private static boolean allFilesHaveSortOrder(
      ScanTaskGroup<?> taskGroup, int expectedSortOrderId) {
    for (ScanTask task : taskGroup.tasks()) {
      if (!(task instanceof FileScanTask)) {
        continue;
      }

      FileScanTask fileTask = (FileScanTask) task;
      Integer fileSortOrderId = fileTask.file().sortOrderId();

      if (fileSortOrderId == null || fileSortOrderId != expectedSortOrderId) {
        return false;
      }
    }

    return true;
  }
}
