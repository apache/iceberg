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
package org.apache.iceberg.flink.source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.Scan;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;

@Internal
public class FlinkSplitPlanner {
  private FlinkSplitPlanner() {}

  static FlinkInputSplit[] planInputSplits(
      Table table, ScanContext context, ExecutorService workerPool) {
    try (CloseableIterable<CombinedScanTask> tasksIterable =
        planTasks(table, context, workerPool)) {
      List<CombinedScanTask> tasks = Lists.newArrayList(tasksIterable);
      FlinkInputSplit[] splits = new FlinkInputSplit[tasks.size()];
      boolean exposeLocality = context.exposeLocality();

      Tasks.range(tasks.size())
          .stopOnFailure()
          .executeWith(exposeLocality ? workerPool : null)
          .run(
              index -> {
                CombinedScanTask task = tasks.get(index);
                String[] hostnames = null;
                if (exposeLocality) {
                  hostnames = Util.blockLocations(table.io(), task);
                }
                splits[index] = new FlinkInputSplit(index, task, hostnames);
              });
      return splits;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to process tasks iterable", e);
    }
  }

  /** This returns splits for the FLIP-27 source */
  public static List<IcebergSourceSplit> planIcebergSourceSplits(
      Table table, ScanContext context, ExecutorService workerPool) {
    try (CloseableIterable<CombinedScanTask> tasksIterable =
        planTasks(table, context, workerPool)) {
      return Lists.newArrayList(
          CloseableIterable.transform(tasksIterable, IcebergSourceSplit::fromCombinedScanTask));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to process task iterable: ", e);
    }
  }

  static CloseableIterable<CombinedScanTask> planTasks(
      Table table, ScanContext context, ExecutorService workerPool) {
    ScanMode scanMode = checkScanMode(context);
    if (scanMode == ScanMode.INCREMENTAL_APPEND_SCAN) {
      IncrementalAppendScan scan = table.newIncrementalAppendScan();
      scan = refineScanWithBaseConfigs(scan, context, workerPool);

      Long startSnapshotId = resolveStartSnapshotId(table, context);
      if (startSnapshotId != null) {
        scan = scan.fromSnapshotExclusive(startSnapshotId);
      }

      Long toSnapshotId = resolveToSnapshotId(table, context);
      if (toSnapshotId != null) {
        scan = scan.toSnapshot(toSnapshotId);
      }

      return scan.planTasks();
    } else {
      TableScan scan = table.newScan();
      scan = refineScanWithBaseConfigs(scan, context, workerPool);

      if (context.snapshotId() != null) {
        scan = scan.useSnapshot(context.snapshotId());
      } else if (context.tag() != null) {
        scan = scan.useRef(context.tag());
      } else if (context.branch() != null) {
        scan = scan.useRef(context.branch());
      }

      if (context.asOfTimestamp() != null) {
        scan = scan.asOfTime(context.asOfTimestamp());
      }

      return scan.planTasks();
    }
  }

  private static Long resolveToSnapshotId(Table table, ScanContext context) {
    if (context.endTag() != null) {
      Preconditions.checkArgument(
          table.snapshot(context.endTag()) != null,
          "Cannot find snapshot with tag %s",
          context.endTag());
      return table.snapshot(context.endTag()).snapshotId();
    }

    if (context.endSnapshotId() != null) {
      return context.endSnapshotId();
    }

    if (context.endSnapshotTimestamp() != null) {
      return SnapshotUtil.snapshotIdAsOfTime(table, context.endSnapshotTimestamp());
    }

    return null;
  }

  private static Long resolveStartSnapshotId(Table table, ScanContext context) {
    if (context.startTag() != null) {
      Preconditions.checkArgument(
          table.snapshot(context.startTag()) != null,
          "Cannot find snapshot with tag %s",
          context.startTag());
      return table.snapshot(context.startTag()).snapshotId();
    }

    if (context.startSnapshotId() != null) {
      return context.startSnapshotId();
    }

    if (context.startSnapshotTimestamp() != null) {
      return getStartSnapshotId(table, context.startSnapshotTimestamp());
    }

    return null;
  }

  private static Long getStartSnapshotId(Table table, Long startTimestamp) {
    Snapshot oldestSnapshotAfter = SnapshotUtil.oldestAncestorAfter(table, startTimestamp);
    Preconditions.checkArgument(
        oldestSnapshotAfter != null,
        "Cannot find a snapshot older than %s for table %s",
        startTimestamp,
        table.name());

    if (oldestSnapshotAfter.timestampMillis() == startTimestamp) {
      return oldestSnapshotAfter.snapshotId();
    } else {
      return oldestSnapshotAfter.parentId();
    }
  }

  @VisibleForTesting
  enum ScanMode {
    BATCH,
    INCREMENTAL_APPEND_SCAN
  }

  @VisibleForTesting
  static ScanMode checkScanMode(ScanContext context) {
    if (context.startSnapshotId() != null
        || context.startSnapshotTimestamp() != null
        || context.endSnapshotId() != null
        || context.startTag() != null
        || context.endTag() != null) {
      return ScanMode.INCREMENTAL_APPEND_SCAN;
    } else {
      return ScanMode.BATCH;
    }
  }

  /** refine scan with common configs */
  private static <T extends Scan<T, FileScanTask, CombinedScanTask>> T refineScanWithBaseConfigs(
      T scan, ScanContext context, ExecutorService workerPool) {
    T refinedScan =
        scan.caseSensitive(context.caseSensitive()).project(context.project()).planWith(workerPool);

    if (context.includeColumnStats()) {
      refinedScan = refinedScan.includeColumnStats();
    }

    if (context.includeStatsForColumns() != null) {
      refinedScan = refinedScan.includeColumnStats(context.includeStatsForColumns());
    }

    refinedScan = refinedScan.option(TableProperties.SPLIT_SIZE, context.splitSize().toString());

    refinedScan =
        refinedScan.option(TableProperties.SPLIT_LOOKBACK, context.splitLookback().toString());

    refinedScan =
        refinedScan.option(
            TableProperties.SPLIT_OPEN_FILE_COST, context.splitOpenFileCost().toString());

    if (context.filters() != null) {
      for (Expression filter : context.filters()) {
        refinedScan = refinedScan.filter(filter);
      }
    }

    return refinedScan;
  }
}
