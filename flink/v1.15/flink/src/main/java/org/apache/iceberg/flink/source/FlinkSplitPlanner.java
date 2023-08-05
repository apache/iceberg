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
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.Scan;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Tasks;

@Internal
public class FlinkSplitPlanner {
  private FlinkSplitPlanner() {}

  @SuppressWarnings("unchecked")
  static FlinkInputSplit[] planInputSplits(
      Table table, ScanContext context, ExecutorService workerPool) {
    ScanMode scanMode = ScanMode.checkScanMode(context);
    try (CloseableIterable<CombinedScanTask> tasksIterable =
        (CloseableIterable<CombinedScanTask>) planTasks(table, context, workerPool, scanMode)) {
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
  @SuppressWarnings("unchecked")
  public static List<IcebergSourceSplit> planIcebergSourceSplits(
      Table table, ScanContext context, ExecutorService workerPool) {
    ScanMode scanMode = ScanMode.checkScanMode(context);

    try (CloseableIterable<? extends ScanTaskGroup<? extends ScanTask>> tasksIterable =
        planTasks(table, context, workerPool, scanMode)) {

      if (scanMode == ScanMode.CHANGELOG_SCAN) {
        CloseableIterable<ScanTaskGroup<ChangelogScanTask>> changeLogIterable =
            (CloseableIterable<ScanTaskGroup<ChangelogScanTask>>) tasksIterable;
        return Lists.newArrayList(
            CloseableIterable.transform(
                changeLogIterable, IcebergSourceSplit::fromChangeLogScanTask));
      } else {
        CloseableIterable<CombinedScanTask> combinedIterable =
            (CloseableIterable<CombinedScanTask>) tasksIterable;
        return Lists.newArrayList(
            CloseableIterable.transform(
                combinedIterable, IcebergSourceSplit::fromCombinedScanTask));
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to process task iterable: ", e);
    }
  }

  static CloseableIterable<? extends ScanTaskGroup<? extends ScanTask>> planTasks(
      Table table, ScanContext context, ExecutorService workerPool, ScanMode scanMode) {
    if (scanMode == ScanMode.INCREMENTAL_APPEND_SCAN) {
      return planIncrementalTask(table, context, workerPool);
    } else if (scanMode == ScanMode.CHANGELOG_SCAN) {
      return planChangelogTask(table, context, workerPool);
    } else {
      return planBatchTask(table, context, workerPool);
    }
  }

  private static CloseableIterable<CombinedScanTask> planBatchTask(
      Table table, ScanContext context, ExecutorService workerPool) {
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

  private static CloseableIterable<ScanTaskGroup<ChangelogScanTask>> planChangelogTask(
      Table table, ScanContext context, ExecutorService workerPool) {
    IncrementalChangelogScan scan = table.newIncrementalChangelogScan();
    scan = refineScanWithBaseConfigs(scan, context, workerPool);

    if (context.startSnapshotId() != null) {
      scan = scan.fromSnapshotExclusive(context.startSnapshotId());
    }

    if (context.endSnapshotId() != null) {
      scan = scan.toSnapshot(context.endSnapshotId());
    }

    return scan.planTasks();
  }

  private static CloseableIterable<CombinedScanTask> planIncrementalTask(
      Table table, ScanContext context, ExecutorService workerPool) {
    IncrementalAppendScan scan = table.newIncrementalAppendScan();
    scan = refineScanWithBaseConfigs(scan, context, workerPool);

    if (context.startTag() != null) {
      Preconditions.checkArgument(
          table.snapshot(context.startTag()) != null,
          "Cannot find snapshot with tag %s",
          context.startTag());
      scan = scan.fromSnapshotExclusive(table.snapshot(context.startTag()).snapshotId());
    }

    if (context.startSnapshotId() != null) {
      Preconditions.checkArgument(
          context.startTag() == null, "START_SNAPSHOT_ID and START_TAG cannot both be set");
      scan = scan.fromSnapshotExclusive(context.startSnapshotId());
    }

    if (context.endTag() != null) {
      Preconditions.checkArgument(
          table.snapshot(context.endTag()) != null,
          "Cannot find snapshot with tag %s",
          context.endTag());
      scan = scan.toSnapshot(table.snapshot(context.endTag()).snapshotId());
    }

    if (context.endSnapshotId() != null) {
      Preconditions.checkArgument(
          context.endTag() == null, "END_SNAPSHOT_ID and END_TAG cannot both be set");
      scan = scan.toSnapshot(context.endSnapshotId());
    }

    return scan.planTasks();
  }

  /** refine scan with common configs */
  private static <
          T extends Scan<T, ? extends ScanTask, ? extends ScanTaskGroup<? extends ScanTask>>>
      T refineScanWithBaseConfigs(T scan, ScanContext context, ExecutorService workerPool) {
    T refinedScan =
        scan.caseSensitive(context.caseSensitive()).project(context.project()).planWith(workerPool);

    if (context.includeColumnStats()) {
      refinedScan = refinedScan.includeColumnStats();
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
