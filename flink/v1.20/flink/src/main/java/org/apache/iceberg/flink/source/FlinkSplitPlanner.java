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
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.Scan;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.source.split.ChangelogScanSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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

  /**
   * Plan changelog scan splits for CDC streaming reads.
   *
   * @param table the Iceberg table
   * @param context the scan context
   * @param workerPool the executor service for planning
   * @return list of ChangelogScanSplit
   */
  public static List<ChangelogScanSplit> planChangelogScanSplits(
      Table table, ScanContext context, ExecutorService workerPool) {
    try (CloseableIterable<ScanTaskGroup<ChangelogScanTask>> taskGroups =
        planChangelogTasks(table, context, workerPool)) {
      return Lists.newArrayList(
          CloseableIterable.transform(
              taskGroups,
              taskGroup -> new ChangelogScanSplit(Lists.newArrayList(taskGroup.tasks()))));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to process changelog task iterable: ", e);
    }
  }

  /**
   * Plan changelog scan tasks.
   *
   * @param table the Iceberg table
   * @param context the scan context
   * @param workerPool the executor service for planning
   * @return closeable iterable of ScanTaskGroup containing ChangelogScanTask
   */
  public static CloseableIterable<ScanTaskGroup<ChangelogScanTask>> planChangelogTasks(
      Table table, ScanContext context, ExecutorService workerPool) {
    IncrementalChangelogScan scan = table.newIncrementalChangelogScan();
    scan = refineChangelogScan(scan, context, workerPool);

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

  static CloseableIterable<CombinedScanTask> planTasks(
      Table table, ScanContext context, ExecutorService workerPool) {
    ScanMode scanMode = checkScanMode(context);
    if (scanMode == ScanMode.INCREMENTAL_APPEND_SCAN) {
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

  @VisibleForTesting
  enum ScanMode {
    BATCH,
    INCREMENTAL_APPEND_SCAN,
    INCREMENTAL_CHANGELOG_SCAN
  }

  @VisibleForTesting
  static ScanMode checkScanMode(ScanContext context) {
    // Check if changelog mode is enabled
    if (context.isChangelogScan()) {
      return ScanMode.INCREMENTAL_CHANGELOG_SCAN;
    }

    if (context.startSnapshotId() != null
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

  /** Refine changelog scan with common configs */
  private static IncrementalChangelogScan refineChangelogScan(
      IncrementalChangelogScan scan, ScanContext context, ExecutorService workerPool) {
    IncrementalChangelogScan refinedScan =
        scan.caseSensitive(context.caseSensitive())
            .project(context.project())
            .planWith(workerPool);

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
