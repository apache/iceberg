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
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Tasks;

@Internal
public class FlinkSplitPlanner {
  private FlinkSplitPlanner() {
  }

  static FlinkInputSplit[] planInputSplits(Table table, ScanContext context, ExecutorService workerPool) {
    try (CloseableIterable<CombinedScanTask> tasksIterable = planTasks(table, context, workerPool)) {
      List<CombinedScanTask> tasks = Lists.newArrayList(tasksIterable);
      FlinkInputSplit[] splits = new FlinkInputSplit[tasks.size()];
      boolean exposeLocality = context.exposeLocality();

      Tasks.range(tasks.size())
          .stopOnFailure()
          .executeWith(exposeLocality ? workerPool : null)
          .run(index -> {
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

  /**
   * This returns splits for the FLIP-27 source
   */
  public static List<IcebergSourceSplit> planIcebergSourceSplits(
      Table table, ScanContext context, ExecutorService workerPool) {
    try (CloseableIterable<CombinedScanTask> tasksIterable = planTasks(table, context, workerPool)) {
      return Lists.newArrayList(CloseableIterable.transform(tasksIterable, IcebergSourceSplit::fromCombinedScanTask));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to process task iterable: ", e);
    }
  }

  static CloseableIterable<CombinedScanTask> planTasks(Table table, ScanContext context, ExecutorService workerPool) {
    TableScan scan = table
        .newScan()
        .caseSensitive(context.caseSensitive())
        .project(context.project())
        .planWith(workerPool);

    if (context.includeColumnStats()) {
      scan = scan.includeColumnStats();
    }

    if (context.snapshotId() != null) {
      scan = scan.useSnapshot(context.snapshotId());
    }

    if (context.asOfTimestamp() != null) {
      scan = scan.asOfTime(context.asOfTimestamp());
    }

    if (context.endSnapshotId() != null) {
      // null startSnapshotId is ok
      scan = scan.appendsInRange(context.startSnapshotId(), context.endSnapshotId());
    } else {
      if (context.startSnapshotId() != null) {
        scan = scan.appendsAfter(context.startSnapshotId());
      }
    }

    if (context.splitSize() != null) {
      scan = scan.option(TableProperties.SPLIT_SIZE, context.splitSize().toString());
    }

    if (context.splitLookback() != null) {
      scan = scan.option(TableProperties.SPLIT_LOOKBACK, context.splitLookback().toString());
    }

    if (context.splitOpenFileCost() != null) {
      scan = scan.option(TableProperties.SPLIT_OPEN_FILE_COST, context.splitOpenFileCost().toString());
    }

    if (context.filters() != null) {
      for (Expression filter : context.filters()) {
        scan = scan.filter(filter);
      }
    }

    return scan.planTasks();
  }
}
