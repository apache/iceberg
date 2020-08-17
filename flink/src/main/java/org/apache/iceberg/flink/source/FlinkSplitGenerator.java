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
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class FlinkSplitGenerator {

  private final Table table;
  private final Schema projectedSchema;
  private final ScanOptions options;
  private final List<Expression> filterExpressions;

  FlinkSplitGenerator(Table table, Schema projectedSchema, ScanOptions options, List<Expression> filterExpressions) {
    this.table = table;
    this.projectedSchema = projectedSchema;
    this.options = options;
    this.filterExpressions = filterExpressions;
  }

  FlinkInputSplit[] createInputSplits() {
    List<CombinedScanTask> tasks = tasks();
    FlinkInputSplit[] splits = new FlinkInputSplit[tasks.size()];
    for (int i = 0; i < tasks.size(); i++) {
      splits[i] = new FlinkInputSplit(i, tasks.get(i));
    }
    return splits;
  }

  private List<CombinedScanTask> tasks() {
    TableScan scan = table
        .newScan()
        .caseSensitive(options.isCaseSensitive())
        .project(projectedSchema);

    if (options.getSnapshotId() != null) {
      scan = scan.useSnapshot(options.getSnapshotId());
    }

    if (options.getAsOfTimestamp() != null) {
      scan = scan.asOfTime(options.getAsOfTimestamp());
    }

    if (options.getStartSnapshotId() != null) {
      if (options.getEndSnapshotId() != null) {
        scan = scan.appendsBetween(options.getStartSnapshotId(), options.getEndSnapshotId());
      } else {
        scan = scan.appendsAfter(options.getStartSnapshotId());
      }
    }

    if (options.getSplitSize() != null) {
      scan = scan.option(TableProperties.SPLIT_SIZE, options.getSplitSize().toString());
    }

    if (options.getSplitLookback() != null) {
      scan = scan.option(TableProperties.SPLIT_LOOKBACK, options.getSplitLookback().toString());
    }

    if (options.getSplitOpenFileCost() != null) {
      scan = scan.option(TableProperties.SPLIT_OPEN_FILE_COST, options.getSplitOpenFileCost().toString());
    }

    if (filterExpressions != null) {
      for (Expression filter : filterExpressions) {
        scan = scan.filter(filter);
      }
    }

    try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
      return Lists.newArrayList(tasksIterable);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close table scan: " + scan, e);
    }
  }
}
