/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg;

import com.google.common.collect.Lists;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.io.CloseableIterable;
import java.io.Closeable;
import java.util.Collection;

/**
 * API for configuring a table scan.
 * <p>
 * TableScan objects are immutable and can be shared between threads. Refinement methods, like
 * {@link #select(Collection)} and {@link #filter(Expression)}, create new TableScan instances.
 */
public interface TableScan {
  /**
   * Returns the {@link Table} from which this scan loads data.
   *
   * @return this scan's table
   */
  Table table();

  /**
   * Create a new {@link TableScan} from this scan's configuration that will use the given snapshot
   * by ID.
   *
   * @param snapshotId a snapshot ID
   * @return a new scan based on this with the given snapshot ID
   * @throws IllegalArgumentException if the snapshot cannot be found
   */
  TableScan useSnapshot(long snapshotId);

  /**
   * Create a new {@link TableScan} from this scan's configuration that will use the most recent
   * snapshot as of the given time in milliseconds.
   *
   * @param timestampMillis a timestamp in milliseconds.
   * @return a new scan based on this with the current snapshot at the given time
   * @throws IllegalArgumentException if the snapshot cannot be found
   */
  TableScan asOfTime(long timestampMillis);

  /**
   * Create a new {@link TableScan} from the results of this that will project the given columns.
   *
   * @param columns column names from the table's schema
   * @return a new scan based on this with the given projection columns
   */
  default TableScan select(String... columns) {
    return select(Lists.newArrayList(columns));
  }

  /**
   * Create a new {@link TableScan} from this that will read the given manifest columns.
   *
   * @param columns column names from the manifest file schema
   * @return a new scan based on this with the given manifest columns
   */
  TableScan select(Collection<String> columns);

  /**
   * Create a new {@link TableScan} from the results of this filtered by the {@link Expression}.
   *
   * @param expr a filter expression
   * @return a new scan based on this with results filtered by the expression
   */
  TableScan filter(Expression expr);

  /**
   * Plan the {@link FileScanTask files} that will be read by this scan.
   * <p>
   * Each file has a residual expression that should be applied to filter the file's rows.
   * <p>
   * This simple plan returns file scans for each file from position 0 to the file's length. For
   * planning that will combine small files, split large files, and attempt to balance work, use
   * {@link #planTasks()} instead.
   *
   * @return an Iterable of file tasks that are required by this scan
   */
  CloseableIterable<FileScanTask> planFiles();

  /**
   * Plan the {@link CombinedScanTask tasks} for this scan.
   * <p>
   * Tasks created by this method may read partial input files, multiple input files, or both.
   *
   * @return an Iterable of tasks for this scan
   */
  CloseableIterable<CombinedScanTask> planTasks();

  /**
   * Returns this scan's filter {@link Expression}.
   *
   * @return this scan's filter expression
   */
  Expression filter();
}
