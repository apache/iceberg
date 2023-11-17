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
package org.apache.iceberg;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Scan objects are immutable and can be shared between threads. Refinement methods, like {@link
 * #select(Collection)} and {@link #filter(Expression)}, create new TableScan instances.
 *
 * @param <ThisT> the child Java API class, returned by method chaining
 * @param <T> the Java type of tasks produces by this scan
 * @param <G> the Java type of task groups produces by this scan
 */
public interface Scan<ThisT, T extends ScanTask, G extends ScanTaskGroup<T>> {
  /**
   * Create a new scan from this scan's configuration that will override the {@link Table}'s
   * behavior based on the incoming pair. Unknown properties will be ignored.
   *
   * @param property name of the table property to be overridden
   * @param value value to override with
   * @return a new scan based on this with overridden behavior
   */
  ThisT option(String property, String value);

  /**
   * Create a new scan from this with the schema as its projection.
   *
   * @param schema a projection schema
   * @return a new scan based on this with the given projection
   */
  ThisT project(Schema schema);

  /**
   * Create a new scan from this that, if data columns where selected via {@link
   * #select(java.util.Collection)}, controls whether the match to the schema will be done with case
   * sensitivity. Default is true.
   *
   * @return a new scan based on this with case sensitivity as stated
   */
  ThisT caseSensitive(boolean caseSensitive);

  /**
   * Returns whether this scan is case-sensitive with respect to column names.
   *
   * @return true if case-sensitive, false otherwise.
   */
  boolean isCaseSensitive();

  /**
   * Create a new scan from this that loads the column stats with each data file.
   *
   * <p>Column stats include: value count, null value count, lower bounds, and upper bounds.
   *
   * @return a new scan based on this that loads column stats.
   */
  ThisT includeColumnStats();

  /**
   * Create a new scan from this that loads the column stats for the specific columns with each data
   * file.
   *
   * <p>Column stats include: value count, null value count, lower bounds, and upper bounds.
   *
   * @param requestedColumns column names for which to keep the stats.
   * @return a new scan based on this that loads column stats for specific columns.
   */
  default ThisT includeColumnStats(Collection<String> requestedColumns) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " doesn't implement includeColumnStats");
  }

  /**
   * Create a new scan from this that will read the given data columns. This produces an expected
   * schema that includes all fields that are either selected or used by this scan's filter
   * expression.
   *
   * @param columns column names from the table's schema
   * @return a new scan based on this with the given projection columns
   */
  ThisT select(Collection<String> columns);

  /**
   * Create a new scan from this that will read the given columns. This produces an expected schema
   * that includes all fields that are either selected or used by this scan's filter expression.
   *
   * @param columns column names
   * @return a new scan based on this with the given projection columns
   */
  default ThisT select(String... columns) {
    return select(Lists.newArrayList(columns));
  }

  /**
   * Create a new scan from the results of this filtered by the {@link Expression}.
   *
   * @param expr a filter expression
   * @return a new scan based on this with results filtered by the expression
   */
  ThisT filter(Expression expr);

  /**
   * Returns this scan's filter {@link Expression}.
   *
   * @return this scan's filter expression
   */
  Expression filter();

  /**
   * Create a new scan from this that applies data filtering to files but not to rows in those
   * files.
   *
   * @return a new scan based on this that does not filter rows in files.
   */
  ThisT ignoreResiduals();

  /**
   * Create a new scan to use a particular executor to plan. The default worker pool will be used by
   * default.
   *
   * @param executorService the provided executor
   * @return a table scan that uses the provided executor to access manifests
   */
  ThisT planWith(ExecutorService executorService);

  /**
   * Returns this scan's projection {@link Schema}.
   *
   * <p>If the projection schema was set directly using {@link #project(Schema)}, returns that
   * schema.
   *
   * <p>If the projection schema was set by calling {@link #select(Collection)}, returns a
   * projection schema that includes the selected data fields and any fields used in the filter
   * expression.
   *
   * @return this scan's projection schema
   */
  Schema schema();

  /**
   * Plan tasks for this scan where each task reads a single file.
   *
   * <p>Use {@link #planTasks()} for planning balanced tasks where each task will read either a
   * single file, a part of a file, or multiple files.
   *
   * @return an Iterable of tasks scanning entire files required by this scan
   */
  CloseableIterable<T> planFiles();

  /**
   * Plan balanced task groups for this scan by splitting large and combining small tasks.
   *
   * <p>Task groups created by this method may read partial input files, multiple input files or
   * both.
   *
   * @return an Iterable of balanced task groups required by this scan
   */
  CloseableIterable<G> planTasks();

  /** Returns the target split size for this scan. */
  long targetSplitSize();

  /** Returns the split lookback for this scan. */
  int splitLookback();

  /** Returns the split open file cost for this scan. */
  long splitOpenFileCost();

  /**
   * Create a new scan that will report scan metrics to the provided reporter in addition to
   * reporters maintained by the scan.
   */
  default ThisT metricsReporter(MetricsReporter reporter) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " doesn't implement metricsReporter");
  }
}
