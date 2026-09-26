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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BatchScan} decorator that constrains {@link #planFiles()} to a fixed set of data file
 * paths, resolved ahead of time from a SCALAR index by {@link SparkScanBuilder}.
 *
 * <p>Only {@link #planFiles()} is overridden. Spark's own task planning ({@link
 * SparkPartitioningAwareScan}) calls only {@link #planFiles()}, never {@link #planTasks()}, so
 * that is the one place this filter needs to apply for a normal query. {@link #planTasks()}
 * delegates unfiltered -- callers that plan through it directly won't see the restriction, but
 * since this is purely an additive, sound restriction (the original predicate is still applied as
 * a residual regardless), an unfiltered {@link #planTasks()} only costs a missed optimization, not
 * a wrong result.
 *
 * <p>{@link #planFiles()} self-verifies before trusting the resolved paths: the index recorded
 * file paths at index-build time via Spark's {@code input_file_name()}, while this class matches
 * them against {@link org.apache.iceberg.DataFile#path()} as reported by normal scan planning --
 * two different code paths that are expected to agree, but have not been verified to always agree
 * across every {@code FileIO} implementation and path-normalization scheme. If none of the
 * resolved paths match any real candidate file (a sign of exactly that kind of mismatch, not a
 * genuine "no files match"), this falls back to the full, unfiltered file list rather than risk
 * silently returning zero rows -- this optimization must never be able to produce a wrong result.
 *
 * <p>Every other method delegates straight through to the wrapped scan. Refinement methods (
 * {@link #filter}, {@link #select}, {@link #useSnapshot}, etc.) are not expected to be called on
 * this decorator in practice -- by the time {@link SparkScanBuilder} wraps a scan with this class,
 * all such refinement has already happened on the underlying scan -- so they delegate without
 * re-wrapping the result.
 */
class FileScanTaskFilteringScan implements BatchScan {

  private static final Logger LOG = LoggerFactory.getLogger(FileScanTaskFilteringScan.class);

  private final BatchScan delegate;
  private final Set<String> allowedFilePaths;

  FileScanTaskFilteringScan(BatchScan delegate, Set<String> allowedFilePaths) {
    Preconditions.checkArgument(
        allowedFilePaths != null && !allowedFilePaths.isEmpty(),
        "allowedFilePaths must be non-empty");
    this.delegate = delegate;
    this.allowedFilePaths = allowedFilePaths;
  }

  @Override
  public CloseableIterable<ScanTask> planFiles() {
    List<ScanTask> allTasks = Lists.newArrayList();
    List<ScanTask> matchedTasks = Lists.newArrayList();
    try (CloseableIterable<ScanTask> tasks = delegate.planFiles()) {
      for (ScanTask task : tasks) {
        allTasks.add(task);
        if (task instanceof FileScanTask
            && allowedFilePaths.contains(((FileScanTask) task).file().location())) {
          matchedTasks.add(task);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    if (!allTasks.isEmpty() && matchedTasks.isEmpty()) {
      LOG.warn(
          "SCALAR index resolved file paths {} matched none of the {} candidate file(s) from "
              + "normal planning -- falling back to the unfiltered file set rather than risk "
              + "returning zero rows",
          allowedFilePaths,
          allTasks.size());
      return CloseableIterable.withNoopClose(allTasks);
    }

    return CloseableIterable.withNoopClose(matchedTasks);
  }

  @Override
  public CloseableIterable<ScanTaskGroup<ScanTask>> planTasks() {
    return delegate.planTasks();
  }

  @Override
  public Table table() {
    return delegate.table();
  }

  @Override
  public BatchScan useSnapshot(long snapshotId) {
    return delegate.useSnapshot(snapshotId);
  }

  @Override
  public BatchScan useRef(String ref) {
    return delegate.useRef(ref);
  }

  @Override
  public BatchScan asOfTime(long timestampMillis) {
    return delegate.asOfTime(timestampMillis);
  }

  @Override
  public Snapshot snapshot() {
    return delegate.snapshot();
  }

  @Override
  public BatchScan option(String property, String value) {
    return delegate.option(property, value);
  }

  @Override
  public BatchScan project(Schema schema) {
    return delegate.project(schema);
  }

  @Override
  public BatchScan caseSensitive(boolean caseSensitive) {
    return delegate.caseSensitive(caseSensitive);
  }

  @Override
  public boolean isCaseSensitive() {
    return delegate.isCaseSensitive();
  }

  @Override
  public BatchScan includeColumnStats() {
    return delegate.includeColumnStats();
  }

  @Override
  public BatchScan includeColumnStats(Collection<String> requestedColumns) {
    return delegate.includeColumnStats(requestedColumns);
  }

  @Override
  public BatchScan select(Collection<String> columns) {
    return delegate.select(columns);
  }

  @Override
  public BatchScan filter(Expression expr) {
    return delegate.filter(expr);
  }

  @Override
  public Expression filter() {
    return delegate.filter();
  }

  @Override
  public BatchScan ignoreResiduals() {
    return delegate.ignoreResiduals();
  }

  @Override
  public BatchScan planWith(ExecutorService executorService) {
    return delegate.planWith(executorService);
  }

  @Override
  public Schema schema() {
    return delegate.schema();
  }

  @Override
  public long targetSplitSize() {
    return delegate.targetSplitSize();
  }

  @Override
  public int splitLookback() {
    return delegate.splitLookback();
  }

  @Override
  public long splitOpenFileCost() {
    return delegate.splitOpenFileCost();
  }

  @Override
  public BatchScan metricsReporter(MetricsReporter reporter) {
    return delegate.metricsReporter(reporter);
  }

  @Override
  public BatchScan minRowsRequested(long numRows) {
    return delegate.minRowsRequested(numRows);
  }

  @Override
  public Supplier<FileIO> fileIO() {
    return delegate.fileIO();
  }
}
