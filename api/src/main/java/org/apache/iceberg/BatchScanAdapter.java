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

/** An adapter that allows using {@link TableScan} as {@link BatchScan}. */
class BatchScanAdapter implements BatchScan {

  private final TableScan scan;

  BatchScanAdapter(TableScan scan) {
    this.scan = scan;
  }

  @Override
  public Table table() {
    return scan.table();
  }

  @Override
  public BatchScan useSnapshot(long snapshotId) {
    return new BatchScanAdapter(scan.useSnapshot(snapshotId));
  }

  @Override
  public BatchScan useRef(String ref) {
    return new BatchScanAdapter(scan.useRef(ref));
  }

  @Override
  public BatchScan asOfTime(long timestampMillis) {
    return new BatchScanAdapter(scan.asOfTime(timestampMillis));
  }

  @Override
  public Snapshot snapshot() {
    return scan.snapshot();
  }

  @Override
  public BatchScan option(String property, String value) {
    return new BatchScanAdapter(scan.option(property, value));
  }

  @Override
  public BatchScan project(Schema schema) {
    return new BatchScanAdapter(scan.project(schema));
  }

  @Override
  public BatchScan caseSensitive(boolean caseSensitive) {
    return new BatchScanAdapter(scan.caseSensitive(caseSensitive));
  }

  @Override
  public boolean isCaseSensitive() {
    return scan.isCaseSensitive();
  }

  @Override
  public BatchScan includeColumnStats() {
    return new BatchScanAdapter(scan.includeColumnStats());
  }

  @Override
  public BatchScan select(Collection<String> columns) {
    return new BatchScanAdapter(scan.select(columns));
  }

  @Override
  public BatchScan filter(Expression expr) {
    return new BatchScanAdapter(scan.filter(expr));
  }

  @Override
  public Expression filter() {
    return scan.filter();
  }

  @Override
  public BatchScan ignoreResiduals() {
    return new BatchScanAdapter(scan.ignoreResiduals());
  }

  @Override
  public BatchScan planWith(ExecutorService executorService) {
    return new BatchScanAdapter(scan.planWith(executorService));
  }

  @Override
  public Schema schema() {
    return scan.schema();
  }

  @SuppressWarnings("unchecked")
  @Override
  public CloseableIterable<ScanTask> planFiles() {
    CloseableIterable<? extends ScanTask> tasks = scan.planFiles();
    return (CloseableIterable<ScanTask>) tasks;
  }

  @SuppressWarnings("unchecked")
  @Override
  public CloseableIterable<ScanTaskGroup<ScanTask>> planTasks() {
    CloseableIterable<? extends ScanTaskGroup<? extends ScanTask>> taskGroups = scan.planTasks();
    return (CloseableIterable<ScanTaskGroup<ScanTask>>) taskGroups;
  }

  @Override
  public long targetSplitSize() {
    return scan.targetSplitSize();
  }

  @Override
  public int splitLookback() {
    return scan.splitLookback();
  }

  @Override
  public long splitOpenFileCost() {
    return scan.splitOpenFileCost();
  }

  @Override
  public BatchScan metricsReporter(MetricsReporter reporter) {
    return new BatchScanAdapter(scan.metricsReporter(reporter));
  }
}
