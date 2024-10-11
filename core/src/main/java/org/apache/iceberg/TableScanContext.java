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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.metrics.LoggingMetricsReporter;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.MetricsReporters;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.ThreadPools;
import org.immutables.value.Value;

/** Context object with optional arguments for a TableScan. */
@Value.Immutable
public abstract class TableScanContext {

  @Nullable
  public abstract Long snapshotId();

  @Value.Default
  public Expression rowFilter() {
    return Expressions.alwaysTrue();
  }

  @Value.Default
  public boolean ignoreResiduals() {
    return false;
  }

  @Value.Default
  public boolean caseSensitive() {
    return true;
  }

  @Value.Default
  public boolean returnColumnStats() {
    return false;
  }

  @Nullable
  public abstract Set<Integer> columnsToKeepStats();

  @Nullable
  public abstract Collection<String> selectedColumns();

  @Nullable
  public abstract Schema projectedSchema();

  @Value.Default
  public Map<String, String> options() {
    return ImmutableMap.of();
  }

  @Nullable
  public abstract Long fromSnapshotId();

  @Value.Default
  public boolean fromSnapshotInclusive() {
    return false;
  }

  @Nullable
  public abstract Long toSnapshotId();

  @Value.Default
  public ExecutorService planExecutor() {
    return ThreadPools.getWorkerPool();
  }

  @Value.Derived
  boolean planWithCustomizedExecutor() {
    return !planExecutor().equals(ThreadPools.getWorkerPool());
  }

  @Value.Default
  public MetricsReporter metricsReporter() {
    return LoggingMetricsReporter.instance();
  }

  @Nullable
  public abstract String branch();

  TableScanContext useSnapshotId(Long scanSnapshotId) {
    return ImmutableTableScanContext.builder().from(this).snapshotId(scanSnapshotId).build();
  }

  TableScanContext filterRows(Expression filter) {
    return ImmutableTableScanContext.builder().from(this).rowFilter(filter).build();
  }

  TableScanContext ignoreResiduals(boolean shouldIgnoreResiduals) {
    return ImmutableTableScanContext.builder()
        .from(this)
        .ignoreResiduals(shouldIgnoreResiduals)
        .build();
  }

  TableScanContext setCaseSensitive(boolean isCaseSensitive) {
    return ImmutableTableScanContext.builder().from(this).caseSensitive(isCaseSensitive).build();
  }

  TableScanContext shouldReturnColumnStats(boolean returnColumnStats) {
    return ImmutableTableScanContext.builder()
        .from(this)
        .returnColumnStats(returnColumnStats)
        .build();
  }

  TableScanContext columnsToKeepStats(Set<Integer> columnsToKeepStats) {
    Preconditions.checkState(
        returnColumnStats(),
        "Cannot select columns to keep stats when column stats are not returned");
    return ImmutableTableScanContext.builder()
        .from(this)
        .columnsToKeepStats(columnsToKeepStats)
        .build();
  }

  TableScanContext selectColumns(Collection<String> columns) {
    Preconditions.checkState(
        projectedSchema() == null, "Cannot select columns when projection schema is set");
    return ImmutableTableScanContext.builder().from(this).selectedColumns(columns).build();
  }

  TableScanContext project(Schema schema) {
    Preconditions.checkState(
        selectedColumns() == null, "Cannot set projection schema when columns are selected");
    return ImmutableTableScanContext.builder().from(this).projectedSchema(schema).build();
  }

  TableScanContext withOption(String property, String value) {
    return ImmutableTableScanContext.builder().from(this).putOptions(property, value).build();
  }

  TableScanContext fromSnapshotIdExclusive(long id) {
    return ImmutableTableScanContext.builder()
        .from(this)
        .fromSnapshotId(id)
        .fromSnapshotInclusive(false)
        .build();
  }

  TableScanContext fromSnapshotIdInclusive(long id) {
    return ImmutableTableScanContext.builder()
        .from(this)
        .fromSnapshotId(id)
        .fromSnapshotInclusive(true)
        .build();
  }

  TableScanContext toSnapshotId(long id) {
    return ImmutableTableScanContext.builder().from(this).toSnapshotId(id).build();
  }

  TableScanContext planWith(ExecutorService executor) {
    return ImmutableTableScanContext.builder().from(this).planExecutor(executor).build();
  }

  TableScanContext reportWith(MetricsReporter reporter) {
    return ImmutableTableScanContext.builder()
        .from(this)
        .metricsReporter(
            metricsReporter() instanceof LoggingMetricsReporter
                ? reporter
                : MetricsReporters.combine(metricsReporter(), reporter))
        .build();
  }

  TableScanContext useBranch(String ref) {
    return ImmutableTableScanContext.builder().from(this).branch(ref).build();
  }

  public static TableScanContext empty() {
    return ImmutableTableScanContext.builder().build();
  }
}
