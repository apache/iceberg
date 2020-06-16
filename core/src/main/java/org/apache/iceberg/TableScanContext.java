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
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * Context object with optional arguments for a TableScan.
 */
final class TableScanContext {
  private final Long snapshotId;
  private final Expression rowFilter;
  private final boolean ignoreResiduals;
  private final boolean caseSensitive;
  private final boolean colStats;
  private final Collection<String> selectedColumns;
  private final ImmutableMap<String, String> options;
  private final Long fromSnapshotId;
  private final Long toSnapshotId;

  TableScanContext() {
    this(null, Expressions.alwaysTrue(), false, true, false, null, ImmutableMap.of(), null, null);
  }

  private TableScanContext(Long snapshotId, Expression rowFilter, boolean ignoreResiduals,
                           boolean caseSensitive, boolean colStats, Collection<String> selectedColumns,
                           ImmutableMap<String, String> options, Long fromSnapshotId, Long toSnapshotId) {
    this.snapshotId = snapshotId;
    this.rowFilter = rowFilter;
    this.ignoreResiduals = ignoreResiduals;
    this.caseSensitive = caseSensitive;
    this.colStats = colStats;
    this.selectedColumns = selectedColumns;
    this.options = options;
    this.fromSnapshotId = fromSnapshotId;
    this.toSnapshotId = toSnapshotId;
  }

  Long snapshotId() {
    return snapshotId;
  }

  TableScanContext snapshotId(Long id) {
    return new TableScanContext(id, rowFilter, ignoreResiduals,
        caseSensitive, colStats, selectedColumns, options, fromSnapshotId, toSnapshotId);
  }

  Expression rowFilter() {
    return rowFilter;
  }

  TableScanContext rowFilter(Expression filter) {
    return new TableScanContext(snapshotId, filter, ignoreResiduals,
        caseSensitive, colStats, selectedColumns, options, fromSnapshotId, toSnapshotId);
  }

  boolean ignoreResiduals() {
    return ignoreResiduals;
  }

  TableScanContext ignoreResiduals(boolean shouldIgnoreResiduals) {
    return new TableScanContext(snapshotId, rowFilter, shouldIgnoreResiduals,
        caseSensitive, colStats, selectedColumns, options, fromSnapshotId, toSnapshotId);
  }

  boolean caseSensitive() {
    return caseSensitive;
  }

  TableScanContext caseSensitive(boolean isCaseSensitive) {
    return new TableScanContext(snapshotId, rowFilter, ignoreResiduals,
        isCaseSensitive, colStats, selectedColumns, options, fromSnapshotId, toSnapshotId);
  }

  boolean colStats() {
    return colStats;
  }

  TableScanContext colStats(boolean shouldUseColumnStats) {
    return new TableScanContext(snapshotId, rowFilter, ignoreResiduals,
        caseSensitive, shouldUseColumnStats, selectedColumns, options, fromSnapshotId, toSnapshotId);
  }

  Collection<String> selectedColumns() {
    return selectedColumns;
  }

  TableScanContext selectedColumns(Collection<String> columns) {
    return new TableScanContext(snapshotId, rowFilter, ignoreResiduals,
        caseSensitive, colStats, columns, options, fromSnapshotId, toSnapshotId);
  }

  ImmutableMap<String, String> options() {
    return options;
  }

  TableScanContext options(Map<String, String> extraOptions) {
    return new TableScanContext(snapshotId, rowFilter, ignoreResiduals,
        caseSensitive, colStats, selectedColumns, ImmutableMap.copyOf(extraOptions),
        fromSnapshotId, toSnapshotId);
  }

  Long fromSnapshotId() {
    return fromSnapshotId;
  }

  TableScanContext fromSnapshotId(Long id) {
    return new TableScanContext(snapshotId, rowFilter, ignoreResiduals,
        caseSensitive, colStats, selectedColumns, options, id, toSnapshotId);
  }

  Long toSnapshotId() {
    return toSnapshotId;
  }

  TableScanContext toSnapshotId(Long id) {
    return new TableScanContext(snapshotId, rowFilter, ignoreResiduals,
        caseSensitive, colStats, selectedColumns, options, fromSnapshotId, id);
  }

  TableScanContext copy() {
    return new TableScanContext(snapshotId, rowFilter, ignoreResiduals,
        caseSensitive, colStats, selectedColumns, options, fromSnapshotId, toSnapshotId);
  }
}
