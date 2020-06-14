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

  Expression rowFilter() {
    return rowFilter;
  }

  boolean ignoreResiduals() {
    return ignoreResiduals;
  }

  boolean caseSensitive() {
    return caseSensitive;
  }

  boolean colStats() {
    return colStats;
  }

  Collection<String> selectedColumns() {
    return selectedColumns;
  }

  ImmutableMap<String, String> options() {
    return options;
  }

  Long fromSnapshotId() {
    return fromSnapshotId;
  }

  Long toSnapshotId() {
    return toSnapshotId;
  }

  TableScanContext copy() {
    return TableScanContext.builder(this).build();
  }

  static Builder builder() {
    return new Builder();
  }

  static Builder builder(TableScanContext other) {
    return new Builder(other);
  }

  static class Builder {
    private Long snapshotId;
    private Expression rowFilter;
    private boolean ignoreResiduals;
    private boolean caseSensitive;
    private boolean colStats;
    private Collection<String> selectedColumns;
    private ImmutableMap<String, String> options;
    private Long fromSnapshotId;
    private Long toSnapshotId;

    private Builder() {
      this.snapshotId = null;
      this.rowFilter = Expressions.alwaysTrue();
      this.ignoreResiduals = false;
      this.caseSensitive = false;
      this.colStats = false;
      this.selectedColumns = null;
      this.options = ImmutableMap.of();
      this.fromSnapshotId = null;
      this.toSnapshotId = null;
    }

    private Builder(TableScanContext other) {
      this.snapshotId = other.snapshotId;
      this.rowFilter = other.rowFilter;
      this.ignoreResiduals = other.ignoreResiduals;
      this.caseSensitive = other.caseSensitive;
      this.colStats = other.colStats;
      this.selectedColumns = other.selectedColumns;
      this.options = ImmutableMap.copyOf(other.options);
      this.fromSnapshotId = other.fromSnapshotId;
      this.toSnapshotId = other.toSnapshotId;
    }

    Builder snapshotId(Long id) {
      this.snapshotId = id;
      return this;
    }

    Builder rowFilter(Expression filter) {
      this.rowFilter = filter;
      return this;
    }

    Builder ignoreResiduals(boolean shouldIgnoreResiduals) {
      this.ignoreResiduals = shouldIgnoreResiduals;
      return this;
    }

    Builder caseSensitive(boolean isCaseSensitive) {
      this.caseSensitive = isCaseSensitive;
      return this;
    }

    Builder colStats(boolean shouldUseColumnStats) {
      this.colStats = shouldUseColumnStats;
      return this;
    }

    Builder selectedColumns(Collection<String> columns) {
      this.selectedColumns = columns;
      return this;
    }

    Builder options(Map<String, String> extraOptions) {
      this.options = ImmutableMap.copyOf(extraOptions);
      return this;
    }

    Builder fromSnapshotId(Long id) {
      this.fromSnapshotId = id;
      return this;
    }

    Builder toSnapshotId(Long id) {
      this.toSnapshotId = id;
      return this;
    }

    TableScanContext build() {
      return new TableScanContext(snapshotId, rowFilter, ignoreResiduals, caseSensitive, colStats,
          selectedColumns, options, fromSnapshotId, toSnapshotId);
    }
  }
}
