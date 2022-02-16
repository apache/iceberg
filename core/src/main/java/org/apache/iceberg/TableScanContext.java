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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
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
  private final Schema projectedSchema;
  private final Collection<String> selectedColumns;
  private final ImmutableMap<String, String> options;
  private final Long fromSnapshotId;
  private final Long toSnapshotId;

  TableScanContext() {
    this.snapshotId = null;
    this.rowFilter = Expressions.alwaysTrue();
    this.ignoreResiduals = false;
    this.caseSensitive = true;
    this.colStats = false;
    this.projectedSchema = null;
    this.selectedColumns = null;
    this.options = ImmutableMap.of();
    this.fromSnapshotId = null;
    this.toSnapshotId = null;
  }

  private TableScanContext(
      Long snapshotId,
      Expression rowFilter,
      boolean ignoreResiduals,
      boolean caseSensitive,
      boolean colStats,
      Schema projectedSchema,
      Collection<String> selectedColumns,
      ImmutableMap<String, String> options,
      Long fromSnapshotId,
      Long toSnapshotId) {

    this.snapshotId = snapshotId;
    this.rowFilter = rowFilter;
    this.ignoreResiduals = ignoreResiduals;
    this.caseSensitive = caseSensitive;
    this.colStats = colStats;
    this.projectedSchema = projectedSchema;
    this.selectedColumns = selectedColumns;
    this.options = options;
    this.fromSnapshotId = fromSnapshotId;
    this.toSnapshotId = toSnapshotId;
  }

  Long snapshotId() {
    return snapshotId;
  }

  TableScanContext useSnapshotId(Long scanSnapshotId) {
    return builder().snapshotId(scanSnapshotId).build();
  }

  Expression rowFilter() {
    return rowFilter;
  }

  TableScanContext filterRows(Expression filter) {
    return builder().rowFilter(filter).build();
  }

  boolean ignoreResiduals() {
    return ignoreResiduals;
  }

  TableScanContext ignoreResiduals(boolean shouldIgnoreResiduals) {
    return builder().ignoreResiduals(shouldIgnoreResiduals).build();
  }

  boolean caseSensitive() {
    return caseSensitive;
  }

  TableScanContext setCaseSensitive(boolean isCaseSensitive) {
    return builder().caseSensitive(isCaseSensitive).build();
  }

  boolean returnColumnStats() {
    return colStats;
  }

  TableScanContext shouldReturnColumnStats(boolean returnColumnStats) {
    return builder().colStats(returnColumnStats).build();
  }

  Collection<String> selectedColumns() {
    return selectedColumns;
  }

  TableScanContext selectColumns(Collection<String> columns) {
    Preconditions.checkState(projectedSchema == null, "Cannot select columns when projection schema is set");
    return builder().projectedSchema(null).selectedColumns(columns).build();
  }

  Schema projectedSchema() {
    return projectedSchema;
  }

  TableScanContext project(Schema schema) {
    Preconditions.checkState(selectedColumns == null, "Cannot set projection schema when columns are selected");
    return builder().projectedSchema(schema).selectedColumns(null).build();
  }

  Map<String, String> options() {
    return options;
  }

  TableScanContext withOption(String property, String value) {
    ImmutableMap.Builder<String, String> mapBuilder = ImmutableMap.builder();
    mapBuilder.putAll(options);
    mapBuilder.put(property, value);
    return builder().options(mapBuilder.build()).build();
  }

  Long fromSnapshotId() {
    return fromSnapshotId;
  }

  TableScanContext fromSnapshotId(long id) {
    return builder().fromSnapshotId(id).build();
  }

  Long toSnapshotId() {
    return toSnapshotId;
  }

  TableScanContext toSnapshotId(long id) {
    return builder().toSnapshotId(id).build();
  }

  private Builder builder() {
    return new Builder()
        .snapshotId(snapshotId)
        .rowFilter(rowFilter)
        .ignoreResiduals(ignoreResiduals)
        .caseSensitive(caseSensitive)
        .colStats(colStats)
        .projectedSchema(projectedSchema)
        .selectedColumns(selectedColumns)
        .options(options)
        .fromSnapshotId(fromSnapshotId)
        .toSnapshotId(toSnapshotId);
  }

  private static class Builder {
    private Long snapshotId;
    private Expression rowFilter;
    private boolean ignoreResiduals;
    private boolean caseSensitive;
    private boolean colStats;
    private Schema projectedSchema;
    private Collection<String> selectedColumns;
    private ImmutableMap<String, String> options;
    private Long fromSnapshotId;
    private Long toSnapshotId;

    Builder snapshotId(Long scanSnapshotId) {
      this.snapshotId = scanSnapshotId;
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

    Builder colStats(boolean returnColumnStats) {
      this.colStats = returnColumnStats;
      return this;
    }

    Builder projectedSchema(Schema schema) {
      this.projectedSchema = schema;
      return this;
    }

    Builder selectedColumns(Collection<String> columns) {
      this.selectedColumns = columns;
      return this;
    }

    Builder options(ImmutableMap<String, String> optionsMap) {
      this.options = optionsMap;
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
      return new TableScanContext(
          snapshotId,
          rowFilter,
          ignoreResiduals,
          caseSensitive,
          colStats,
          projectedSchema,
          selectedColumns,
          options,
          fromSnapshotId,
          toSnapshotId);
    }
  }
}
