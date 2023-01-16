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
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.TableScanUtil;

abstract class BaseInformationSchemaTableScan implements BatchScan {

  private final InformationSchemaTable table;
  private final TableScanContext context;

  protected BaseInformationSchemaTableScan(
      InformationSchemaTable table, TableScanContext context) {
    this.table = table;
    this.context = context;
  }

  protected TableScanContext context() {
    return context;
  }

  protected abstract BatchScan newRefinedScan(TableScanContext newContext);

  @Override
  public Table table() {
    return table;
  }

  @Override
  public Snapshot snapshot() {
    throw new UnsupportedOperationException(
        "Cannot return snapshot for information_schema." + table.typeName());
  }

  @Override
  public BatchScan useSnapshot(long scanSnapshotId) {
    throw new UnsupportedOperationException(
        "Cannot time travel with information_schema." + table.typeName());
  }

  @Override
  public BatchScan asOfTime(long timestampMillis) {
    throw new UnsupportedOperationException(
        "Cannot time travel with information_schema." + table.typeName());
  }

  @Override
  public BatchScan useRef(String name) {
    Preconditions.checkArgument(
        SnapshotRef.MAIN_BRANCH.equalsIgnoreCase(name), "Unknown branch: %s", name);
    return this;
  }

  @Override
  public BatchScan option(String property, String value) {
    return newRefinedScan(context.withOption(property, value));
  }

  @Override
  public BatchScan project(Schema schema) {
    return newRefinedScan(context.project(schema));
  }

  @Override
  public BatchScan caseSensitive(boolean caseSensitive) {
    return newRefinedScan(context.setCaseSensitive(caseSensitive));
  }

  @Override
  public BatchScan select(Collection<String> columns) {
    return newRefinedScan(context.selectColumns(columns));
  }

  @Override
  public BatchScan filter(Expression expr) {
    return newRefinedScan(context.filterRows(Expressions.and(context.rowFilter(), expr)));
  }

  @Override
  public BatchScan includeColumnStats() {
    return newRefinedScan(context.shouldReturnColumnStats(true));
  }

  @Override
  public BatchScan planWith(ExecutorService executorService) {
    return newRefinedScan(context.planWith(executorService));
  }

  @Override
  public boolean isCaseSensitive() {
    return context.caseSensitive();
  }

  @Override
  public BatchScan ignoreResiduals() {
    return newRefinedScan(context.ignoreResiduals(true));
  }

  @Override
  public Expression filter() {
    return context.rowFilter();
  }

  @Override
  public CloseableIterable<ScanTaskGroup<ScanTask>> planTasks() {
    return TableScanUtil.planTaskGroups(planFiles(), targetSplitSize(), splitLookback(), splitOpenFileCost());
  }

  @Override
  public long targetSplitSize() {
    return PropertyUtil.propertyAsLong(
        context.options(), TableProperties.SPLIT_SIZE, TableProperties.METADATA_SPLIT_SIZE_DEFAULT);
  }

  @Override
  public int splitLookback() {
    return PropertyUtil.propertyAsInt(
        context.options(), TableProperties.SPLIT_LOOKBACK, TableProperties.SPLIT_LOOKBACK_DEFAULT);
  }

  @Override
  public long splitOpenFileCost() {
    return PropertyUtil.propertyAsLong(
        context.options(),
        TableProperties.SPLIT_OPEN_FILE_COST,
        TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);
  }
}
