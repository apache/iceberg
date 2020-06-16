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

import java.util.function.Function;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;

class StaticTableScan extends BaseTableScan {
  private final Function<StaticTableScan, DataTask> buildTask;

  StaticTableScan(TableOperations ops, Table table, Schema schema, Function<StaticTableScan, DataTask> buildTask) {
    super(ops, table, schema);
    this.buildTask = buildTask;
  }

  private StaticTableScan(TableOperations ops, Table table, Schema schema,
                          Function<StaticTableScan, DataTask> buildTask, TableScanContext context) {
    super(ops, table, schema, context);
    this.buildTask = buildTask;
  }

  @Override
  protected long targetSplitSize(TableOperations ops) {
    return ops.current().propertyAsLong(
        TableProperties.METADATA_SPLIT_SIZE, TableProperties.METADATA_SPLIT_SIZE_DEFAULT);
  }

  @Override
  protected TableScan newRefinedScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
    return new StaticTableScan(
        ops, table, schema, buildTask, context);
  }

  @Override
  protected CloseableIterable<FileScanTask> planFiles(
      TableOperations ops, Snapshot snapshot, Expression rowFilter,
      boolean ignoreResiduals, boolean caseSensitive, boolean colStats) {
    return CloseableIterable.withNoopClose(buildTask.apply(this));
  }
}
