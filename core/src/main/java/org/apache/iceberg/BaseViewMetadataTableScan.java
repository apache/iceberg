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

import org.apache.iceberg.util.PropertyUtil;

abstract class BaseViewMetadataTableScan extends BaseTableScan {

  private final ViewMetadataTableType tableType;

  protected BaseViewMetadataTableScan(Table table, Schema schema, ViewMetadataTableType tableType) {
    super(table, schema, TableScanContext.empty());
    this.tableType = tableType;
  }

  protected BaseViewMetadataTableScan(
      Table table, Schema schema, ViewMetadataTableType tableType, TableScanContext context) {
    super(table, schema, context);
    this.tableType = tableType;
  }

  @Override
  public TableScan appendsBetween(long fromSnapshotId, long toSnapshotId) {
    throw new UnsupportedOperationException(
        String.format("Cannot incrementally scan table of type %s", tableType));
  }

  @Override
  public TableScan appendsAfter(long fromSnapshotId) {
    throw new UnsupportedOperationException(
        String.format("Cannot incrementally scan table of type %s", tableType));
  }

  @Override
  public long targetSplitSize() {
    return PropertyUtil.propertyAsLong(
        options(), TableProperties.SPLIT_SIZE, TableProperties.METADATA_SPLIT_SIZE_DEFAULT);
  }
}
