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

abstract class BaseMetadataTableScan extends BaseTableScan {

  private final MetadataTableType tableType;

  protected BaseMetadataTableScan(Table table, Schema schema, MetadataTableType tableType) {
    super(table, schema, TableScanContext.empty());
    this.tableType = tableType;
  }

  protected BaseMetadataTableScan(
      Table table, Schema schema, MetadataTableType tableType, TableScanContext context) {
    super(table, schema, context);
    this.tableType = tableType;
  }

  /**
   * Type of scan being performed, such as {@link MetadataTableType#ALL_DATA_FILES} when scanning a
   * table's {@link org.apache.iceberg.AllDataFilesTable}.
   *
   * <p>Used for logging and error messages.
   */
  protected MetadataTableType tableType() {
    return tableType;
  }

  @Override
  public TableScan appendsBetween(long fromSnapshotId, long toSnapshotId) {
    throw new UnsupportedOperationException(
        String.format("Cannot incrementally scan table of type %s", tableType()));
  }

  @Override
  public TableScan appendsAfter(long fromSnapshotId) {
    throw new UnsupportedOperationException(
        String.format("Cannot incrementally scan table of type %s", tableType()));
  }

  @Override
  public long targetSplitSize() {
    long tableValue =
        ((BaseTable) table())
            .operations()
            .current()
            .propertyAsLong(
                TableProperties.METADATA_SPLIT_SIZE, TableProperties.METADATA_SPLIT_SIZE_DEFAULT);
    return PropertyUtil.propertyAsLong(options(), TableProperties.SPLIT_SIZE, tableValue);
  }
}
