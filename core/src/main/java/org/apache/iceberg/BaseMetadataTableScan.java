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

import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class BaseMetadataTableScan extends BaseTableScan {
  private static final Logger LOG = LoggerFactory.getLogger(BaseMetadataTableScan.class);

  protected BaseMetadataTableScan(TableOperations ops, Table table, Schema schema) {
    super(ops, table, schema);
  }

  protected BaseMetadataTableScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
    super(ops, table, schema, context);
  }

  @Override
  public long targetSplitSize() {
    long tableValue = tableOps().current().propertyAsLong(
        TableProperties.METADATA_SPLIT_SIZE,
        TableProperties.METADATA_SPLIT_SIZE_DEFAULT);
    return PropertyUtil.propertyAsLong(options(), TableProperties.SPLIT_SIZE, tableValue);
  }

  /**
   * Alternative to {@link #planFiles()}, allows exploring old snapshots even for an empty table.
   */
  protected CloseableIterable<FileScanTask> planFilesAllSnapshots() {
    LOG.info("Scanning metadata table {} with filter {}.", table(), filter());
    Listeners.notifyAll(new ScanEvent(table().name(), 0L, filter(), schema()));

    return planFiles(tableOps(), snapshot(), filter(), shouldIgnoreResiduals(), isCaseSensitive(), colStats());
  }
}
