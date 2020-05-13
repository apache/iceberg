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

import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class BaseAllMetadataTableScan extends BaseTableScan {
  private static final Logger LOG = LoggerFactory.getLogger(BaseAllMetadataTableScan.class);

  BaseAllMetadataTableScan(TableOperations ops, Table table, Schema fileSchema) {
    super(ops, table, fileSchema);
  }

  BaseAllMetadataTableScan(
      TableOperations ops, Table table, Long snapshotId, Schema schema, Expression rowFilter,
      boolean caseSensitive, boolean colStats, Collection<String> selectedColumns,
      ImmutableMap<String, String> options) {
    super(ops, table, snapshotId, schema, rowFilter, caseSensitive, colStats, selectedColumns, options);
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles() {
    LOG.info("Scanning metadata table {} with filter {}.", table(), filter());
    Listeners.notifyAll(new ScanEvent(table().toString(), 0L, filter(), schema()));

    return planFiles(tableOps(), snapshot(), filter(), isCaseSensitive(), colStats());
  }
}
