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

import org.apache.iceberg.io.CloseableIterable;

/** A {@link BatchScan} implementation that returns empty results. */
public class EmptyBatchScan extends BaseScan<BatchScan, ScanTask, ScanTaskGroup<ScanTask>>
    implements BatchScan {

  public EmptyBatchScan(Table table) {
    super(table, table.schema(), TableScanContext.empty());
  }

  protected EmptyBatchScan(Table table, Schema schema, TableScanContext context) {
    super(table, schema, context);
  }

  @Override
  public Table table() {
    return super.table();
  }

  @Override
  public Snapshot snapshot() {
    return null;
  }

  @Override
  public BatchScan useSnapshot(long snapshotId) {
    throw new UnsupportedOperationException("Cannot time travel in " + getClass().getName());
  }

  @Override
  public BatchScan useRef(String ref) {
    throw new UnsupportedOperationException("Cannot select ref in " + getClass().getName());
  }

  @Override
  public BatchScan asOfTime(long timestampMillis) {
    throw new UnsupportedOperationException("Cannot time travel in " + getClass().getName());
  }

  @Override
  public CloseableIterable<ScanTask> planFiles() {
    return CloseableIterable.empty();
  }

  @Override
  public CloseableIterable<ScanTaskGroup<ScanTask>> planTasks() {
    return CloseableIterable.empty();
  }

  @Override
  protected BatchScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
    return new EmptyBatchScan(table, schema, context);
  }
}
