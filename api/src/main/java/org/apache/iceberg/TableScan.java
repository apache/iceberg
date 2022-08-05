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

import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/** API for configuring a table scan. */
public interface TableScan extends Scan<TableScan, FileScanTask, CombinedScanTask> {
  /**
   * Returns the {@link Table} from which this scan loads data.
   *
   * @return this scan's table
   */
  Table table();

  /**
   * Create a new {@link TableScan} from this scan's configuration that will use the given snapshot
   * by ID.
   *
   * @param snapshotId a snapshot ID
   * @return a new scan based on this with the given snapshot ID
   * @throws IllegalArgumentException if the snapshot cannot be found
   */
  TableScan useSnapshot(long snapshotId);

  /**
   * Create a new {@link TableScan} from this scan's configuration that will use the most recent
   * snapshot as of the given time in milliseconds.
   *
   * @param timestampMillis a timestamp in milliseconds.
   * @return a new scan based on this with the current snapshot at the given time
   * @throws IllegalArgumentException if the snapshot cannot be found
   */
  TableScan asOfTime(long timestampMillis);

  /**
   * Create a new {@link TableScan} from this scan's configuration that will use the given snapshot
   * by ID.
   *
   * @param snapshotRef a snapshot Ref
   * @return a new scan based on this with the given snapshot Ref
   * @throws IllegalArgumentException if the snapshot cannot be found
   */
  TableScan useSnapshotRef(String snapshotRef);

  /**
   * Create a new {@link TableScan} from this that will read the given data columns. This produces
   * an expected schema that includes all fields that are either selected or used by this scan's
   * filter expression.
   *
   * @param columns column names from the table's schema
   * @return a new scan based on this with the given projection columns
   */
  default TableScan select(String... columns) {
    return select(Lists.newArrayList(columns));
  }

  /**
   * Create a new {@link TableScan} to read appended data from {@code fromSnapshotId} exclusive to
   * {@code toSnapshotId} inclusive.
   *
   * @param fromSnapshotId the last snapshot id read by the user, exclusive
   * @param toSnapshotId read append data up to this snapshot id
   * @return a table scan which can read append data from {@code fromSnapshotId} exclusive and up to
   *     {@code toSnapshotId} inclusive
   */
  default TableScan appendsBetween(long fromSnapshotId, long toSnapshotId) {
    throw new UnsupportedOperationException("Incremental scan is not supported");
  }

  /**
   * Create a new {@link TableScan} to read appended data from {@code fromSnapshotId} exclusive to
   * the current snapshot inclusive.
   *
   * @param fromSnapshotId - the last snapshot id read by the user, exclusive
   * @return a table scan which can read append data from {@code fromSnapshotId} exclusive and up to
   *     current snapshot inclusive
   */
  default TableScan appendsAfter(long fromSnapshotId) {
    throw new UnsupportedOperationException("Incremental scan is not supported");
  }

  /**
   * Returns the {@link Snapshot} that will be used by this scan.
   *
   * <p>If the snapshot was not configured using {@link #asOfTime(long)} or {@link
   * #useSnapshot(long)}, the current table snapshot will be used.
   *
   * @return the Snapshot this scan will use
   */
  Snapshot snapshot();
}
