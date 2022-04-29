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

/**
 * API for configuring an incremental table scan for appends only snapshots
 */
public interface IncrementalAppendScan extends Scan<IncrementalAppendScan> {

  /**
   * Refine the incremental scan with the start snapshot inclusive.
   * <p>
   * If the start snapshot (inclusive or exclusive) is not provided,
   * the oldest snapshot will be used as the start snapshot.
   * Oldest snapshot is a snapshot whose parent-snapshot-id is null.
   * If there are multiple snapshots has parent-snapshot-id as null
   * (e.g. due to disjointed lineage history), snapshot timestamp will
   * be used as a tiebreaker.
   *
   * @param fromSnapshotId the start snapshot id inclusive
   * @return an incremental table scan from {@code fromSnapshotId} inclusive
   * @throws IllegalArgumentException if the start snapshot is not an ancestor
   * of the end snapshot
   */
  IncrementalAppendScan fromSnapshotInclusive(long fromSnapshotId);

  /**
   * Refine the incremental scan with the start snapshot exclusive.
   * <p>
   * If the start snapshot (inclusive or exclusive) is not provided,
   * the oldest snapshot will be used as the start snapshot.
   * Oldest snapshot is a snapshot whose parent-snapshot-id is null.
   * If there are multiple snapshots has parent-snapshot-id as null
   * (e.g. due to disjointed lineage history), snapshot timestamp will
   * be used as a tiebreaker.
   *
   * @param fromSnapshotId the start snapshot id (exclusive)
   * @return an incremental table scan from {@code fromSnapshotId} exclusive
   * @throws IllegalArgumentException if the start snapshot is not an ancestor
   * of the end snapshot
   */
  IncrementalAppendScan fromSnapshotExclusive(long fromSnapshotId);

  /**
   * Refine the incremental scan with the end snapshot inclusive.
   * <p>
   * If the end snapshot is not provided, the current table snapshot will be used.
   *
   * @param toSnapshotId the end snapshot id (inclusive)
   * @return an incremental table scan up to {@code toSnapshotId} inclusive
   */
  IncrementalAppendScan toSnapshot(long toSnapshotId);
}
