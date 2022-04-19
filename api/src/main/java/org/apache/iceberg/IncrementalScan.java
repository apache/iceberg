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
 * API for configuring an incremental table scan
 */
public interface IncrementalScan extends Scan<IncrementalScan> {

  /**
   * Optional. if not set, null value will be used for the start snapshot id.
   * That would include the oldest ancestor of the {@link IncrementalScan#toSnapshotId(long)},
   * as its parent snapshot id is null which matches the null start snapshot id
   *
   * @param fromSnapshotId the start snapshot id (exclusive)
   * @return an incremental table scan from {@code fromSnapshotId} exclusive
   */
  IncrementalScan fromSnapshotId(long fromSnapshotId);

  /**
   * Optional. if not set, current table snapshot id is used as the end snapshot id
   *
   * @param toSnapshotId the end snapshot id (inclusive)
   * @return an incremental table scan up to {@code toSnapshotId} inclusive
   */
  IncrementalScan toSnapshotId(long toSnapshotId);

  /**
   * Only interested in snapshots with append operation
   */
  IncrementalScan appendsOnly();

  /**
   * Ignore snapshots with overwrite operation.
   *
   * Default behavior for incremental scan fails if there are overwrite operations in the incremental snapshot range
   */
  IncrementalScan ignoreOverwrites();
}
