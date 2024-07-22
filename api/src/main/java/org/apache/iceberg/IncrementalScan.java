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

/** API for configuring an incremental scan. */
public interface IncrementalScan<ThisT extends Scan, T extends ScanTask, G extends ScanTaskGroup<T>>
    extends Scan<ThisT, T, G> {
  /**
   * Instructs this scan to look for changes starting from a particular snapshot (inclusive).
   *
   * <p>If the start snapshot is not configured, it defaults to the oldest ancestor of the end
   * snapshot (inclusive).
   *
   * @param fromSnapshotId the start snapshot ID (inclusive)
   * @return this for method chaining
   * @throws IllegalArgumentException if the start snapshot is not an ancestor of the end snapshot
   */
  ThisT fromSnapshotInclusive(long fromSnapshotId);

  /**
   * Instructs this scan to look for changes starting from a particular snapshot (inclusive).
   *
   * <p>If the start snapshot is not configured, it defaults to the oldest ancestor of the end
   * snapshot (inclusive).
   *
   * @param ref the start ref name that points to a particular snapshot ID (inclusive)
   * @return this for method chaining
   * @throws IllegalArgumentException if the start snapshot is not an ancestor of the end snapshot
   */
  default ThisT fromSnapshotInclusive(String ref) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " doesn't implement fromSnapshotInclusive");
  }

  /**
   * Instructs this scan to look for changes starting from a particular snapshot (exclusive).
   *
   * <p>If the start snapshot is not configured, it defaults to the oldest ancestor of the end
   * snapshot (inclusive).
   *
   * @param fromSnapshotId the start snapshot ID (exclusive)
   * @return this for method chaining
   * @throws IllegalArgumentException if the start snapshot is not an ancestor of the end snapshot
   */
  ThisT fromSnapshotExclusive(long fromSnapshotId);

  /**
   * Instructs this scan to look for changes starting from a particular snapshot (exclusive).
   *
   * <p>If the start snapshot is not configured, it defaults to the oldest ancestor of the end
   * snapshot (inclusive).
   *
   * @param ref the start ref name that points to a particular snapshot ID (exclusive)
   * @return this for method chaining
   * @throws IllegalArgumentException if the start snapshot is not an ancestor of the end snapshot
   */
  default ThisT fromSnapshotExclusive(String ref) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " doesn't implement fromSnapshotExclusive");
  }

  /**
   * Instructs this scan to look for changes up to a particular snapshot (inclusive).
   *
   * <p>If the end snapshot is not configured, it defaults to the current table snapshot
   * (inclusive).
   *
   * @param toSnapshotId the end snapshot ID (inclusive)
   * @return this for method chaining
   */
  ThisT toSnapshot(long toSnapshotId);

  /**
   * Instructs this scan to look for changes up to a particular snapshot ref (inclusive).
   *
   * <p>If the end snapshot is not configured, it defaults to the current table snapshot
   * (inclusive).
   *
   * @param ref the end snapshot Ref (inclusive)
   * @return this for method chaining
   */
  default ThisT toSnapshot(String ref) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " doesn't implement toSnapshot");
  }

  /**
   * Use the specified branch
   *
   * @param branch the branch name
   * @return this for method chaining
   */
  default ThisT useBranch(String branch) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " doesn't implement useBranch");
  }
}
