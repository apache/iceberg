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

package org.apache.iceberg.actions;

/**
 * An action that generates a change data set from snapshots.
 */
public interface GenerateChangeSet extends Action<GenerateChangeSet, GenerateChangeSet.Result> {
  /**
   * Emit changed data set by a snapshot id.
   *
   * @param snapshotId id of the snapshot to generate changed data
   * @return this for method chaining
   */
  GenerateChangeSet forSnapshot(long snapshotId);

  /**
   * Emit change data set from the start snapshot (inclusive).
   * <p>
   * If neither {@link #fromSnapshotInclusive(long)} or {@link #fromSnapshotExclusive(long)} is provided,
   * the start snapshot (inclusive) is defaulted to the oldest ancestor of the snapshot history.
   *
   * @param fromSnapshotId id of the start snapshot (inclusive)
   * @return this for method chaining
   */
  GenerateChangeSet fromSnapshotInclusive(long fromSnapshotId);

  /**
   * Emit change data set from the start snapshot (exclusive).
   * <p>
   * If neither {@link #fromSnapshotInclusive(long)} or {@link #fromSnapshotExclusive(long)} is provided,
   * the start snapshot (inclusive) is defaulted to the oldest ancestor of the snapshot history.
   *
   * @param fromSnapshotId id of the start snapshot (exclusive)
   * @return this for method chaining
   */
  GenerateChangeSet fromSnapshotExclusive(long fromSnapshotId);

  /**
   * Emit changed data to a particular snapshot (inclusive).
   * <p>
   * If not provided, end snapshot is defaulted to the table's current snapshot.
   *
   * @param toSnapshotId id of the end snapshot (inclusive)
   * @return this for method chaining
   */
  GenerateChangeSet toSnapshot(long toSnapshotId);

  /**
   * The action result that contains a dataset of changed rows.
   */
  interface Result<T> {
    /**
     * Returns the change set.
     */
    T changeSet();
  }
}
