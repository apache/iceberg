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

public interface GenerateChangeSet extends Action<GenerateChangeSet, GenerateChangeSet.Result> {
  /**
   * Emit changed data set by a snapshot id.
   *
   * @param snapshotId id of the snapshot to generate changed data
   * @return this for method chaining
   */
  GenerateChangeSet forSnapshot(long snapshotId);

  /**
   * Emit changed data set for the current snapshot.
   *
   * @return this for method chaining
   */
  GenerateChangeSet forCurrentSnapshot();

  /**
   * Emit changed data from a particular snapshot(exclusive).
   *
   * @param fromSnapshotId id of the start snapshot
   * @return this for method chaining
   */
  GenerateChangeSet afterSnapshot(long fromSnapshotId);

  /**
   * Emit change data set from the start snapshot (exclusive) to the end snapshot (inclusive).
   *
   * @param fromSnapshotId id of the start snapshot
   * @param toSnapshotId   id of the end snapshot
   * @return this for method chaining
   */
  GenerateChangeSet betweenSnapshots(long fromSnapshotId, long toSnapshotId);

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
