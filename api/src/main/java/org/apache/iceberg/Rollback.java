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

import org.apache.iceberg.exceptions.CommitFailedException;

/**
 * API for rolling table data back to the state at an older table {@link Snapshot snapshot}.
 *
 * <p>This API does not allow conflicting calls to {@link #toSnapshotId(long)} and {@link
 * #toSnapshotAtTime(long)}.
 *
 * <p>When committing, these changes will be applied to the current table metadata. Commit conflicts
 * will not be resolved and will result in a {@link CommitFailedException}.
 */
public interface Rollback extends PendingUpdate<Snapshot> {

  /**
   * Roll this table's data back to a specific {@link Snapshot} identified by id.
   *
   * @param snapshotId long id of the snapshot to roll back table data to
   * @return this for method chaining
   * @throws IllegalArgumentException If the table has no snapshot with the given id
   * @deprecated Replaced by {@link ManageSnapshots#setCurrentSnapshot(long)}
   */
  @Deprecated
  Rollback toSnapshotId(long snapshotId);

  /**
   * Roll this table's data back to the last {@link Snapshot} before the given timestamp.
   *
   * @param timestampMillis a long timestamp, as returned by {@link System#currentTimeMillis()}
   * @return this for method chaining
   * @throws IllegalArgumentException If the table has no old snapshot before the given timestamp
   * @deprecated Replaced by {@link ManageSnapshots#rollbackToTime(long)}
   */
  @Deprecated
  Rollback toSnapshotAtTime(long timestampMillis);
}
