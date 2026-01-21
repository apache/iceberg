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
package org.apache.iceberg.index;

import java.util.List;
import java.util.Set;
import org.apache.iceberg.PendingUpdate;

/**
 * A builder interface for removing {@link IndexSnapshot} instances from an index.
 *
 * <p>This API accumulates snapshot deletions and commits the new list to the index metadata.
 *
 * <p>When committing, these changes will be applied to the current index metadata. Commit conflicts
 * will be resolved by applying the pending changes to the new index metadata.
 *
 * <p>{@link #apply()} returns a list of the snapshots that will be removed.
 */
public interface RemoveIndexSnapshots extends PendingUpdate<List<IndexSnapshot>> {

  /**
   * Add a snapshot to remove by its index snapshot ID.
   *
   * @param indexSnapshotId the index snapshot ID to remove
   * @return this for method chaining
   */
  RemoveIndexSnapshots removeSnapshotById(long indexSnapshotId);

  /**
   * Add multiple snapshots to remove by their index snapshot IDs.
   *
   * @param indexSnapshotIds the index snapshot IDs to remove
   * @return this for method chaining
   */
  RemoveIndexSnapshots removeSnapshotsByIds(Set<Long> indexSnapshotIds);

  /**
   * Add multiple snapshots to remove by their index snapshot IDs.
   *
   * @param indexSnapshotIds the index snapshot IDs to remove
   * @return this for method chaining
   */
  RemoveIndexSnapshots removeSnapshotsByIds(long... indexSnapshotIds);
}
