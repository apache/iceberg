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

import java.util.Set;

/**
 * A builder interface for removing IndexSnapshots from an index.
 *
 * @param <T> the concrete builder type for method chaining
 */
public interface RemoveSnapshotsBuilder<T> {

  /**
   * Add a snapshot to remove by its index snapshot ID.
   *
   * @param indexSnapshotId the index snapshot ID to remove
   * @return this for method chaining
   */
  T removeSnapshotById(long indexSnapshotId);

  /**
   * Add multiple snapshots to remove by their index snapshot IDs.
   *
   * @param indexSnapshotIds the index snapshot IDs to remove
   * @return this for method chaining
   */
  T removeSnapshotsByIds(Set<Long> indexSnapshotIds);

  /**
   * Add multiple snapshots to remove by their index snapshot IDs.
   *
   * @param indexSnapshotIds the index snapshot IDs to remove
   * @return this for method chaining
   */
  T removeSnapshotsByIds(long... indexSnapshotIds);
}
