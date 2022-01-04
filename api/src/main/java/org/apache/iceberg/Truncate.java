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
 * API for removing all data from a table.
 * <p>
 * This API removes all data, produces a new {@link Snapshot} of the table, and commits
 * that snapshot as the current.
 * <p>
 * When committing, these changes will be applied to the latest table snapshot. Commit conflicts
 * will be resolved by applying the changes to the new latest snapshot and reattempting the commit.
 */
public interface Truncate extends PendingUpdate<Snapshot> {

  /**
   * Truncate a table and keep the historical snapshots.
   * <p>
   * If fastMode is set to true, the implementation will directly commit an empty snapshot to table.
   * Otherwise, all data will be deleted and committed.
   *
   * @param fastMode if true, an empty snapshot will be committed to table.
   * @return this for method chaining
   */
  Truncate truncate(boolean fastMode);
}
