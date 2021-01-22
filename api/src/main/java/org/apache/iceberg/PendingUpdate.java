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
import org.apache.iceberg.exceptions.ValidationException;

/**
 * API for table metadata changes.
 *
 * @param <T> Java class of changes from this update; returned by {@link #apply} for validation.
 */
public interface PendingUpdate<T> {

  /**
   * Apply the pending changes and return the uncommitted changes for validation.
   * <p>
   * This does not result in a permanent update.
   *
   * @return the uncommitted changes that would be committed by calling {@link #commit()}
   * @throws ValidationException If the pending changes cannot be applied to the current metadata
   * @throws IllegalArgumentException If the pending changes are conflicting or invalid
   */
  T apply();

  /**
   * Apply the pending changes and commit.
   * <p>
   * Changes are committed by calling the underlying table's commit method.
   * <p>
   * Once the commit is successful, the updated table will be refreshed.
   *
   * @throws ValidationException If the update cannot be applied to the current table metadata.
   * @throws CommitFailedException If the update cannot be committed due to conflicts.
   */
  void commit();

  /**
   * Generates update event to notify about metadata changes
   * @return the generated event
   */
  default Object updateEvent() {
    return null;
  }
}
