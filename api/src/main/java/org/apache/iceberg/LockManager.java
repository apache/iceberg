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

import java.util.Map;

/** An interface for locking, used to ensure commit isolation. */
public interface LockManager extends AutoCloseable {

  /**
   * Try to acquire a lock
   *
   * @param entityId ID of the entity to lock
   * @param ownerId ID of the owner if the lock
   * @return if the lock for the entity is acquired by the owner
   */
  boolean acquire(String entityId, String ownerId);

  /**
   * Release a lock
   *
   * <p>exception must not be thrown for this method.
   *
   * @param entityId ID of the entity to lock
   * @param ownerId ID of the owner if the lock
   * @return if the owner held the lock and successfully released it.
   */
  boolean release(String entityId, String ownerId);

  /**
   * Initialize lock manager from catalog properties.
   *
   * @param properties catalog properties
   */
  void initialize(Map<String, String> properties);
}
