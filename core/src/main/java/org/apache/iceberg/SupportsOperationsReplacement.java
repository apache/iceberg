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
 * Internal SPI for a table that can copy itself with replacement operations while preserving its
 * concrete type.
 *
 * <p>{@link CachingCatalog} uses this capability to observe commits without mutating a table
 * instance owned by the wrapped catalog. Tables that do not support operations replacement retain
 * the existing caching behavior without commit tracking. Implementations that violate this contract
 * are returned normally but are not cached. This interface is internal and may change without
 * notice.
 */
public interface SupportsOperationsReplacement extends HasTableOperations {
  /**
   * Returns a copy of this table backed by the given operations.
   *
   * <p>This table must not be modified. The returned table must be a different instance with the
   * same concrete type, and its {@link #operations()} method must return {@code newOperations}.
   * Implementations must only construct the copy; this method is invoked while a cache entry is
   * being computed and must not call back into the catalog or perform table operations.
   *
   * @param newOperations operations for the returned table
   * @return a copy backed by {@code newOperations}
   */
  Table withOperations(TableOperations newOperations);
}
