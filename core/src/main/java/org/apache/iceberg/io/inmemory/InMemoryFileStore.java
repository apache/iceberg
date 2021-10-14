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

package org.apache.iceberg.io.inmemory;

import java.util.Collection;
import java.util.Optional;

/**
 * An interface for in-memory collection-based key-value storage
 * for byte arrays keyed by string location.
 * The string location can be optionally converted to a custom
 * canonical location specified by the user.
 */
public interface InMemoryFileStore {

  /**
   * Put the byte array data at the given location, overwrite if it already exists.
   */
  void put(String location, byte[] data);

  /**
   * Put the byte array at the given location only if it does not exist.
   * @return Existing byte array if the location exists or {@code null} otherwise.
   */
  byte[] putIfAbsent(String location, byte[] data);

  /**
   * Get the byte array at the given location.
   * @return {@link Optional} containing the byte array if the location exists
   * or {@link Optional#empty()} otherwise.
   */
  Optional<byte[]> get(String location);

  /**
   * Remove the given location.
   * @return {@code true} if the location existed and was successfully removed,
   * {@code false} otherwise.
   */
  boolean remove(String location);

  /**
   * Check whether the location exists.
   */
  boolean exists(String location);

  /**
   * Get a copy of existing locations.
   * The locations are returned in no particular order.
   */
  Collection<String> getLocations();
}
