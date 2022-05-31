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

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/** An in-memory collection based storage for file contents keyed by a string location. */
class InMemoryFileStore {

  private final ConcurrentMap<String, ByteBuffer> store;

  InMemoryFileStore() {
    this.store = Maps.newConcurrentMap();
  }

  /** Put the file contents at the given location, overwrite if it already exists. */
  public void put(String location, ByteBuffer data) {
    store.put(location, data.asReadOnlyBuffer());
  }

  /** Get the file contents for the given location. */
  public Optional<ByteBuffer> get(String location) {
    return Optional.ofNullable(store.get(location));
  }

  /**
   * Remove the given location and its contents.
   *
   * @return <code>true</code> if content for the given location was removed, <code>false</code> if
   *     there was no content for the given location.
   */
  public boolean remove(String location) {
    return store.remove(location) != null;
  }

  /** Check whether the location exists. */
  public boolean exists(String location) {
    return store.containsKey(location);
  }
}
