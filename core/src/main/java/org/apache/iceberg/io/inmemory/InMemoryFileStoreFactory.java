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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryFileStoreFactory {

  private InMemoryFileStoreFactory() {
    throw new UnsupportedOperationException();
  }

  /**
   * Create a new instance.
   */
  public static InMemoryFileStore create() {
    return new InMemoryFileStoreImpl();
  }

  private static final class InMemoryFileStoreImpl implements InMemoryFileStore {

    private final ConcurrentMap<String, byte[]> store;

    InMemoryFileStoreImpl() {
      this.store = new ConcurrentHashMap<>();
    }

    @Override
    public void put(String location, byte[] data) {
      store.put(location, data);
    }

    @Override
    public byte[] putIfAbsent(String location, byte[] data) {
      return store.putIfAbsent(location, data);
    }

    @Override
    public Optional<byte[]> get(String location) {
      return Optional.ofNullable(store.get(location));
    }

    @Override
    public boolean remove(String location) {
      return store.remove(location) != null;
    }

    @Override
    public boolean exists(String location) {
      return store.containsKey(location);
    }

    @Override
    public Set<String> getLocations() {
      return new HashSet<>(store.keySet());
    }
  }
}
