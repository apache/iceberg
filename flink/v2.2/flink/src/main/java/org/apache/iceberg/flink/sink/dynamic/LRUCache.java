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
package org.apache.iceberg.flink.sink.dynamic;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * A performant, fixed size least recently used (LRU) cache implementation.
 *
 * <p>This cache has O(1) time complexity for get/put operations and provides eviction notifications
 * when entries are removed due to size constraints. It offers better performance than similarly
 * configured Caffeine caches, making it ideal for hot path operations.
 *
 * <p>This implementation extends {@link LinkedHashMap} with access-order traversal and automated
 * removal of least recently used entries when the maximum size is reached.
 */
@SuppressWarnings("checkstyle:IllegalType")
class LRUCache<K, V> extends LinkedHashMap<K, V> {
  /** Defaults from {@link java.util.HashMap} */
  private static final int DEFAULT_INITIAL_CAPACITY = 1 << 4;

  private static final float DEFAULT_LOAD_FACTOR = 0.75f;

  private final int maximumSize;
  private final Consumer<Map.Entry<K, V>> evictionCallback;

  LRUCache(int maximumSize) {
    this(maximumSize, ignored -> {});
  }

  LRUCache(int maximumSize, Consumer<Map.Entry<K, V>> evictionCallback) {
    super(Math.min(maximumSize, DEFAULT_INITIAL_CAPACITY), DEFAULT_LOAD_FACTOR, true);
    this.maximumSize = maximumSize;
    this.evictionCallback = evictionCallback;
  }

  @Override
  protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
    boolean remove = size() > maximumSize;
    if (remove) {
      evictionCallback.accept(eldest);
    }

    return remove;
  }
}
