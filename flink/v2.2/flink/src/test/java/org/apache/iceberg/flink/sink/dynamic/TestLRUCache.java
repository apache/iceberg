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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

class TestLRUCache {
  private static final Consumer<Map.Entry<Integer, Integer>> NO_OP_CALLBACK = ignored -> {};

  @Test
  void testPut() {
    LRUCache<Integer, Integer> cache = new LRUCache<>(1, NO_OP_CALLBACK);
    cache.put(1, 1);

    assertThat(cache).hasSize(1).containsEntry(1, 1);
  }

  @Test
  void testGet() {
    LRUCache<Integer, Integer> cache = new LRUCache<>(1, NO_OP_CALLBACK);
    cache.put(1, 123);

    assertThat(cache).hasSize(1);
    assertThat(cache.get(1)).isEqualTo(123);
  }

  @Test
  void testElementEviction() {
    int maxSize = 2;
    LRUCache<Integer, Integer> cache = new LRUCache<>(maxSize, NO_OP_CALLBACK);

    cache.put(1, 1);
    cache.put(2, 2);
    Integer value = cache.get(1);
    assertThat(value).isEqualTo(1);

    cache.put(3, 3); // "2" should be evicted

    assertThat(cache).hasSize(2).containsEntry(1, 1).containsEntry(3, 3);
  }

  @Test
  void testEvictionCallback() {
    int maxSize = 2;
    TestEvictionCallback callback = new TestEvictionCallback();
    LRUCache<Integer, Integer> cache = new LRUCache<>(maxSize, callback);

    cache.put(1, 1);
    cache.put(2, 2);
    Integer value = cache.get(1);
    assertThat(value).isEqualTo(1);

    cache.put(3, 3); // "2" should be evicted

    assertThat(callback.evictedEntries).containsExactly(Map.entry(2, 2));
  }

  private static class TestEvictionCallback implements Consumer<Map.Entry<Integer, Integer>> {
    private final List<Map.Entry<Integer, Integer>> evictedEntries = Lists.newArrayList();

    @Override
    public void accept(Map.Entry<Integer, Integer> entry) {
      evictedEntries.add(entry);
    }
  }
}
