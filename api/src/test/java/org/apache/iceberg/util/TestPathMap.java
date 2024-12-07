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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.assertj.core.api.AbstractObjectAssert;
import org.junit.jupiter.api.Test;

public class TestPathMap {

  @Test
  public void nullString() {
    assertMap(PathMap.create()).doesNotContainKey(null);
    assertMap(PathMap.create()).doesNotContainValue(null);
  }

  @Test
  public void testEmptyMap() {
    PathMap<String> map = PathMap.create();
    assertMap(map).isEmpty();
    assertMap(map).hasSize(0);
    assertMap(map).doesNotContainKey("key");
    assertMap(map).doesNotContainValue("value");
    assertThat(map.values()).isEmpty();
    assertThat(map.keys()).isEmpty();
    assertThat(map.entries()).isEmpty();
  }

  @Test
  public void testDifferentCharSequenceImplementations() {
    PathMap<String> map = PathMap.create();
    map.put("abc", "value1");
    map.put(new StringBuffer("def"), "value2");
    assertMap(map).containsEntry(new StringBuilder("abc"), "value1");
    assertMap(map).containsEntry("def", "value2");
  }

  @Test
  public void testPutAndGet() {
    PathMap<String> map = PathMap.create();
    map.put("key1", "value1");
    assertMap(map).containsEntry("key1", "value1");
  }

  @Test
  public void testRemove() {
    PathMap<String> map = PathMap.create();
    map.put("key1", "value1");
    map.remove(new StringBuilder("key1"));
    assertMap(map).doesNotContainKey("key1");
    assertMap(map).isEmpty();
  }

  @Test
  public void testClear() {
    PathMap<String> map = PathMap.create();
    map.put("key1", "value1");
    map.clear();
    assertMap(map).isEmpty();
  }

  @Test
  public void testValues() {
    PathMap<String> map = PathMap.create();
    map.put("key1", "value1");
    map.put("key2", "value2");
    assertThat(map.values()).containsAll(ImmutableList.of("value1", "value2"));
  }

  @Test
  public void testEntrySet() {
    PathMap<String> map = PathMap.create();
    map.put("key1", "value1");
    map.put(new StringBuilder("key2"), "value2");
    assertThat(map.entries()).hasSize(2);
  }

  @Test
  public void testEquals() {
    PathMap<String> map1 = PathMap.create();
    map1.put(new StringBuilder("key"), "value");

    PathMap<String> map2 = PathMap.create();
    map2.put("key", "value");

    assertThat(map1).isEqualTo(map2);
  }

  @Test
  public void testHashCode() {
    PathMap<String> map1 = PathMap.create();
    map1.put(new StringBuilder("key"), "value");

    PathMap<String> map2 = PathMap.create();
    map2.put("key", "value");

    assertThat(map1.hashCode()).isEqualTo(map2.hashCode());
  }

  @Test
  public void testToString() {
    PathMap<String> map = PathMap.create();

    // empty map
    assertThat(map.toString()).isEqualTo("{}");

    // single entry
    map.put("key1", "value1");
    assertThat(map.toString()).isEqualTo("{key1=value1}");

    // multiple entries
    map.put("key2", "value2");
    map.put("key3", "value3");
    String toStringResult = map.toString();
    assertThat(toStringResult).contains("key1=value1", "key2=value2", "key3=value3");
  }

  @Test
  public void testComputeIfAbsent() {
    PathMap<String> map = PathMap.create();

    String result1 = map.computeIfAbsent("key1", () -> "computedValue1");
    assertThat(result1).isEqualTo("computedValue1");
    assertMap(map).containsEntry("key1", "computedValue1");

    // verify existing key is not affected
    String result2 = map.computeIfAbsent("key1", () -> "newValue");
    assertThat(result2).isEqualTo("computedValue1");
    assertMap(map).containsEntry("key1", "computedValue1");
  }

  @Test
  public void testConcurrentReadAccess() throws InterruptedException {
    PathMap<String> map = PathMap.create();
    map.put("key1", "value1");
    map.put("key2", "value2");
    map.put("key3", "value3");

    int numThreads = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

    // read the map from multiple threads to ensure thread-local wrappers are used
    for (int i = 0; i < numThreads; i++) {
      executorService.submit(
          () -> {
            assertThat(map.get("key1")).isEqualTo("value1");
            assertThat(map.get("key2")).isEqualTo("value2");
            assertThat(map.get("key3")).isEqualTo("value3");
          });
    }

    executorService.shutdown();
    assertThat(executorService.awaitTermination(1, TimeUnit.MINUTES)).isTrue();
  }

  private static <T> CharSequenceMapAssert<T> assertMap(PathMap<T> pathMap) {
    return new CharSequenceMapAssert<>(pathMap);
  }

  private static class CharSequenceMapAssert<T>
      extends AbstractObjectAssert<CharSequenceMapAssert<T>, PathMap<T>> {
    CharSequenceMapAssert(PathMap<T> pathMap) {
      super(pathMap, CharSequenceMapAssert.class);
    }

    void doesNotContainKey(CharSequence key) {
      assertThat(actual.keys()).doesNotContain(key);
    }

    void doesNotContainValue(T value) {
      assertThat(actual.values()).doesNotContain(value);
    }

    void isEmpty() {
      assertThat(actual.isEmpty()).isTrue();
    }

    void hasSize(int size) {
      assertThat(actual.size()).isEqualTo(size);
    }

    void containsEntry(CharSequence key, T value) {
      assertThat(actual.get(key)).isEqualTo(value);
      assertThat(actual.entries())
          .satisfiesOnlyOnce(
              entry -> {
                assertThat(CharSequenceWrapper.wrap(entry.getKey()))
                    .isEqualTo(CharSequenceWrapper.wrap(key));
                assertThat(entry.getValue()).isEqualTo(value);
              });
    }
  }
}
