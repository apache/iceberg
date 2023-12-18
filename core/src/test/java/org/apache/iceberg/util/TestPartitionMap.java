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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers.CustomRow;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestPartitionMap {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()),
          required(3, "category", Types.StringType.get()));
  private static final PartitionSpec UNPARTITIONED_SPEC = PartitionSpec.unpartitioned();
  private static final PartitionSpec BY_DATA_SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("data").withSpecId(1).build();
  private static final PartitionSpec BY_DATA_CATEGORY_BUCKET_SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("data").bucket("category", 8).withSpecId(3).build();
  private static final Map<Integer, PartitionSpec> SPECS =
      ImmutableMap.of(
          UNPARTITIONED_SPEC.specId(),
          UNPARTITIONED_SPEC,
          BY_DATA_SPEC.specId(),
          BY_DATA_SPEC,
          BY_DATA_CATEGORY_BUCKET_SPEC.specId(),
          BY_DATA_CATEGORY_BUCKET_SPEC);

  @Test
  public void testEmptyMap() {
    PartitionMap<String> map = PartitionMap.create(SPECS);
    assertThat(map).isEmpty();
    assertThat(map).hasSize(0);
    assertThat(map).doesNotContainKey(Pair.of(1, Row.of(1))).doesNotContainValue("value");
    assertThat(map.values()).isEmpty();
    assertThat(map.keySet()).isEmpty();
    assertThat(map.entrySet()).isEmpty();
  }

  @Test
  public void testSize() {
    PartitionMap<String> map = PartitionMap.create(SPECS);
    map.put(UNPARTITIONED_SPEC.specId(), null, "v1");
    map.put(BY_DATA_SPEC.specId(), Row.of("aaa"), "v2");
    map.put(BY_DATA_SPEC.specId(), Row.of("bbb"), "v3");
    map.put(BY_DATA_CATEGORY_BUCKET_SPEC.specId(), Row.of("ccc", 2), "v4");
    assertThat(map).isNotEmpty();
    assertThat(map).hasSize(4);
  }

  @Test
  public void testDifferentStructLikeImplementations() {
    PartitionMap<String> map = PartitionMap.create(SPECS);
    map.put(BY_DATA_SPEC.specId(), CustomRow.of("aaa"), "value");
    map.put(UNPARTITIONED_SPEC.specId(), null, "value");
    assertThat(map)
        .containsEntry(Pair.of(BY_DATA_SPEC.specId(), CustomRow.of("aaa")), "value")
        .containsEntry(Pair.of(BY_DATA_SPEC.specId(), Row.of("aaa")), "value")
        .containsEntry(Pair.of(UNPARTITIONED_SPEC.specId(), null), "value");
  }

  @Test
  public void testPutAndGet() {
    PartitionMap<String> map = PartitionMap.create(SPECS);
    map.put(UNPARTITIONED_SPEC.specId(), null, "v1");
    map.put(BY_DATA_CATEGORY_BUCKET_SPEC.specId(), Row.of("aaa", 1), "v2");
    assertThat(map.get(UNPARTITIONED_SPEC.specId(), null)).isEqualTo("v1");
    assertThat(map.get(BY_DATA_CATEGORY_BUCKET_SPEC.specId(), Row.of("aaa", 1))).isEqualTo("v2");
  }

  @Test
  public void testRemove() {
    PartitionMap<String> map = PartitionMap.create(SPECS);

    map.put(BY_DATA_SPEC.specId(), Row.of("aaa"), "v1");
    map.put(BY_DATA_SPEC.specId(), Row.of("bbb"), "v2");

    map.removeKey(BY_DATA_SPEC.specId(), Row.of("aaa"));

    assertThat(map).doesNotContainKey(Pair.of(BY_DATA_SPEC.specId(), Row.of("aaa")));
    assertThat(map.get(BY_DATA_SPEC.specId(), Row.of("aaa"))).isNull();
    assertThat(map).containsKey(Pair.of(BY_DATA_SPEC.specId(), Row.of("bbb")));
    assertThat(map.get(BY_DATA_SPEC.specId(), Row.of("bbb"))).isEqualTo("v2");
  }

  @Test
  public void putAll() {
    PartitionMap<String> map = PartitionMap.create(SPECS);

    Map<Pair<Integer, StructLike>, String> otherMap = Maps.newHashMap();
    otherMap.put(Pair.of(BY_DATA_SPEC.specId(), Row.of("aaa")), "v1");
    otherMap.put(Pair.of(BY_DATA_SPEC.specId(), Row.of("bbb")), "v2");
    map.putAll(otherMap);

    assertThat(map)
        .containsEntry(Pair.of(BY_DATA_SPEC.specId(), Row.of("aaa")), "v1")
        .containsEntry(Pair.of(BY_DATA_SPEC.specId(), Row.of("bbb")), "v2");
  }

  @Test
  public void testClear() {
    PartitionMap<String> map = PartitionMap.create(SPECS);
    map.put(UNPARTITIONED_SPEC.specId(), null, "v1");
    map.put(BY_DATA_SPEC.specId(), Row.of("aaa"), "v2");
    assertThat(map).hasSize(2);
    map.clear();
    assertThat(map).isEmpty();
  }

  @Test
  public void testValues() {
    PartitionMap<Integer> map = PartitionMap.create(SPECS);
    map.put(BY_DATA_SPEC.specId(), Row.of("aaa"), 1);
    map.put(BY_DATA_CATEGORY_BUCKET_SPEC.specId(), Row.of("aaa", 2), 2);
    map.put(BY_DATA_SPEC.specId(), Row.of("bbb"), 3);
    assertThat(map.values()).containsAll(ImmutableList.of(1, 2, 3));
  }

  @Test
  public void testEntrySet() {
    PartitionMap<String> map = PartitionMap.create(SPECS);
    map.put(BY_DATA_SPEC.specId(), Row.of("aaa"), "v1");
    map.put(BY_DATA_CATEGORY_BUCKET_SPEC.specId(), Row.of("bbb", 2), "v2");
    map.put(BY_DATA_SPEC.specId(), CustomRow.of("ccc"), "v3");
    assertThat(map.entrySet()).hasSize(3);
  }

  @Test
  public void testKeySet() {
    PartitionMap<String> map = PartitionMap.create(SPECS);
    map.put(BY_DATA_SPEC.specId(), Row.of("aaa"), "v1");
    map.put(BY_DATA_SPEC.specId(), CustomRow.of("ccc"), "v2");
    assertThat(map.get(BY_DATA_SPEC.specId(), CustomRow.of("aaa"))).isEqualTo("v1");
    assertThat(map.get(BY_DATA_SPEC.specId(), Row.of("ccc"))).isEqualTo("v2");
  }

  @Test
  public void testEqualsAndHashCode() {
    PartitionMap<String> map1 = PartitionMap.create(SPECS);
    PartitionMap<String> map2 = PartitionMap.create(SPECS);

    assertThat(map1).isEqualTo(map2);
    assertThat(map1.hashCode()).isEqualTo(map2.hashCode());

    map1.put(BY_DATA_SPEC.specId(), Row.of("aaa"), "v1");
    map1.put(BY_DATA_SPEC.specId(), Row.of("bbb"), "v2");

    map2.put(BY_DATA_SPEC.specId(), CustomRow.of("aaa"), "v1");
    map2.put(BY_DATA_SPEC.specId(), CustomRow.of("bbb"), "v2");

    assertThat(map1).isEqualTo(map2);
    assertThat(map1.hashCode()).isEqualTo(map2.hashCode());
  }

  @Test
  public void testToString() {
    PartitionMap<String> map = PartitionMap.create(SPECS);

    // empty map
    assertThat(map.toString()).isEqualTo("{}");

    // single entry
    map.put(BY_DATA_SPEC.specId(), Row.of("aaa"), "v1");
    assertThat(map.toString()).isEqualTo("{data=aaa -> v1}");

    // multiple entries
    map.put(BY_DATA_SPEC.specId(), CustomRow.of("bbb"), "v2");
    map.put(BY_DATA_CATEGORY_BUCKET_SPEC.specId(), Row.of("ccc", 2), "v3");
    assertThat(map.toString())
        .contains("data=aaa -> v1")
        .contains("data=bbb -> v2")
        .contains("data=ccc/category_bucket=2 -> v3");
  }

  @Test
  public void testConcurrentReadAccess() throws InterruptedException {
    PartitionMap<String> map = PartitionMap.create(SPECS);
    map.put(BY_DATA_SPEC.specId(), Row.of("aaa"), "v1");
    map.put(BY_DATA_SPEC.specId(), Row.of("bbb"), "v2");
    map.put(UNPARTITIONED_SPEC.specId(), null, "v3");
    map.put(BY_DATA_SPEC.specId(), CustomRow.of("ccc"), "v4");

    int numThreads = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

    // read the map from multiple threads to ensure thread-local wrappers are used
    for (int i = 0; i < numThreads; i++) {
      executorService.submit(
          () -> {
            assertThat(map.get(BY_DATA_SPEC.specId(), Row.of("aaa"))).isEqualTo("v1");
            assertThat(map.get(BY_DATA_SPEC.specId(), Row.of("bbb"))).isEqualTo("v2");
            assertThat(map.get(UNPARTITIONED_SPEC.specId(), null)).isEqualTo("v3");
            assertThat(map.get(BY_DATA_SPEC.specId(), Row.of("ccc"))).isEqualTo("v4");
          });
    }

    executorService.shutdown();
    assertThat(executorService.awaitTermination(1, TimeUnit.MINUTES)).isTrue();
  }

  @Test
  public void testNullKey() {
    PartitionMap<String> map = PartitionMap.create(SPECS);
    assertThatThrownBy(() -> map.put(null, "value")).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> map.get(null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> map.remove(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void testUnknownSpecId() {
    PartitionMap<String> map = PartitionMap.create(SPECS);
    assertThatThrownBy(() -> map.put(Integer.MAX_VALUE, null, "value"))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Cannot find spec with ID");
  }

  @Test
  public void testUnmodifiableViews() {
    PartitionMap<String> map = PartitionMap.create(SPECS);
    map.put(BY_DATA_SPEC.specId(), Row.of("aaa"), "v1");
    map.put(BY_DATA_SPEC.specId(), Row.of("bbb"), "v2");

    assertThatThrownBy(() -> map.keySet().add(Pair.of(1, null)))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> map.values().add("other"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> map.entrySet().add(null))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> map.entrySet().iterator().next().setValue("other"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> map.entrySet().iterator().remove())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void testKeyAndEntrySetEquality() {
    PartitionMap<String> map1 = PartitionMap.create(SPECS);
    PartitionMap<String> map2 = PartitionMap.create(SPECS);

    assertThat(map1.keySet()).isEqualTo(map2.keySet());
    assertThat(map1.entrySet()).isEqualTo(map2.entrySet());

    map1.put(BY_DATA_SPEC.specId(), Row.of("aaa"), "v1");
    map1.put(BY_DATA_SPEC.specId(), Row.of("bbb"), "v2");
    map1.put(UNPARTITIONED_SPEC.specId(), null, "v3");

    map2.put(BY_DATA_SPEC.specId(), CustomRow.of("aaa"), "v1");
    map2.put(BY_DATA_SPEC.specId(), CustomRow.of("bbb"), "v2");
    map2.put(UNPARTITIONED_SPEC.specId(), null, "v3");

    assertThat(map1.keySet()).isEqualTo(map2.keySet());
    assertThat(map1.entrySet()).isEqualTo(map2.entrySet());
  }

  @Test
  public void testLookupArbitraryKeyTypes() {
    PartitionMap<String> map = PartitionMap.create(SPECS);
    map.put(BY_DATA_SPEC.specId(), Row.of("aaa"), "v1");
    map.put(UNPARTITIONED_SPEC.specId(), null, "v2");
    assertThat(map.containsKey("some-string")).isFalse();
    assertThat(map.get("some-string")).isNull();
    assertThat(map.remove("some-string")).isNull();
  }

  @Test
  public void testComputeIfAbsent() {
    PartitionMap<String> map = PartitionMap.create(SPECS);

    String result1 = map.computeIfAbsent(BY_DATA_SPEC.specId(), Row.of("a"), () -> "v1");
    assertThat(result1).isEqualTo("v1");
    assertThat(map.get(BY_DATA_SPEC.specId(), CustomRow.of("a"))).isEqualTo("v1");

    // verify existing key is not affected
    String result2 = map.computeIfAbsent(BY_DATA_SPEC.specId(), CustomRow.of("a"), () -> "v2");
    assertThat(result2).isEqualTo("v1");
    assertThat(map.get(BY_DATA_SPEC.specId(), CustomRow.of("a"))).isEqualTo("v1");
  }
}
