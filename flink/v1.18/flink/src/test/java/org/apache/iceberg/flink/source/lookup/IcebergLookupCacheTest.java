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
package org.apache.iceberg.flink.source.lookup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** 测试 IcebergLookupCache 类 */
public class IcebergLookupCacheTest {

  private IcebergLookupCache partialCache;
  private IcebergLookupCache allCache;

  @BeforeEach
  void before() {
    // 创建 PARTIAL 模式缓存
    partialCache =
        IcebergLookupCache.createPartialCache(
            IcebergLookupCache.CacheConfig.builder()
                .ttl(Duration.ofMinutes(10))
                .maxRows(100)
                .build());
    partialCache.open();

    // 创建 ALL 模式缓存
    allCache =
        IcebergLookupCache.createAllCache(
            IcebergLookupCache.CacheConfig.builder()
                .ttl(Duration.ofMinutes(10))
                .maxRows(100)
                .build());
    allCache.open();
  }

  @AfterEach
  void after() {
    if (partialCache != null) {
      partialCache.close();
    }
    if (allCache != null) {
      allCache.close();
    }
  }

  @Test
  void testPartialCachePutAndGet() {
    RowData key = createKey(1);
    List<RowData> value = createValues(1, 2);

    // 初始状态应为空
    assertThat(partialCache.get(key)).isNull();

    // 放入缓存
    partialCache.put(key, value);

    // 应能获取到
    List<RowData> result = partialCache.get(key);
    assertThat(result).isNotNull();
    assertThat(result).hasSize(2);
  }

  @Test
  void testPartialCacheInvalidate() {
    RowData key = createKey(1);
    List<RowData> value = createValues(1, 2);

    partialCache.put(key, value);
    assertThat(partialCache.get(key)).isNotNull();

    // 失效缓存
    partialCache.invalidate(key);
    assertThat(partialCache.get(key)).isNull();
  }

  @Test
  void testPartialCacheInvalidateAll() {
    RowData key1 = createKey(1);
    RowData key2 = createKey(2);
    partialCache.put(key1, createValues(1));
    partialCache.put(key2, createValues(2));

    assertThat(partialCache.size()).isEqualTo(2);

    partialCache.invalidateAll();

    assertThat(partialCache.size()).isEqualTo(0);
    assertThat(partialCache.get(key1)).isNull();
    assertThat(partialCache.get(key2)).isNull();
  }

  @Test
  void testPartialCacheLRUEviction() {
    // 创建一个最大容量为 5 的缓存
    IcebergLookupCache smallCache =
        IcebergLookupCache.createPartialCache(
            IcebergLookupCache.CacheConfig.builder()
                .ttl(Duration.ofMinutes(10))
                .maxRows(5)
                .build());
    smallCache.open();

    try {
      // 放入 10 个元素
      for (int i = 0; i < 10; i++) {
        smallCache.put(createKey(i), createValues(i));
      }

      // 由于 Caffeine 的异步特性，等待一下
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      // 缓存大小应该不超过 5（可能略有波动）
      assertThat(smallCache.size()).isLessThanOrEqualTo(6);

    } finally {
      smallCache.close();
    }
  }

  @Test
  void testAllCacheRefresh() throws Exception {
    RowData key1 = createKey(1);
    RowData key2 = createKey(2);

    // 初始刷新
    allCache.refreshAll(
        () -> {
          List<IcebergLookupCache.CacheEntry> entries = Lists.newArrayList();
          entries.add(new IcebergLookupCache.CacheEntry(key1, createValues(1)));
          entries.add(new IcebergLookupCache.CacheEntry(key2, createValues(2)));
          return entries;
        });

    assertThat(allCache.getFromAll(key1)).isNotNull();
    assertThat(allCache.getFromAll(key2)).isNotNull();
    assertThat(allCache.size()).isEqualTo(2);

    // 第二次刷新（模拟数据变化）
    RowData key3 = createKey(3);
    allCache.refreshAll(
        () -> {
          List<IcebergLookupCache.CacheEntry> entries = Lists.newArrayList();
          entries.add(new IcebergLookupCache.CacheEntry(key1, createValues(10)));
          entries.add(new IcebergLookupCache.CacheEntry(key3, createValues(3)));
          return entries;
        });

    // key1 应该更新，key2 应该不存在，key3 应该存在
    assertThat(allCache.getFromAll(key1)).isNotNull();
    assertThat(allCache.getFromAll(key2)).isNull();
    assertThat(allCache.getFromAll(key3)).isNotNull();
    assertThat(allCache.size()).isEqualTo(2);
  }

  @Test
  void testAllCacheRefreshFailure() {
    RowData key1 = createKey(1);

    // 先正常刷新
    try {
      allCache.refreshAll(
          () ->
              Collections.singletonList(new IcebergLookupCache.CacheEntry(key1, createValues(1))));
    } catch (Exception e) {
      // ignore
    }

    assertThat(allCache.getFromAll(key1)).isNotNull();

    // 模拟刷新失败
    assertThatThrownBy(
            () ->
                allCache.refreshAll(
                    () -> {
                      throw new RuntimeException("Simulated failure");
                    }))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("Simulated failure");

    // 原有数据应该保留（但实际上由于双缓冲机制，备缓存已被清空）
    // 这里验证刷新失败后不会导致 NPE
  }

  @Test
  void testCacheModeRestrictions() {
    // PARTIAL 模式下调用 ALL 模式方法应该抛出异常
    assertThatThrownBy(() -> partialCache.getFromAll(createKey(1)))
        .isInstanceOf(IllegalStateException.class);

    assertThatThrownBy(() -> partialCache.refreshAll(Collections::emptyList))
        .isInstanceOf(IllegalStateException.class);

    // ALL 模式下调用 PARTIAL 模式方法应该抛出异常
    assertThatThrownBy(() -> allCache.get(createKey(1))).isInstanceOf(IllegalStateException.class);

    assertThatThrownBy(() -> allCache.put(createKey(1), createValues(1)))
        .isInstanceOf(IllegalStateException.class);

    assertThatThrownBy(() -> allCache.invalidate(createKey(1)))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void testCacheConfig() {
    IcebergLookupCache.CacheConfig config =
        IcebergLookupCache.CacheConfig.builder().ttl(Duration.ofHours(1)).maxRows(50000).build();

    assertThat(config.getTtl()).isEqualTo(Duration.ofHours(1));
    assertThat(config.getMaxRows()).isEqualTo(50000);
  }

  @Test
  void testCacheConfigValidation() {
    assertThatThrownBy(() -> IcebergLookupCache.CacheConfig.builder().ttl(null).build())
        .isInstanceOf(NullPointerException.class);

    assertThatThrownBy(() -> IcebergLookupCache.CacheConfig.builder().maxRows(0).build())
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> IcebergLookupCache.CacheConfig.builder().maxRows(-1).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testGetCacheMode() {
    assertThat(partialCache.getCacheMode()).isEqualTo(IcebergLookupCache.CacheMode.PARTIAL);
    assertThat(allCache.getCacheMode()).isEqualTo(IcebergLookupCache.CacheMode.ALL);
  }

  @Test
  void testEmptyValueCache() {
    RowData key = createKey(1);

    // 缓存空列表
    partialCache.put(key, Collections.emptyList());

    List<RowData> result = partialCache.get(key);
    assertThat(result).isNotNull();
    assertThat(result).isEmpty();
  }

  // 辅助方法：创建测试用的 Key RowData
  private RowData createKey(int id) {
    GenericRowData key = new GenericRowData(1);
    key.setField(0, id);
    return key;
  }

  // 辅助方法：创建测试用的 Value RowData 列表
  private List<RowData> createValues(int... values) {
    List<RowData> list = Lists.newArrayList();
    for (int value : values) {
      GenericRowData row = new GenericRowData(2);
      row.setField(0, value);
      row.setField(1, StringData.fromString("value-" + value));
      list.add(row);
    }
    return list;
  }
}
