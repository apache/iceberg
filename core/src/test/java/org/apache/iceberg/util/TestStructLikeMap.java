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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestStructLikeMap {
  private static final Types.StructType STRUCT_TYPE =
      Types.StructType.of(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.LongType.get()));

  @Test
  public void testSingleRecord() {
    Record gRecord = GenericRecord.create(STRUCT_TYPE);
    Record record1 = gRecord.copy(ImmutableMap.of("id", 1, "data", "aaa"));

    Map<StructLike, String> map = StructLikeMap.create(STRUCT_TYPE);
    Assert.assertEquals(0, map.size());

    map.put(record1, "1-aaa");
    Assert.assertEquals(1, map.size());
    Assert.assertFalse(map.isEmpty());
    Assert.assertTrue(map.containsKey(record1));
    Assert.assertTrue(map.containsValue("1-aaa"));
    Assert.assertEquals("1-aaa", map.get(record1));

    Set<StructLike> keySet = map.keySet();
    Assert.assertEquals(1, keySet.size());
    Assert.assertTrue(keySet.contains(record1));

    Collection<String> values = map.values();
    Assert.assertEquals(1, values.size());
    Assert.assertEquals("1-aaa", values.toArray(new String[0])[0]);

    Set<Map.Entry<StructLike, String>> entrySet = map.entrySet();
    Assert.assertEquals(1, entrySet.size());
    for (Map.Entry<StructLike, String> entry : entrySet) {
      Assert.assertEquals(record1, entry.getKey());
      Assert.assertEquals("1-aaa", entry.getValue());
      break;
    }
  }

  @Test
  public void testMultipleRecord() {
    Record gRecord = GenericRecord.create(STRUCT_TYPE);
    Record record1 = gRecord.copy(ImmutableMap.of("id", 1, "data", "aaa"));
    Record record2 = gRecord.copy(ImmutableMap.of("id", 2, "data", "bbb"));
    Record record3 = gRecord.copy();
    record3.setField("id", 3);
    record3.setField("data", null);

    Map<StructLike, String> map = StructLikeMap.create(STRUCT_TYPE);
    Assert.assertEquals(0, map.size());

    map.putAll(ImmutableMap.of(record1, "1-aaa", record2, "2-bbb", record3, "3-null"));
    Assert.assertEquals(3, map.size());
    Assert.assertTrue(map.containsKey(record1));
    Assert.assertTrue(map.containsKey(record2));
    Assert.assertTrue(map.containsKey(record3));
    Assert.assertTrue(map.containsValue("1-aaa"));
    Assert.assertTrue(map.containsValue("2-bbb"));
    Assert.assertTrue(map.containsValue("3-null"));
    Assert.assertEquals("1-aaa", map.get(record1));
    Assert.assertEquals("2-bbb", map.get(record2));
    Assert.assertEquals("3-null", map.get(record3));

    Set<StructLike> keySet = map.keySet();
    Assert.assertEquals(3, keySet.size());
    Assert.assertEquals(ImmutableSet.of(record1, record2, record3), keySet);

    Collection<String> values = map.values();
    Assert.assertEquals(3, values.size());
    Assert.assertEquals(ImmutableSet.of("1-aaa", "2-bbb", "3-null"), Sets.newHashSet(values));

    Set<Map.Entry<StructLike, String>> entrySet = map.entrySet();
    Assert.assertEquals(3, entrySet.size());
    Set<StructLike> structLikeSet = Sets.newHashSet();
    Set<String> valueSet = Sets.newHashSet();
    for (Map.Entry<StructLike, String> entry : entrySet) {
      structLikeSet.add(entry.getKey());
      valueSet.add(entry.getValue());
    }
    Assert.assertEquals(ImmutableSet.of(record1, record2, record3), structLikeSet);
    Assert.assertEquals(ImmutableSet.of("1-aaa", "2-bbb", "3-null"), valueSet);
  }

  @Test
  public void testRemove() {
    Record gRecord = GenericRecord.create(STRUCT_TYPE);
    Record record = gRecord.copy(ImmutableMap.of("id", 1, "data", "aaa"));

    Map<StructLike, String> map = StructLikeMap.create(STRUCT_TYPE);
    map.put(record, "1-aaa");
    Assert.assertEquals(1, map.size());
    Assert.assertEquals("1-aaa", map.get(record));
    Assert.assertEquals("1-aaa", map.remove(record));
    Assert.assertEquals(0, map.size());

    map.put(record, "1-aaa");
    Assert.assertEquals("1-aaa", map.get(record));
  }

  @Test
  public void testNullKeys() {
    Map<StructLike, String> map = StructLikeMap.create(STRUCT_TYPE);
    Assert.assertFalse(map.containsKey(null));

    map.put(null, "aaa");
    Assert.assertTrue(map.containsKey(null));
    Assert.assertEquals("aaa", map.get(null));

    String replacedValue = map.put(null, "bbb");
    Assert.assertEquals("aaa", replacedValue);

    String removedValue = map.remove(null);
    Assert.assertEquals("bbb", removedValue);
  }

  @Test
  public void testKeysWithNulls() {
    Record recordTemplate = GenericRecord.create(STRUCT_TYPE);
    Record record1 = recordTemplate.copy("id", 1, "data", null);
    Record record2 = recordTemplate.copy("id", 2, "data", null);

    Map<StructLike, String> map = StructLikeMap.create(STRUCT_TYPE);
    map.put(record1, "aaa");
    map.put(record2, "bbb");

    Assert.assertEquals("aaa", map.get(record1));
    Assert.assertEquals("bbb", map.get(record2));

    Record record3 = record1.copy();
    Assert.assertTrue(map.containsKey(record3));
    Assert.assertEquals("aaa", map.get(record3));

    Assert.assertEquals("aaa", map.remove(record3));
  }
}
