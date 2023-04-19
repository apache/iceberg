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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

public class TestMetricsSerialization {

  @Test
  public void testSerialization() throws IOException, ClassNotFoundException {
    Metrics original = generateMetrics();

    byte[] serialized = serialize(original);
    Metrics result = deserialize(serialized);

    assertEquals(original, result);
  }

  @Test
  public void testSerializationWithNulls() throws IOException, ClassNotFoundException {
    Metrics original = generateMetricsWithNulls();

    byte[] serialized = serialize(original);
    Metrics result = deserialize(serialized);

    assertEquals(original, result);
  }

  private static byte[] serialize(Metrics metrics) throws IOException {
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
      objectOutputStream.writeObject(metrics);
      objectOutputStream.flush();

      return byteArrayOutputStream.toByteArray();
    }
  }

  private static Metrics deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes)) {
      ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);

      return (Metrics) objectInputStream.readObject();
    }
  }

  private static Metrics generateMetrics() {
    Map<Integer, Long> longMap1 = Maps.newHashMap();
    longMap1.put(1, 2L);
    longMap1.put(3, 4L);

    Map<Integer, Long> longMap2 = Maps.newHashMap();
    longMap2.put(5, 6L);

    Map<Integer, Long> longMap3 = Maps.newHashMap();
    longMap3.put(7, 8L);

    Map<Integer, ByteBuffer> byteMap1 = Maps.newHashMap();
    byteMap1.put(1, ByteBuffer.wrap(new byte[] {1, 2, 3}));
    byteMap1.put(2, ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));

    Map<Integer, ByteBuffer> byteMap2 = Maps.newHashMap();
    byteMap1.put(3, ByteBuffer.wrap(new byte[] {1, 2}));

    return new Metrics(0L, longMap1, longMap2, longMap3, null, byteMap1, byteMap2);
  }

  private static Metrics generateMetricsWithNulls() {
    Map<Integer, Long> longMap = Maps.newHashMap();
    longMap.put(null, 1L);
    longMap.put(2, null);

    Map<Integer, ByteBuffer> byteMap = Maps.newHashMap();
    byteMap.put(null, ByteBuffer.wrap(new byte[] {1, 2, 3}));
    byteMap.put(4, null);

    return new Metrics(null, null, longMap, longMap, null, null, byteMap);
  }

  private static void assertEquals(Metrics expected, Metrics actual) {
    Assert.assertEquals(expected.recordCount(), actual.recordCount());
    Assert.assertEquals(expected.columnSizes(), actual.columnSizes());
    Assert.assertEquals(expected.valueCounts(), actual.valueCounts());
    Assert.assertEquals(expected.nullValueCounts(), actual.nullValueCounts());

    assertEquals(expected.lowerBounds(), actual.lowerBounds());
    assertEquals(expected.upperBounds(), actual.upperBounds());
  }

  private static void assertEquals(
      Map<Integer, ByteBuffer> expected, Map<Integer, ByteBuffer> actual) {
    if (expected == null) {
      Assert.assertNull(actual);
    } else {
      Assert.assertEquals(expected.size(), actual.size());
      expected.keySet().forEach(key -> Assert.assertEquals(expected.get(key), actual.get(key)));
    }
  }
}
