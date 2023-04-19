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

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ByteBuffers;

class SerializableByteBufferMap implements Map<Integer, ByteBuffer>, Serializable {
  private final Map<Integer, ByteBuffer> wrapped;

  static Map<Integer, ByteBuffer> wrap(Map<Integer, ByteBuffer> map) {
    if (map == null) {
      return null;
    }

    if (map instanceof SerializableByteBufferMap) {
      return map;
    }

    return new SerializableByteBufferMap(map);
  }

  SerializableByteBufferMap() {
    this.wrapped = Maps.newLinkedHashMap();
  }

  private SerializableByteBufferMap(Map<Integer, ByteBuffer> wrapped) {
    this.wrapped = wrapped;
  }

  private static class MapSerializationProxy implements Serializable {
    private int[] keys = null;
    private byte[][] values = null;

    /** Constructor for Java serialization. */
    MapSerializationProxy() {}

    MapSerializationProxy(int[] keys, byte[][] values) {
      this.keys = keys;
      this.values = values;
    }

    Object readResolve() throws ObjectStreamException {
      Map<Integer, ByteBuffer> map = Maps.newLinkedHashMap();

      for (int i = 0; i < keys.length; i += 1) {
        map.put(keys[i], ByteBuffer.wrap(values[i]));
      }

      return SerializableByteBufferMap.wrap(map);
    }
  }

  Object writeReplace() throws ObjectStreamException {
    Collection<Map.Entry<Integer, ByteBuffer>> entries = wrapped.entrySet();
    int[] keys = new int[entries.size()];
    byte[][] values = new byte[keys.length][];

    int keyIndex = 0;
    for (Map.Entry<Integer, ByteBuffer> entry : entries) {
      keys[keyIndex] = entry.getKey();
      values[keyIndex] = ByteBuffers.toByteArray(entry.getValue());
      keyIndex += 1;
    }

    return new MapSerializationProxy(keys, values);
  }

  @Override
  public int size() {
    return wrapped.size();
  }

  @Override
  public boolean isEmpty() {
    return wrapped.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return wrapped.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return wrapped.containsValue(value);
  }

  @Override
  public ByteBuffer get(Object key) {
    return wrapped.get(key);
  }

  @Override
  public ByteBuffer put(Integer key, ByteBuffer value) {
    return wrapped.put(key, value);
  }

  @Override
  public ByteBuffer remove(Object key) {
    return wrapped.remove(key);
  }

  @Override
  public void putAll(Map<? extends Integer, ? extends ByteBuffer> m) {
    wrapped.putAll(m);
  }

  @Override
  public void clear() {
    wrapped.clear();
  }

  @Override
  public Set<Integer> keySet() {
    return wrapped.keySet();
  }

  @Override
  public Collection<ByteBuffer> values() {
    return wrapped.values();
  }

  @Override
  public Set<Entry<Integer, ByteBuffer>> entrySet() {
    return wrapped.entrySet();
  }

  @Override
  public boolean equals(Object o) {
    return wrapped.equals(o);
  }

  @Override
  public int hashCode() {
    return wrapped.hashCode();
  }
}
