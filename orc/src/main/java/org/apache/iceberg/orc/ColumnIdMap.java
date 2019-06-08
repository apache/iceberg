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

package org.apache.iceberg.orc;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.orc.TypeDescription;

/**
 * The mapping from ORC's TypeDescription to the Iceberg column ids.
 * <p>
 * Keep the API limited to Map rather than a concrete type so that we can
 * change it later.
 */
public class ColumnIdMap implements Map<TypeDescription, Integer> {

  private final IdentityHashMap<TypeDescription, Integer> idMap =
      new IdentityHashMap<>();

  @Override
  public int size() {
    return idMap.size();
  }

  @Override
  public boolean isEmpty() {
    return idMap.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return idMap.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return idMap.containsValue(value);
  }

  @Override
  public Integer get(Object key) {
    return idMap.get(key);
  }

  @Override
  public Integer put(TypeDescription key, Integer value) {
    return idMap.put(key, value);
  }

  @Override
  public Integer remove(Object key) {
    return idMap.remove(key);
  }

  @Override
  public void putAll(Map<? extends TypeDescription, ? extends Integer> map) {
    idMap.putAll(map);
  }

  @Override
  public void clear() {
    idMap.clear();
  }

  @Override
  public Set<TypeDescription> keySet() {
    return idMap.keySet();
  }

  @Override
  public Collection<Integer> values() {
    return idMap.values();
  }

  @Override
  public Set<Entry<TypeDescription, Integer>> entrySet() {
    return idMap.entrySet();
  }

  public ByteBuffer serialize() {
    StringBuilder buffer = new StringBuilder();
    boolean needComma = false;
    for (TypeDescription key : idMap.keySet()) {
      if (needComma) {
        buffer.append(',');
      } else {
        needComma = true;
      }
      buffer.append(key.getId());
      buffer.append(':');
      buffer.append(idMap.get(key).intValue());
    }
    return ByteBuffer.wrap(buffer.toString().getBytes(StandardCharsets.UTF_8));
  }

  public static ColumnIdMap deserialize(TypeDescription schema,
                                        ByteBuffer serial) {
    ColumnIdMap result = new ColumnIdMap();
    String[] parts = StandardCharsets.UTF_8.decode(serial).toString().split(",");
    for (int i = 0; i < parts.length; ++i) {
      String[] subparts = parts[i].split(":");
      result.put(schema.findSubtype(Integer.parseInt(subparts[0])),
          Integer.parseInt(subparts[1]));
    }
    return result;
  }
}
