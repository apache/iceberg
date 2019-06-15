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

import com.google.common.base.Objects;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.orc.TypeDescription;

/**
 * The mapping from ORC's TypeDescription to the Iceberg column ids.
 * <p>
 * Keep the API limited to Map rather than a concrete type so that we can
 * change it later.
 */
public final class ColumnMap implements Map<TypeDescription, ColumnMap.IcebergColumn> {

  private final IdentityHashMap<TypeDescription, IcebergColumn> idMap =
      new IdentityHashMap<>();

  static class IcebergColumn {

    private final int id;
    private final boolean required;

    private IcebergColumn(int id, boolean isRequired) {
      this.id = id;
      this.required = isRequired;
    }

    public boolean isRequired() {
      return required;
    }

    public int getId() {
      return id;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(this.id, this.required);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj instanceof IcebergColumn) {
        IcebergColumn other = (IcebergColumn) obj;
        return Objects.equal(this.id, other.getId()) &&
            Objects.equal(this.required, other.isRequired());
      }
      return false;
    }
  }

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
  public IcebergColumn get(Object key) {
    return idMap.get(key);
  }

  @Override
  public IcebergColumn put(TypeDescription key, IcebergColumn value) {
    return idMap.put(key, value);
  }

  @Override
  public IcebergColumn remove(Object key) {
    return idMap.remove(key);
  }

  @Override
  public void putAll(Map<? extends TypeDescription, ? extends IcebergColumn> map) {
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
  public Collection<IcebergColumn> values() {
    return idMap.values();
  }

  @Override
  public Set<Entry<TypeDescription, IcebergColumn>> entrySet() {
    return idMap.entrySet();
  }

  public Map<IcebergColumn, TypeDescription> inverse() {
    return idMap.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
  }

  ByteBuffer serialize() {
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
      buffer.append(idMap.get(key).getId());
      buffer.append(':');
      buffer.append(idMap.get(key).isRequired());
    }
    return ByteBuffer.wrap(buffer.toString().getBytes(StandardCharsets.UTF_8));
  }

  static ColumnMap deserialize(TypeDescription schema,
                                      ByteBuffer serial) {
    ColumnMap result = new ColumnMap();
    String[] parts = StandardCharsets.UTF_8.decode(serial).toString().split(",");
    for (int i = 0; i < parts.length; ++i) {
      String[] subparts = parts[i].split(":");
      boolean isRequired = (subparts.length == 3) && Boolean.parseBoolean(subparts[2]);
      result.put(schema.findSubtype(Integer.parseInt(subparts[0])),
          newIcebergColumn(Integer.parseInt(subparts[1]), isRequired));
    }
    return result;
  }

  static IcebergColumn newIcebergColumn(int id, boolean isRequired) {
    return new IcebergColumn(id, isRequired);
  }
}
