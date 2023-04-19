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
package org.apache.iceberg.data;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;

public class GenericRecord implements Record, StructLike {
  private static final LoadingCache<StructType, Map<String, Integer>> NAME_MAP_CACHE =
      Caffeine.newBuilder()
          .weakKeys()
          .build(
              struct -> {
                Map<String, Integer> idToPos = Maps.newHashMap();
                List<Types.NestedField> fields = struct.fields();
                for (int i = 0; i < fields.size(); i += 1) {
                  idToPos.put(fields.get(i).name(), i);
                }
                return idToPos;
              });

  public static GenericRecord create(Schema schema) {
    return new GenericRecord(schema.asStruct());
  }

  public static GenericRecord create(StructType struct) {
    return new GenericRecord(struct);
  }

  private final StructType struct;
  private final int size;
  private final Object[] values;
  private final Map<String, Integer> nameToPos;

  private GenericRecord(StructType struct) {
    this.struct = struct;
    this.size = struct.fields().size();
    this.values = new Object[size];
    this.nameToPos = NAME_MAP_CACHE.get(struct);
  }

  private GenericRecord(GenericRecord toCopy) {
    this.struct = toCopy.struct;
    this.size = toCopy.size;
    this.values = Arrays.copyOf(toCopy.values, toCopy.values.length);
    this.nameToPos = toCopy.nameToPos;
  }

  private GenericRecord(GenericRecord toCopy, Map<String, Object> overwrite) {
    this.struct = toCopy.struct;
    this.size = toCopy.size;
    this.values = Arrays.copyOf(toCopy.values, toCopy.values.length);
    this.nameToPos = toCopy.nameToPos;
    for (Map.Entry<String, Object> entry : overwrite.entrySet()) {
      setField(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public StructType struct() {
    return struct;
  }

  @Override
  public Object getField(String name) {
    Integer pos = nameToPos.get(name);
    if (pos != null) {
      return values[pos];
    }

    return null;
  }

  @Override
  public void setField(String name, Object value) {
    Integer pos = nameToPos.get(name);
    Preconditions.checkArgument(pos != null, "Cannot set unknown field named: %s", name);
    values[pos] = value;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public Object get(int pos) {
    return values[pos];
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    Object value = get(pos);
    if (value == null || javaClass.isInstance(value)) {
      return javaClass.cast(value);
    } else {
      throw new IllegalStateException("Not an instance of " + javaClass.getName() + ": " + value);
    }
  }

  @Override
  public <T> void set(int pos, T value) {
    values[pos] = value;
  }

  @Override
  public GenericRecord copy() {
    return new GenericRecord(this);
  }

  @Override
  public GenericRecord copy(Map<String, Object> overwriteValues) {
    return new GenericRecord(this, overwriteValues);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Record(");
    for (int i = 0; i < values.length; i += 1) {
      if (i != 0) {
        sb.append(", ");
      }
      sb.append(values[i]);
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof GenericRecord)) {
      return false;
    }

    GenericRecord that = (GenericRecord) other;
    return Arrays.deepEquals(this.values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(values);
  }
}
