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
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.Pair;

public class CombinedRecord implements Record, StructLike {
  // Cache to address the column values based on the field name or id.
  private static final LoadingCache<
          Pair<StructType, Integer[][]>, Map<Object, Pair<Integer, Integer>>>
      COMBINER_CACHE =
          Caffeine.newBuilder()
              .weakKeys()
              .build(
                  key -> {
                    Map<Object, Pair<Integer, Integer>> nameAndIdToPos = Maps.newHashMap();

                    // Populate the map with field names and their corresponding record and field
                    // positions.
                    for (int recordId = 0; recordId < key.second().length; recordId += 1) {
                      for (int recordFieldPos = 0;
                          recordFieldPos < key.second()[recordId].length;
                          recordFieldPos += 1) {
                        Types.NestedField field =
                            key.first().field(key.second()[recordId][recordFieldPos]);
                        nameAndIdToPos.put(field.name(), Pair.of(recordId, recordFieldPos));
                      }
                    }

                    // Populate the map with field ids and their corresponding record and field
                    // positions.
                    for (int fieldPos = 0; fieldPos < key.first().fields().size(); fieldPos += 1) {
                      nameAndIdToPos.put(
                          fieldPos, nameAndIdToPos.get(key.first().fields().get(fieldPos).name()));
                    }

                    return nameAndIdToPos;
                  });

  public static CombinedRecord create(Schema schema, Integer[]... families) {
    return new CombinedRecord(schema.asStruct(), families);
  }

  public static CombinedRecord clone(CombinedRecord toClone) {
    return new CombinedRecord(toClone);
  }

  private final StructType struct;
  private final Integer[][] families;
  private final int size;
  private final Map<Object, Pair<Integer, Integer>> nameAndIdToPos;
  private final Record[] values;

  private CombinedRecord(CombinedRecord toClone) {
    this.struct = toClone.struct;
    this.families = toClone.families;
    this.size = toClone.size;
    this.nameAndIdToPos = toClone.nameAndIdToPos;
    this.values = new Record[families.length];
  }

  private CombinedRecord(StructType struct, Integer[][] families) {
    this.struct = struct;
    this.families = families;
    this.size = struct.fields().size();
    this.nameAndIdToPos = COMBINER_CACHE.get(Pair.of(struct, families));
    this.values = new Record[families.length];
  }

  public void setFamily(int recordPos, Record value) {
    Preconditions.checkArgument(
        recordPos >= 0 && recordPos < families.length,
        "Position out of bounds: %s (size: %s)",
        recordPos,
        families.length);
    Preconditions.checkArgument(
        value.struct().fields().size() >= families[recordPos].length,
        "Cannot set value with struct %s at position %s, expected minimal size is %s",
        value.struct(),
        recordPos,
        families[recordPos].length);
    values[recordPos] = value;
  }

  @Override
  public StructType struct() {
    return struct;
  }

  @Override
  public Object getField(String name) {
    Pair<Integer, Integer> internalPos = nameAndIdToPos.get(name);
    if (internalPos != null) {
      return values[internalPos.first()].get(internalPos.second());
    }

    return null;
  }

  @Override
  public void setField(String name, Object value) {
    Pair<Integer, Integer> internalPos = nameAndIdToPos.get(name);
    Preconditions.checkArgument(internalPos != null, "Cannot set unknown field named: %s", name);
    values[internalPos.first()].set(internalPos.second(), value);
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public Object get(int pos) {
    Pair<Integer, Integer> internalPos = nameAndIdToPos.get(pos);
    return values[internalPos.first()].get(internalPos.second());
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
    Pair<Integer, Integer> internalPos = nameAndIdToPos.get(pos);
    Preconditions.checkArgument(internalPos != null, "Cannot set unknown field with id: %s", pos);
    values[internalPos.first()].set(internalPos.second(), value);
  }

  @Override
  public Record copy() {
    return copy(ImmutableMap.of());
  }

  @Override
  public Record copy(Map<String, Object> overwriteValues) {
    GenericRecord copy = GenericRecord.create(this.struct);
    for (int i = 0; i < struct.fields().size(); i += 1) {
      Object overwriteValue = overwriteValues.get(struct.fields().get(i).name());
      copy.set(i, overwriteValue != null ? overwriteValue : get(i));
    }

    return copy;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("CombinedRecord(");
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
    } else if (!(other instanceof CombinedRecord)) {
      return false;
    }

    CombinedRecord that = (CombinedRecord) other;
    return Arrays.deepEquals(this.values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode((Object[]) values);
  }
}
