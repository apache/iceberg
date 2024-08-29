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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionStats;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;

/**
 * Wraps the {@link PartitionStats} as {@link Record}. Used by generic file format writers and
 * readers.
 */
public class PartitionStatsRecord implements Record, StructLike {
  private static final LoadingCache<StructType, Map<String, Integer>> NAME_MAP_CACHE =
      Caffeine.newBuilder()
          .weakKeys()
          .build(
              struct -> {
                Map<String, Integer> idToPos = Maps.newHashMap();
                List<Types.NestedField> fields = struct.fields();
                for (int index = 0; index < fields.size(); index += 1) {
                  idToPos.put(fields.get(index).name(), index);
                }
                return idToPos;
              });

  private final StructType struct;
  private final PartitionStats partitionStats;
  private final Map<String, Integer> nameToPos;

  public static PartitionStatsRecord create(Schema schema, PartitionStats partitionStats) {
    return new PartitionStatsRecord(schema.asStruct(), partitionStats);
  }

  public static PartitionStatsRecord create(StructType struct, PartitionStats partitionStats) {
    return new PartitionStatsRecord(struct, partitionStats);
  }

  public PartitionStats unwrap() {
    return partitionStats;
  }

  private PartitionStatsRecord(StructType struct, PartitionStats partitionStats) {
    this.struct = struct;
    this.partitionStats = partitionStats;
    this.nameToPos = NAME_MAP_CACHE.get(struct);
  }

  private PartitionStatsRecord(PartitionStatsRecord toCopy) {
    this.struct = toCopy.struct;
    this.partitionStats = toCopy.partitionStats;
    this.nameToPos = toCopy.nameToPos;
  }

  private PartitionStatsRecord(PartitionStatsRecord toCopy, Map<String, Object> overwrite) {
    this.struct = toCopy.struct;
    this.partitionStats = toCopy.partitionStats;
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
      return partitionStats.get(pos, Object.class);
    }

    return null;
  }

  @Override
  public void setField(String name, Object value) {
    Integer pos = nameToPos.get(name);
    Preconditions.checkArgument(pos != null, "Cannot set unknown field named: %s", name);
    partitionStats.set(pos, value);
  }

  @Override
  public int size() {
    return partitionStats.size();
  }

  @Override
  public Object get(int pos) {
    return partitionStats.get(pos, Object.class);
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
    partitionStats.set(pos, value);
  }

  @Override
  public PartitionStatsRecord copy() {
    return new PartitionStatsRecord(this);
  }

  @Override
  public PartitionStatsRecord copy(Map<String, Object> overwriteValues) {
    return new PartitionStatsRecord(this, overwriteValues);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Record(");
    for (int index = 0; index < partitionStats.size(); index += 1) {
      if (index != 0) {
        sb.append(", ");
      }
      sb.append(partitionStats.get(index, Object.class));
    }
    sb.append(")");
    return sb.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof PartitionStatsRecord)) {
      return false;
    }

    PartitionStatsRecord that = (PartitionStatsRecord) other;
    return this.partitionStats.equals(that.partitionStats);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(partitionStats);
  }
}
