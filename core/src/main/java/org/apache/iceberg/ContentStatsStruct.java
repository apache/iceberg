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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

class ContentStatsStruct implements ContentStats, StructLike, Serializable {
  private final Map<Integer, FieldStats<?>> idToFieldStats = Maps.newHashMap();
  private final Types.StructType struct;
  private final int[] posToId;

  ContentStatsStruct(Types.StructType struct) {
    this.struct = struct;
    this.posToId = posToId(struct);
  }

  private ContentStatsStruct(ContentStatsStruct toCopy, Set<Integer> fieldIds) {
    this(fieldIds != null ? TypeUtil.select(toCopy.struct, statsIds(fieldIds)) : toCopy.struct);
    if (fieldIds != null) {
      for (int fieldId : fieldIds) {
        FieldStats<?> fieldToCopy = toCopy.idToFieldStats.get(fieldId);
        if (fieldToCopy != null) {
          idToFieldStats.put(fieldId, fieldToCopy.copy());
        }
      }
    } else {
      for (Map.Entry<Integer, FieldStats<?>> entry : toCopy.idToFieldStats.entrySet()) {
        idToFieldStats.put(entry.getKey(), entry.getValue().copy());
      }
    }
  }

  @Override
  public Iterable<FieldStats<?>> fieldStats() {
    return idToFieldStats.values();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> FieldStats<T> statsFor(int id) {
    return (FieldStats<T>) idToFieldStats.get(id);
  }

  public <T> void setStats(int id, FieldStats<T> fieldStats) {
    Preconditions.checkArgument(
        struct.field(StatsUtil.toBaseId(id)) != null,
        "Cannot set stats for unknown field ID: %s",
        id);
    Preconditions.checkArgument(
        id == fieldStats.fieldId(),
        "Mismatched field stats for ID %s: actual ID %s",
        id,
        fieldStats.fieldId());

    idToFieldStats.put(id, fieldStats);
  }

  @Override
  public Types.StructType type() {
    return struct;
  }

  @Override
  public int size() {
    return struct.fields().size();
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    return javaClass.cast(idToFieldStats.get(posToId[pos]));
  }

  @Override
  public <T> void set(int pos, T value) {
    idToFieldStats.put(posToId[pos], (FieldStats<?>) value);
  }

  @Override
  public ContentStatsStruct copy() {
    return new ContentStatsStruct(this, null);
  }

  @Override
  public ContentStatsStruct copy(Set<Integer> fieldIds) {
    Preconditions.checkArgument(
        fieldIds != null && !fieldIds.isEmpty(), "Invalid ID set to copy: %s", fieldIds);

    return new ContentStatsStruct(this, fieldIds);
  }

  private static int[] posToId(Types.StructType struct) {
    List<Types.NestedField> fields = struct.fields();
    int[] posToId = new int[fields.size()];
    for (int i = 0; i < posToId.length; i += 1) {
      int statsFieldId = fields.get(i).fieldId();
      posToId[i] = StatsUtil.toFieldId(statsFieldId);
    }

    return posToId;
  }

  private static Set<Integer> statsIds(Set<Integer> fieldIds) {
    return fieldIds.stream().map(StatsUtil::toBaseId).collect(Collectors.toSet());
  }
}
