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
package org.apache.iceberg.stats;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class BaseContentStats implements ContentStats, StructLike, Serializable {

  private final List<FieldStats<?>> fieldStats;

  public BaseContentStats(Types.StructType projection) {
    this.fieldStats = Lists.newArrayListWithCapacity(projection.fields().size());
    for (int i = 0; i < projection.fields().size(); i++) {
      Types.NestedField field = projection.fields().get(i);
      Preconditions.checkArgument(
          field.type().isStructType(), "ColumnStats must contain structs: %s", field.type());
      Types.StructType structType = field.type().asStructType();
      Type type =
          null != structType.field("lower_bound")
              ? structType.field("lower_bound").type()
              : null != structType.field("upper_bound")
                  ? structType.field("upper_bound").type()
                  : null;
      fieldStats.add(
          BaseFieldStats.builder()
              .fieldId(StatsUtil.fieldIdForStatsField(field.fieldId()))
              .type(type)
              .build());
    }
  }

  private BaseContentStats(List<FieldStats<?>> fieldStats) {
    this.fieldStats = Lists.newArrayList(fieldStats);
  }

  @Override
  public List<FieldStats<?>> fieldStats() {
    return fieldStats;
  }

  @Override
  public int size() {
    return fieldStats.size();
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    if (pos > fieldStats().size() - 1) {
      return null;
    }

    return javaClass.cast(fieldStats.get(pos));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public <T> void set(int pos, T value) {
    if (value instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) value;
      FieldStats<?> stat = fieldStats.get(pos);
      BaseFieldStats.Builder builder = BaseFieldStats.buildFrom(stat);
      Type type = stat.type();
      if (null != record.getField("column_size")) {
        builder.columnSize((Long) record.getField("column_size"));
      }

      if (null != record.getField("value_count")) {
        builder.valueCount((Long) record.getField("value_count"));
      }

      if (null != record.getField("nan_value_count")) {
        builder.nanValueCount((Long) record.getField("nan_value_count"));
      }

      if (null != record.getField("null_value_count")) {
        builder.nullValueCount((Long) record.getField("null_value_count"));
      }

      if (null != type && null != record.getField("lower_bound")) {
        builder.lowerBound(type.typeId().javaClass().cast(record.getField("lower_bound")));
      }

      if (null != type && null != record.getField("upper_bound")) {
        builder.upperBound(type.typeId().javaClass().cast(record.getField("upper_bound")));
      }

      BaseFieldStats<?> newStat = builder.build();
      fieldStats.set(pos, newStat);
    } else {
      fieldStats.set(pos, (FieldStats<?>) value);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("fieldStats", fieldStats).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof BaseContentStats)) {
      return false;
    }

    BaseContentStats that = (BaseContentStats) o;
    return Objects.equals(fieldStats, that.fieldStats);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(fieldStats);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder buildFrom(ContentStats stats) {
    return builder().withFieldStats(stats.fieldStats());
  }

  public static Builder buildFrom(ContentStats stats, Set<Integer> requestedColumnIds) {
    if (null == requestedColumnIds) {
      return buildFrom(stats);
    }

    return builder()
        .withFieldStats(
            stats.fieldStats().stream()
                .filter(stat -> requestedColumnIds.contains(stat.fieldId()))
                .collect(Collectors.toList()));
  }

  public static class Builder {
    private final List<FieldStats<?>> stats = Lists.newArrayList();

    private Builder() {}

    public Builder withFieldStats(FieldStats<?> fieldStats) {
      stats.add(fieldStats);
      return this;
    }

    public Builder withFieldStats(List<FieldStats<?>> fieldStats) {
      stats.addAll(fieldStats);
      return this;
    }

    public BaseContentStats build() {
      return new BaseContentStats(stats);
    }
  }
}
