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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class BaseContentStats implements ContentStats, Serializable {

  private final List<FieldStats<?>> fieldStats;
  private final Map<Integer, FieldStats<?>> fieldStatsById;
  private final Types.StructType statsStruct;

  /** Used by Avro reflection to instantiate this class when reading manifest files. */
  public BaseContentStats(Types.StructType projection) {
    this.statsStruct = projection;
    this.fieldStats = Lists.newArrayListWithCapacity(projection.fields().size());
    this.fieldStatsById = Maps.newLinkedHashMapWithExpectedSize(projection.fields().size());
    for (int i = 0; i < projection.fields().size(); i++) {
      Types.NestedField field = projection.fields().get(i);
      Preconditions.checkArgument(
          field.type().isStructType(), "Field stats must be a struct type: %s", field.type());
      Types.StructType structType = field.type().asStructType();
      Type type = null;
      if (null != structType.field("lower_bound")) {
        type = structType.field("lower_bound").type();
      } else if (null != structType.field("upper_bound")) {
        type = structType.field("upper_bound").type();
      }

      fieldStats.add(
          BaseFieldStats.builder()
              .fieldId(StatsUtil.fieldIdForStatsField(field.fieldId()))
              .type(type)
              .build());
    }
  }

  private BaseContentStats(Types.StructType struct, List<FieldStats<?>> fieldStats) {
    this.statsStruct = struct;
    this.fieldStats = Lists.newArrayList(fieldStats);
    this.fieldStatsById = Maps.newLinkedHashMapWithExpectedSize(fieldStats.size());
  }

  @Override
  public List<FieldStats<?>> fieldStats() {
    return fieldStats;
  }

  @Override
  public Types.StructType statsStruct() {
    return statsStruct;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> FieldStats<T> statsFor(int fieldId) {
    if (fieldStatsById.isEmpty() && !fieldStats.isEmpty()) {
      fieldStats.stream()
          .filter(Objects::nonNull)
          .forEach(stat -> fieldStatsById.put(stat.fieldId(), stat));
    }

    return (FieldStats<T>) fieldStatsById.get(fieldId);
  }

  @Override
  public int size() {
    return fieldStats.size();
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    if (pos > statsStruct.fields().size() - 1) {
      // return null in case there are more stats schemas than actual stats available as Avro calls
      // get() for all available stats schemas of a given table
      return null;
    }

    int statsFieldId = statsStruct.fields().get(pos).fieldId();
    FieldStats<?> value = statsFor(StatsUtil.fieldIdForStatsField(statsFieldId));
    if (value == null || javaClass.isInstance(value)) {
      return javaClass.cast(value);
    }

    throw new IllegalArgumentException(
        String.format(
            "Wrong class, expected %s but was %s for object: %s",
            javaClass.getName(), value.getClass().getName(), value));
  }

  @SuppressWarnings({"unchecked", "rawtypes", "CyclomaticComplexity"})
  @Override
  public <T> void set(int pos, T value) {
    if (value instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) value;
      FieldStats<?> stat = fieldStats.get(pos);
      BaseFieldStats.Builder builder = BaseFieldStats.buildFrom(stat);
      Type type = stat.type();
      if (null != record.getField(FieldStatistic.VALUE_COUNT.fieldName())) {
        builder.valueCount((Long) record.getField(FieldStatistic.VALUE_COUNT.fieldName()));
      }

      if (null != record.getField(FieldStatistic.NAN_VALUE_COUNT.fieldName())) {
        builder.nanValueCount((Long) record.getField(FieldStatistic.NAN_VALUE_COUNT.fieldName()));
      }

      if (null != record.getField(FieldStatistic.NULL_VALUE_COUNT.fieldName())) {
        builder.nullValueCount((Long) record.getField(FieldStatistic.NULL_VALUE_COUNT.fieldName()));
      }

      if (null != record.getField(FieldStatistic.AVG_VALUE_SIZE.fieldName())) {
        builder.avgValueSize((Integer) record.getField(FieldStatistic.AVG_VALUE_SIZE.fieldName()));
      }

      if (null != record.getField(FieldStatistic.MAX_VALUE_SIZE.fieldName())) {
        builder.maxValueSize((Integer) record.getField(FieldStatistic.MAX_VALUE_SIZE.fieldName()));
      }

      Object lowerBound = record.getField(FieldStatistic.LOWER_BOUND.fieldName());
      if (null != type && null != lowerBound) {
        Preconditions.checkArgument(
            type.typeId().javaClass().isInstance(lowerBound),
            "Invalid lower bound type, expected a subtype of %s: %s",
            type.typeId().javaClass(),
            lowerBound.getClass().getName());
        builder.lowerBound(type.typeId().javaClass().cast(lowerBound));
      }

      Object upperBound = record.getField(FieldStatistic.UPPER_BOUND.fieldName());
      if (null != type && null != upperBound) {
        Preconditions.checkArgument(
            type.typeId().javaClass().isInstance(upperBound),
            "Invalid upper bound type, expected a subtype of %s: %s",
            type.typeId().javaClass(),
            upperBound.getClass().getName());
        builder.upperBound(type.typeId().javaClass().cast(upperBound));
      }

      if (null != record.getField(FieldStatistic.IS_EXACT.fieldName())) {
        Boolean isExact = (Boolean) record.getField(FieldStatistic.IS_EXACT.fieldName());
        builder.isExact(null != isExact && isExact);
      }

      BaseFieldStats<?> newStat = builder.build();
      fieldStats.set(pos, newStat);
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
    return Objects.equals(fieldStats, that.fieldStats)
        && Objects.equals(statsStruct, that.statsStruct);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldStats, statsStruct);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder buildFrom(ContentStats stats) {
    return builder().withStatsStruct(stats.statsStruct()).withFieldStats(stats.fieldStats());
  }

  public static Builder buildFrom(ContentStats stats, Set<Integer> requestedColumnIds) {
    if (null == requestedColumnIds) {
      return buildFrom(stats);
    }

    return builder()
        .withStatsStruct(stats.statsStruct())
        .withFieldStats(
            stats.fieldStats().stream()
                .filter(stat -> requestedColumnIds.contains(stat.fieldId()))
                .collect(Collectors.toList()));
  }

  public static class Builder {
    private final List<FieldStats<?>> stats = Lists.newArrayList();
    private Types.StructType statsStruct;
    private Schema schema;

    private Builder() {}

    public Builder withStatsStruct(Types.StructType struct) {
      this.statsStruct = struct;
      return this;
    }

    public Builder withTableSchema(Schema tableSchema) {
      this.schema = tableSchema;
      return this;
    }

    public Builder withFieldStats(FieldStats<?> fieldStats) {
      stats.add(fieldStats);
      return this;
    }

    public Builder withFieldStats(List<FieldStats<?>> fieldStats) {
      stats.addAll(fieldStats);
      return this;
    }

    public BaseContentStats build() {
      Preconditions.checkArgument(
          null != statsStruct || null != schema, "Either stats struct or table schema must be set");
      Preconditions.checkArgument(
          null == statsStruct || null == schema, "Cannot set stats struct and table schema");
      if (null != schema) {
        this.statsStruct = StatsUtil.contentStatsFor(schema).type().asStructType();
      }

      return new BaseContentStats(statsStruct, stats);
    }
  }
}
