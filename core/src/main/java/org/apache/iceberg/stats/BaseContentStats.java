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
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

// TODO: Add an API that projects only single stats fields
// e.g. project only lowerBound for columnX
public class BaseContentStats implements ContentStats, StructLike, Serializable {

  private final List<Statistic> statistics;
  private final Map<Integer, Statistic> statisticsById = Maps.newLinkedHashMap();
  private long recordCount = -1L;

  public BaseContentStats(Types.StructType projection) {
    this.statistics = Lists.newArrayListWithCapacity(projection.fields().size());
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
      statistics.add(
          BaseStatistic.builder().columnId(Integer.parseInt(field.name())).type(type).build());
    }
  }

  private BaseContentStats(long recordCount, List<Statistic> statistics) {
    this.recordCount = recordCount;
    this.statistics = Lists.newArrayList(statistics);
  }

  @Override
  public List<Statistic> statistics() {
    return statistics;
  }

  @Override
  public long recordCount() {
    return recordCount;
  }

  @Override
  public Statistic statsFor(int columnId) {
    if (statisticsById.isEmpty() && !statistics.isEmpty()) {
      statistics.stream()
          .filter(Objects::nonNull)
          .forEach(stat -> statisticsById.put(stat.columnId(), stat));
    }

    return statisticsById.get(columnId);
  }

  @Override
  public int size() {
    return statistics.size();
  }

  @Override
  public <T> T get(int basePos, Class<T> javaClass) {
    if (basePos > statistics().size() - 1) {
      return null;
    }

    return javaClass.cast(statistics.get(basePos));
  }

  @Override
  public <T> void set(int pos, T value) {
    if (value instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) value;
      BaseStatistic stat = (BaseStatistic) statistics.get(pos);
      BaseStatistic.Builder builder = BaseStatistic.buildFrom(stat);
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
      if (null != record.getField("lower_bound")) {
        Object lowerBound = record.getField("lower_bound");
        if (null != type) {
          builder.lowerBound(type.typeId().javaClass().cast(lowerBound));
        }
      }
      if (null != record.getField("upper_bound")) {
        Object upperBound = record.getField("upper_bound");
        if (null != type) {
          builder.upperBound(type.typeId().javaClass().cast(upperBound));
        }
      }

      BaseStatistic newStat = builder.build();
      statistics.set(pos, newStat);
      statisticsById.put(newStat.columnId(), newStat);
    } else {
      statistics.set(pos, (Statistic) value);
    }
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", BaseContentStats.class.getSimpleName() + "[", "]")
        .add("recordCount=" + recordCount)
        .add("statistics=" + statistics)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof BaseContentStats)) {
      return false;
    }
    BaseContentStats that = (BaseContentStats) o;
    return recordCount == that.recordCount && Objects.equals(statistics, that.statistics);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statistics, recordCount);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder buildFrom(ContentStats stats) {
    return builder().withStatistics(stats.statistics()).recordCount(stats.recordCount());
  }

  public static Builder buildFrom(ContentStats stats, Set<Integer> requestedColumnIds) {
    if (null == requestedColumnIds) {
      return buildFrom(stats);
    }

    return builder()
        .withStatistics(
            stats.statistics().stream()
                .filter(stat -> requestedColumnIds.contains(stat.columnId()))
                .collect(Collectors.toList()))
        .recordCount(stats.recordCount());
  }

  public static class Builder {
    private final List<Statistic> stats = Lists.newArrayList();
    private long recordCount = -1L;

    private Builder() {}

    public Builder withStatistic(Statistic statistic) {
      stats.add(statistic);
      return this;
    }

    public Builder withStatistics(List<Statistic> statistics) {
      stats.addAll(statistics);
      return this;
    }

    public Builder recordCount(long newRecordCount) {
      this.recordCount = newRecordCount;
      return this;
    }

    public BaseContentStats build() {
      return new BaseContentStats(recordCount, stats);
    }
  }
}
