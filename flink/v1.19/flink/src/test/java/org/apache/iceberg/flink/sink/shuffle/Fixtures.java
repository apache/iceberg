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
package org.apache.iceberg.flink.sink.shuffle;

import java.util.Comparator;
import java.util.Map;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderComparators;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;

class Fixtures {
  private Fixtures() {}

  public static final int NUM_SUBTASKS = 2;
  public static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.StringType.get()),
          Types.NestedField.optional(2, "number", Types.IntegerType.get()));
  public static final RowType ROW_TYPE = RowType.of(new VarCharType(), new IntType());
  public static final TypeSerializer<RowData> ROW_SERIALIZER = new RowDataSerializer(ROW_TYPE);
  public static final RowDataWrapper ROW_WRAPPER = new RowDataWrapper(ROW_TYPE, SCHEMA.asStruct());
  public static final SortOrder SORT_ORDER = SortOrder.builderFor(SCHEMA).asc("id").build();
  public static final Comparator<StructLike> SORT_ORDER_COMPARTOR =
      SortOrderComparators.forSchema(SCHEMA, SORT_ORDER);
  public static final SortKeySerializer SORT_KEY_SERIALIZER =
      new SortKeySerializer(SCHEMA, SORT_ORDER);
  public static final DataStatisticsSerializer TASK_STATISTICS_SERIALIZER =
      new DataStatisticsSerializer(SORT_KEY_SERIALIZER);
  public static final AggregatedStatisticsSerializer AGGREGATED_STATISTICS_SERIALIZER =
      new AggregatedStatisticsSerializer(SORT_KEY_SERIALIZER);

  public static final SortKey SORT_KEY = new SortKey(SCHEMA, SORT_ORDER);
  public static final Map<String, SortKey> CHAR_KEYS = createCharKeys();

  public static StatisticsEvent createStatisticsEvent(
      StatisticsType type,
      TypeSerializer<DataStatistics> statisticsSerializer,
      long checkpointId,
      SortKey... keys) {
    DataStatistics statistics = createTaskStatistics(type, keys);
    return StatisticsEvent.createTaskStatisticsEvent(
        checkpointId, statistics, statisticsSerializer);
  }

  public static DataStatistics createTaskStatistics(StatisticsType type, SortKey... keys) {
    DataStatistics statistics;
    if (type == StatisticsType.Sketch) {
      statistics = new SketchDataStatistics(128);
    } else {
      statistics = new MapDataStatistics();
    }

    for (SortKey key : keys) {
      statistics.add(key);
    }

    return statistics;
  }

  private static Map<String, SortKey> createCharKeys() {
    Map<String, SortKey> keys = Maps.newHashMap();
    for (char c = 'a'; c <= 'z'; ++c) {
      String key = Character.toString(c);
      SortKey sortKey = SORT_KEY.copy();
      sortKey.set(0, key);
      keys.put(key, sortKey);
    }

    return keys;
  }
}
