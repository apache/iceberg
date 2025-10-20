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

import static org.apache.iceberg.flink.sink.shuffle.Fixtures.CHAR_KEYS;
import static org.apache.iceberg.flink.sink.shuffle.Fixtures.NUM_SUBTASKS;
import static org.apache.iceberg.flink.sink.shuffle.Fixtures.ROW_SERIALIZER;
import static org.apache.iceberg.flink.sink.shuffle.Fixtures.SORT_ORDER_COMPARTOR;
import static org.apache.iceberg.flink.sink.shuffle.Fixtures.STATISTICS_OR_RECORD_SERIALIZER;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class TestStatisticsOrRecordSerializer extends SerializerTestBase<StatisticsOrRecord> {

  @Override
  protected TypeSerializer<StatisticsOrRecord> createSerializer() {
    return STATISTICS_OR_RECORD_SERIALIZER;
  }

  @Override
  protected int getLength() {
    return -1;
  }

  @Override
  protected Class<StatisticsOrRecord> getTypeClass() {
    return StatisticsOrRecord.class;
  }

  @Override
  protected StatisticsOrRecord[] getTestData() {
    return new StatisticsOrRecord[] {
      // use BinaryRowData as row data serializer only deserialize to BinaryRowData
      StatisticsOrRecord.fromRecord(
          ROW_SERIALIZER.toBinaryRow(GenericRowData.of(StringData.fromString("key1"), 100))),
      // map global statistics
      StatisticsOrRecord.fromStatistics(
          GlobalStatistics.fromMapAssignment(
              1L,
              MapAssignment.fromKeyFrequency(
                  NUM_SUBTASKS,
                  ImmutableMap.of(CHAR_KEYS.get("a"), 1L, CHAR_KEYS.get("b"), 2L),
                  0.0d,
                  SORT_ORDER_COMPARTOR))),
      // range bound global statistics
      StatisticsOrRecord.fromStatistics(
          GlobalStatistics.fromRangeBounds(
              2L, new SortKey[] {CHAR_KEYS.get("a"), CHAR_KEYS.get("b")}))
    };
  }
}
