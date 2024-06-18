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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestMapDataStatistics {
  private final SortOrder sortOrder = SortOrder.builderFor(TestFixtures.SCHEMA).asc("data").build();
  private final SortKey sortKey = new SortKey(TestFixtures.SCHEMA, sortOrder);
  private final RowType rowType = FlinkSchemaUtil.convert(TestFixtures.SCHEMA);
  private final RowDataWrapper rowWrapper =
      new RowDataWrapper(rowType, TestFixtures.SCHEMA.asStruct());

  @Test
  public void testAddsAndGet() {
    MapDataStatistics dataStatistics = new MapDataStatistics();

    GenericRowData reusedRow =
        GenericRowData.of(StringData.fromString("a"), 1, StringData.fromString("2023-06-20"));
    sortKey.wrap(rowWrapper.wrap(reusedRow));
    dataStatistics.add(sortKey);

    reusedRow.setField(0, StringData.fromString("b"));
    sortKey.wrap(rowWrapper.wrap(reusedRow));
    dataStatistics.add(sortKey);

    reusedRow.setField(0, StringData.fromString("c"));
    sortKey.wrap(rowWrapper.wrap(reusedRow));
    dataStatistics.add(sortKey);

    reusedRow.setField(0, StringData.fromString("b"));
    sortKey.wrap(rowWrapper.wrap(reusedRow));
    dataStatistics.add(sortKey);

    reusedRow.setField(0, StringData.fromString("a"));
    sortKey.wrap(rowWrapper.wrap(reusedRow));
    dataStatistics.add(sortKey);

    reusedRow.setField(0, StringData.fromString("b"));
    sortKey.wrap(rowWrapper.wrap(reusedRow));
    dataStatistics.add(sortKey);

    Map<SortKey, Long> actual = dataStatistics.statistics();

    rowWrapper.wrap(
        GenericRowData.of(StringData.fromString("a"), 1, StringData.fromString("2023-06-20")));
    sortKey.wrap(rowWrapper);
    SortKey keyA = sortKey.copy();

    rowWrapper.wrap(
        GenericRowData.of(StringData.fromString("b"), 1, StringData.fromString("2023-06-20")));
    sortKey.wrap(rowWrapper);
    SortKey keyB = sortKey.copy();

    rowWrapper.wrap(
        GenericRowData.of(StringData.fromString("c"), 1, StringData.fromString("2023-06-20")));
    sortKey.wrap(rowWrapper);
    SortKey keyC = sortKey.copy();

    Map<SortKey, Long> expected = ImmutableMap.of(keyA, 2L, keyB, 3L, keyC, 1L);
    assertThat(actual).isEqualTo(expected);
  }
}
