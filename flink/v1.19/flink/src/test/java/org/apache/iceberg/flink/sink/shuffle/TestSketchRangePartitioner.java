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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.TestFixtures;
import org.junit.jupiter.api.Test;

public class TestSketchRangePartitioner {
  // sort on the long id field
  private static final SortOrder SORT_ORDER =
      SortOrder.builderFor(TestFixtures.SCHEMA).asc("id").build();
  private static final SortKey SORT_KEY = new SortKey(TestFixtures.SCHEMA, SORT_ORDER);
  private static final RowType ROW_TYPE = FlinkSchemaUtil.convert(TestFixtures.SCHEMA);
  private static final int NUM_PARTITIONS = 16;
  private static final long RANGE_STEP = 1_000;
  private static final long MAX_ID = RANGE_STEP * NUM_PARTITIONS;
  private static final SortKey[] RANGE_BOUNDS = createRangeBounds();

  /**
   * To understand how range bounds are used in range partitioning, here is an example for human
   * ages with 4 partitions: [15, 32, 60]. The 4 ranges would be
   *
   * <ul>
   *   <li>age <= 15
   *   <li>age > 15 && age <= 32
   *   <li>age >32 && age <= 60
   *   <li>age > 60
   * </ul>
   */
  private static SortKey[] createRangeBounds() {
    SortKey[] rangeBounds = new SortKey[NUM_PARTITIONS - 1];
    for (int i = 0; i < NUM_PARTITIONS - 1; ++i) {
      RowData rowData =
          GenericRowData.of(
              StringData.fromString("data"),
              RANGE_STEP * (i + 1),
              StringData.fromString("2023-06-20"));
      RowDataWrapper keyWrapper = new RowDataWrapper(ROW_TYPE, TestFixtures.SCHEMA.asStruct());
      keyWrapper.wrap(rowData);
      SortKey sortKey = new SortKey(TestFixtures.SCHEMA, SORT_ORDER);
      sortKey.wrap(keyWrapper);
      rangeBounds[i] = sortKey;
    }

    return rangeBounds;
  }

  @Test
  public void test() {
    SketchRangePartitioner partitioner =
        new SketchRangePartitioner(TestFixtures.SCHEMA, SORT_ORDER, RANGE_BOUNDS);
    GenericRowData row =
        GenericRowData.of(StringData.fromString("data"), 0L, StringData.fromString("2023-06-20"));
    for (long id = 0; id < MAX_ID; ++id) {
      row.setField(1, id);
      int partition = partitioner.partition(row, NUM_PARTITIONS);
      assertThat(partition).isGreaterThanOrEqualTo(0).isLessThan(NUM_PARTITIONS);
      int expectedPartition = id == 0L ? 0 : (int) ((id - 1) / RANGE_STEP);
      assertThat(partition).isEqualTo(expectedPartition);
    }
  }
}
