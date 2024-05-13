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
import static org.apache.iceberg.flink.sink.shuffle.Fixtures.ROW_WRAPPER;

import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.SortKey;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSketchDataStatistics {
  @SuppressWarnings("unchecked")
  @Test
  public void testAddsAndGet() {
    SketchDataStatistics dataStatistics = new SketchDataStatistics(128);

    GenericRowData reusedRow = GenericRowData.of(StringData.fromString("a"), 1);
    Fixtures.SORT_KEY.wrap(ROW_WRAPPER.wrap(reusedRow));
    dataStatistics.add(Fixtures.SORT_KEY);

    reusedRow.setField(0, StringData.fromString("b"));
    Fixtures.SORT_KEY.wrap(ROW_WRAPPER.wrap(reusedRow));
    dataStatistics.add(Fixtures.SORT_KEY);

    reusedRow.setField(0, StringData.fromString("c"));
    Fixtures.SORT_KEY.wrap(ROW_WRAPPER.wrap(reusedRow));
    dataStatistics.add(Fixtures.SORT_KEY);

    reusedRow.setField(0, StringData.fromString("b"));
    Fixtures.SORT_KEY.wrap(ROW_WRAPPER.wrap(reusedRow));
    dataStatistics.add(Fixtures.SORT_KEY);

    ReservoirItemsSketch<SortKey> actual = (ReservoirItemsSketch<SortKey>) dataStatistics.result();
    Assertions.assertThat(actual.getSamples())
        .isEqualTo(
            new SortKey[] {
              CHAR_KEYS.get("a"), CHAR_KEYS.get("b"), CHAR_KEYS.get("c"), CHAR_KEYS.get("b")
            });
  }
}
