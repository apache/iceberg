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
package org.apache.iceberg.expressions;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestHelpers.TestDataFile;
import org.apache.iceberg.stats.BaseContentStats;
import org.apache.iceberg.stats.BaseFieldStats;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StringType;

public class TestInclusiveStatsEvaluator extends TestInclusiveMetricsEvaluator {
  @Override
  protected DataFile file() {
    return new TestDataFile(
        "file.avro",
        Row.of(),
        50,
        BaseContentStats.builder()
            .withFieldStats(
                BaseFieldStats.builder()
                    .fieldId(1)
                    .type(Types.IntegerType.get())
                    .lowerBound(INT_MIN_VALUE)
                    .upperBound(INT_MAX_VALUE)
                    .build())
            .withFieldStats(
                BaseFieldStats.builder().fieldId(4).valueCount(50L).nullValueCount(50L).build())
            .withFieldStats(
                BaseFieldStats.builder().fieldId(5).valueCount(50L).nullValueCount(10L).build())
            .withFieldStats(
                BaseFieldStats.builder().fieldId(6).valueCount(50L).nullValueCount(0L).build())
            .withFieldStats(
                BaseFieldStats.builder().fieldId(7).valueCount(50L).nanValueCount(50L).build())
            .withFieldStats(
                BaseFieldStats.builder().fieldId(8).valueCount(50L).nanValueCount(10L).build())
            .withFieldStats(
                BaseFieldStats.builder().fieldId(9).valueCount(50L).nanValueCount(0L).build())
            .withFieldStats(
                BaseFieldStats.builder().fieldId(10).valueCount(50L).nullValueCount(50L).build())
            .withFieldStats(
                BaseFieldStats.builder()
                    .fieldId(11)
                    .valueCount(50L)
                    .nullValueCount(0L)
                    .type(Types.LongType.get())
                    .lowerBound(Float.NaN)
                    .upperBound(Float.NaN)
                    .build())
            .withFieldStats(
                BaseFieldStats.builder()
                    .fieldId(12)
                    .valueCount(50L)
                    .nullValueCount(1L)
                    .type(Types.DoubleType.get())
                    .lowerBound(Double.NaN)
                    .upperBound(Double.NaN)
                    .build())
            .withFieldStats(BaseFieldStats.builder().fieldId(13).valueCount(50L).build())
            .withFieldStats(
                BaseFieldStats.builder()
                    .fieldId(14)
                    .valueCount(50L)
                    .nullValueCount(0L)
                    .type(StringType.get())
                    .lowerBound("")
                    .upperBound("房东整租霍营小区二层两居室")
                    .build())
            .build());
  }

  @Override
  protected DataFile file2() {
    return new TestDataFile(
        "file_2.avro",
        Row.of(),
        50,
        BaseContentStats.builder()
            .withFieldStats(
                BaseFieldStats.builder()
                    .fieldId(3)
                    .valueCount(50L)
                    .nullValueCount(0L)
                    .type(StringType.get())
                    .lowerBound("aa")
                    .upperBound("dC")
                    .build())
            .build());
  }

  @Override
  protected DataFile file3() {
    return new TestDataFile(
        "file_3.avro",
        Row.of(),
        50,
        BaseContentStats.builder()
            .withFieldStats(
                BaseFieldStats.builder()
                    .fieldId(3)
                    .valueCount(50L)
                    .nullValueCount(0L)
                    .type(StringType.get())
                    .lowerBound("1str1")
                    .upperBound("3str3")
                    .build())
            .build());
  }

  @Override
  protected DataFile file4() {
    return new TestDataFile(
        "file_4.avro",
        Row.of(),
        50,
        BaseContentStats.builder()
            .withFieldStats(
                BaseFieldStats.builder()
                    .fieldId(3)
                    .valueCount(50L)
                    .nullValueCount(0L)
                    .type(StringType.get())
                    .lowerBound("abc")
                    .upperBound("イロハニホヘト")
                    .build())
            .build());
  }

  @Override
  protected DataFile file5() {
    return new TestDataFile(
        "file_5.avro",
        Row.of(),
        50,
        BaseContentStats.builder()
            .withFieldStats(
                BaseFieldStats.builder()
                    .fieldId(3)
                    .valueCount(50L)
                    .nullValueCount(0L)
                    .type(StringType.get())
                    .lowerBound("abc")
                    .upperBound("abcdefghi")
                    .build())
            .build());
  }
}
