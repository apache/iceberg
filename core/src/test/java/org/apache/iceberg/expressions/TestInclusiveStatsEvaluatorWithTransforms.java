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

public class TestInclusiveStatsEvaluatorWithTransforms
    extends TestInclusiveMetricsEvaluatorWithTransforms {

  @Override
  protected DataFile file() {
    return new TestDataFile(
        "file.avro",
        Row.of(),
        50,
        BaseContentStats.builder()
            .withFieldStats(
                BaseFieldStats.builder().fieldId(1).valueCount(50L).nullValueCount(0L).build())
            .withFieldStats(
                BaseFieldStats.<Long>builder()
                    .fieldId(2)
                    .type(Types.TimestampType.withZone())
                    .valueCount(50L)
                    .nullValueCount(0L)
                    .lowerBound(TS_MIN_VALUE)
                    .upperBound(TS_MAX_VALUE)
                    .build())
            .withFieldStats(
                BaseFieldStats.builder().fieldId(3).valueCount(50L).nullValueCount(50L).build())
            .withFieldStats(
                BaseFieldStats.builder().fieldId(4).valueCount(50L).nullValueCount(50L).build())
            .withFieldStats(
                BaseFieldStats.<String>builder()
                    .fieldId(6)
                    .type(Types.StringType.get())
                    .valueCount(50L)
                    .nullValueCount(0L)
                    .lowerBound("abc")
                    .upperBound("abe")
                    .build())
            .build());
  }
}
