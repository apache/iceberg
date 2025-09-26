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

import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.stats.BaseContentStats;
import org.apache.iceberg.stats.BaseFieldStats;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;

public class TestInclusiveStatsEvaluatorWithExtract
    extends TestInclusiveMetricsEvaluatorWithExtract {

  @Override
  protected DataFile file() {
    return new TestHelpers.TestDataFile(
        "file.avro",
        TestHelpers.Row.of(),
        50,
        BaseContentStats.builder()
            .withFieldStats(
                BaseFieldStats.builder().fieldId(1).valueCount(50L).nullValueCount(0L).build())
            .withFieldStats(
                BaseFieldStats.<VariantValue>builder()
                    .fieldId(2)
                    .type(Types.VariantType.get())
                    .valueCount(50L)
                    .nullValueCount(0L)
                    .lowerBound(
                        VariantTestUtil.variant(
                                Map.of(
                                    "$['event_id']",
                                    Variants.of(INT_MIN_VALUE),
                                    "$['str']",
                                    Variants.of("abc")))
                            .value())
                    .upperBound(
                        VariantTestUtil.variant(
                                Map.of(
                                    "$['event_id']",
                                    Variants.of(INT_MAX_VALUE),
                                    "$['str']",
                                    Variants.of("abe")))
                            .value())
                    .build())
            .withFieldStats(
                BaseFieldStats.builder().fieldId(3).valueCount(50L).nullValueCount(50L).build())
            .build());
  }
}
