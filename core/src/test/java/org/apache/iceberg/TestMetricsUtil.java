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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

class TestMetricsUtil {

  @Test
  void copyWithoutFieldCountsDropsAvgValueSizesForExcludedFields() {
    Metrics copy =
        MetricsUtil.copyWithoutFieldCounts(metricsWithAvgValueSizes(), ImmutableSet.of(1));

    assertThat(copy.avgValueSizes()).isEqualTo(ImmutableMap.of(2, 20));
  }

  @Test
  void copyWithoutFieldCountsAndBoundsDropsAvgValueSizesForExcludedFields() {
    Metrics copy =
        MetricsUtil.copyWithoutFieldCountsAndBounds(metricsWithAvgValueSizes(), ImmutableSet.of(1));

    assertThat(copy.avgValueSizes()).isEqualTo(ImmutableMap.of(2, 20));
  }

  @Test
  void copyDropsAvgValueSizesEntirelyWhenAllFieldsExcluded() {
    // once every tracked field is excluded the filtered map is empty, and copyWithoutKeys returns
    // null rather than an empty map, matching the "null otherwise" contract of avgValueSizes()
    Metrics copy =
        MetricsUtil.copyWithoutFieldCounts(metricsWithAvgValueSizes(), ImmutableSet.of(1, 2));

    assertThat(copy.avgValueSizes()).isNull();
  }

  @Test
  void copyWithStatsReturnsNullAvgValueSizesWhenNoRequestedColumnMatches() {
    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withMetrics(metricsWithAvgValueSizes())
            .build();

    assertThat(file.copyWithStats(ImmutableSet.of(2)).avgValueSizes())
        .isEqualTo(ImmutableMap.of(2, 20));
    assertThat(file.copyWithStats(ImmutableSet.of(3)).avgValueSizes()).isNull();
  }

  private static Metrics metricsWithAvgValueSizes() {
    return new Metrics(3L, null, null, null, null, null, null, ImmutableMap.of(1, 10, 2, 20), null);
  }
}
