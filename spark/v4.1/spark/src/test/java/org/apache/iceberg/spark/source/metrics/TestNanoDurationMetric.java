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
package org.apache.iceberg.spark.source.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.spark.sql.connector.metric.CustomSumMetric;
import org.junit.jupiter.api.Test;

public class TestNanoDurationMetric {

  private final CustomSumMetric metric = new WriteDuration();

  @Test
  public void testFormatsLargestMeaningfulUnit() {
    assertThat(metric.aggregateTaskMetrics(new long[] {})).isEqualTo("0 ns");
    assertThat(metric.aggregateTaskMetrics(new long[] {300L, 400L})).isEqualTo("700 ns");
    assertThat(metric.aggregateTaskMetrics(new long[] {1_500L, 2_500L})).isEqualTo("4 us");
    assertThat(metric.aggregateTaskMetrics(new long[] {1_000_000L, 500_000L})).isEqualTo("1 ms");
    assertThat(metric.aggregateTaskMetrics(new long[] {1_500_000_000L, 1_000_000_000L}))
        .isEqualTo("2.5 s");
  }

  @Test
  public void testUnitBoundaries() {
    assertThat(metric.aggregateTaskMetrics(new long[] {999L})).isEqualTo("999 ns");
    assertThat(metric.aggregateTaskMetrics(new long[] {1_000L})).isEqualTo("1 us");
    assertThat(metric.aggregateTaskMetrics(new long[] {999_999L})).isEqualTo("999 us");
    assertThat(metric.aggregateTaskMetrics(new long[] {1_000_000L})).isEqualTo("1 ms");
    assertThat(metric.aggregateTaskMetrics(new long[] {999_999_999L})).isEqualTo("999 ms");
    assertThat(metric.aggregateTaskMetrics(new long[] {1_000_000_000L})).isEqualTo("1.0 s");
  }

  @Test
  public void testMetricNames() {
    assertThat(new WriteDuration().name()).isEqualTo("writeDuration");
    assertThat(new WriteCloseDuration().name()).isEqualTo("writeCloseDuration");
  }
}
