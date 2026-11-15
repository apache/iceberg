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

import java.util.Locale;
import org.apache.iceberg.spark.source.metrics.NanoDurationMetric.MaxTaskWriteDuration;
import org.apache.iceberg.spark.source.metrics.NanoDurationMetric.MedianTaskWriteDuration;
import org.apache.iceberg.spark.source.metrics.NanoDurationMetric.MinTaskWriteDuration;
import org.apache.iceberg.spark.source.metrics.NanoDurationMetric.WriteCloseDuration;
import org.apache.iceberg.spark.source.metrics.NanoDurationMetric.WriteDuration;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomSumMetric;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestNanoDurationMetric {

  private final CustomSumMetric metric = new WriteDuration();

  @Test
  void sumsTaskValues() {
    // write tasks report 0 and the driver reports the value
    assertThat(metric.aggregateTaskMetrics(new long[] {0L, 0L, 1_500_000L})).isEqualTo("1 ms");
  }

  @Test
  void unitBoundaries() {
    assertThat(total(999L)).isEqualTo("999 ns");
    assertThat(total(1_000L)).isEqualTo("1 us");
    assertThat(total(999_999L)).isEqualTo("999 us");
    assertThat(total(1_000_000L)).isEqualTo("1 ms");
    assertThat(total(999_999_999L)).isEqualTo("999 ms");
    assertThat(total(1_000_000_000L)).isEqualTo("1.0 s");
  }

  @Test
  void aggregateTaskMetricsWithNoTasks() {
    assertThat(metric.aggregateTaskMetrics(new long[] {})).isEqualTo("0 ns");
  }

  @Test
  void aggregateTaskMetricsIsLocaleIndependent() {
    Locale previous = Locale.getDefault();
    try {
      // a comma-decimal locale would render "2,5 s" without an explicit Locale.ROOT
      Locale.setDefault(Locale.GERMANY);
      assertThat(total(2_500_000_000L)).isEqualTo("2.5 s");
    } finally {
      Locale.setDefault(previous);
    }
  }

  @ParameterizedTest
  @ValueSource(
      classes = {
        WriteDuration.class,
        MinTaskWriteDuration.class,
        MedianTaskWriteDuration.class,
        MaxTaskWriteDuration.class,
        WriteCloseDuration.class
      })
  void sparkCanRecreateMetricFromClassName(Class<? extends CustomMetric> metricClass)
      throws ReflectiveOperationException {
    // the SQL UI aggregates a metric by loading its class name with a public no-arg constructor
    CustomMetric recreated =
        (CustomMetric) Class.forName(metricClass.getName()).getConstructor().newInstance();

    assertThat(recreated).isInstanceOf(metricClass);
    assertThat(recreated.aggregateTaskMetrics(new long[] {0L, 1_500_000L})).isEqualTo("1 ms");
  }

  private String total(long nanos) {
    return metric.aggregateTaskMetrics(new long[] {nanos});
  }
}
