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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.spark.source.metrics.NanoDurationMetric.MaxTaskWriteDuration;
import org.apache.iceberg.spark.source.metrics.NanoDurationMetric.WriteCloseDuration;
import org.apache.iceberg.spark.source.metrics.NanoDurationMetric.WriteDuration;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.junit.jupiter.api.Test;

class TestWriteDurations {

  @Test
  void driverMetricsReportTaskSpread() {
    List<WriteDurations> tasks =
        List.of(
            WriteDurations.of(40L, 1L),
            WriteDurations.of(10L, 2L),
            WriteDurations.of(900L, 3L),
            WriteDurations.of(20L, 4L),
            WriteDurations.of(30L, 5L));

    Map<String, Long> metrics = values(WriteDurations.driverMetrics(null, tasks));

    assertThat(metrics)
        .containsEntry(WriteDuration.NAME, 1_000L)
        .containsEntry(MaxTaskWriteDuration.NAME, 900L)
        .containsEntry(WriteCloseDuration.NAME, 15L);
  }

  @Test
  void driverMetricsWithNoTasks() {
    Map<String, Long> metrics = values(WriteDurations.driverMetrics(null, List.of()));

    assertThat(metrics)
        .containsEntry(WriteDuration.NAME, 0L)
        .containsEntry(MaxTaskWriteDuration.NAME, 0L)
        .containsEntry(WriteCloseDuration.NAME, 0L);
  }

  private static Map<String, Long> values(CustomTaskMetric[] metrics) {
    return Arrays.stream(metrics)
        .collect(Collectors.toMap(CustomTaskMetric::name, CustomTaskMetric::value));
  }
}
