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
package org.apache.iceberg.lance;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link LanceMetrics}. */
public class TestLanceMetrics {

  private static final Schema TEST_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "value", Types.DoubleType.get()));

  @Test
  public void testSimpleMetricsCreation() {
    Metrics metrics = LanceMetrics.createMetrics(100);

    assertThat(metrics.recordCount()).isEqualTo(100);
    assertThat(metrics.columnSizes()).isNull();
    assertThat(metrics.valueCounts()).isNull();
    assertThat(metrics.nullValueCounts()).isNull();
  }

  @Test
  public void testFullMetricsCreation() {
    Map<Integer, Long> columnSizes = Maps.newHashMap();
    columnSizes.put(1, 400L);
    columnSizes.put(2, 1000L);
    columnSizes.put(3, 800L);

    Map<Integer, Long> valueCounts = Maps.newHashMap();
    valueCounts.put(1, 100L);
    valueCounts.put(2, 100L);
    valueCounts.put(3, 100L);

    Map<Integer, Long> nullCounts = Maps.newHashMap();
    nullCounts.put(1, 0L);
    nullCounts.put(2, 5L);
    nullCounts.put(3, 10L);

    Map<Integer, Object> lowerBounds = Maps.newHashMap();
    lowerBounds.put(1, 1);
    lowerBounds.put(3, 0.5);

    Map<Integer, Object> upperBounds = Maps.newHashMap();
    upperBounds.put(1, 100);
    upperBounds.put(3, 99.5);

    Metrics metrics =
        LanceMetrics.createMetrics(
            100L, TEST_SCHEMA, columnSizes, valueCounts, nullCounts, lowerBounds, upperBounds);

    assertThat(metrics.recordCount()).isEqualTo(100);
    assertThat(metrics.columnSizes()).hasSize(3);
    assertThat(metrics.columnSizes().get(1)).isEqualTo(400L);
    assertThat(metrics.valueCounts()).hasSize(3);
    assertThat(metrics.nullValueCounts()).hasSize(3);
    assertThat(metrics.nullValueCounts().get(2)).isEqualTo(5L);
    assertThat(metrics.lowerBounds()).isNotNull();
    assertThat(metrics.upperBounds()).isNotNull();
  }

  @Test
  public void testMetricsCollector() {
    LanceMetrics.MetricsCollector collector = new LanceMetrics.MetricsCollector();

    // Simulate writing 3 records
    for (int i = 0; i < 3; i++) {
      collector.incrementRowCount();
      collector.incrementValueCount(1);
      collector.incrementValueCount(2);
      collector.incrementValueCount(3);
      collector.addColumnSize(1, 4);
      collector.addColumnSize(2, 10);
      collector.addColumnSize(3, 8);
    }

    // Add some null counts
    collector.incrementNullCount(2);

    // Update bounds
    collector.updateLowerBound(1, 1);
    collector.updateUpperBound(1, 100);
    collector.updateLowerBound(1, 0); // should update lower bound to 0
    collector.updateUpperBound(1, 50); // should NOT update upper bound (50 < 100)

    assertThat(collector.rowCount()).isEqualTo(3);

    Metrics metrics = collector.toMetrics(TEST_SCHEMA);
    assertThat(metrics.recordCount()).isEqualTo(3);
    assertThat(metrics.columnSizes().get(1)).isEqualTo(12L); // 4 * 3
    assertThat(metrics.valueCounts().get(2)).isEqualTo(3L);
    assertThat(metrics.nullValueCounts().get(2)).isEqualTo(1L);
    assertThat(metrics.lowerBounds()).isNotNull();
    assertThat(metrics.upperBounds()).isNotNull();
  }

  @Test
  public void testMetricsCollectorBoundsUpdate() {
    LanceMetrics.MetricsCollector collector = new LanceMetrics.MetricsCollector();

    // Test that lower bound is correctly tracked as the minimum
    collector.updateLowerBound(1, 50);
    collector.updateLowerBound(1, 10);
    collector.updateLowerBound(1, 30);

    // Test that upper bound is correctly tracked as the maximum
    collector.updateUpperBound(1, 50);
    collector.updateUpperBound(1, 90);
    collector.updateUpperBound(1, 70);

    collector.incrementRowCount();
    Metrics metrics = collector.toMetrics(TEST_SCHEMA);

    // Lower bound should be the minimum observed value
    assertThat(metrics.lowerBounds()).isNotNull();
    // Upper bound should be the maximum observed value
    assertThat(metrics.upperBounds()).isNotNull();
  }

  @Test
  public void testNullBoundsMap() {
    Metrics metrics =
        LanceMetrics.createMetrics(10L, TEST_SCHEMA, null, null, null, null, null);

    assertThat(metrics.recordCount()).isEqualTo(10);
    assertThat(metrics.lowerBounds()).isNull();
    assertThat(metrics.upperBounds()).isNull();
  }
}
