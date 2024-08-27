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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Comparator;
import org.apache.iceberg.MetricsModes.MetricsMode;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.Test;

public class TestFileGenerationUtil {

  public static final Schema SCHEMA =
      new Schema(
          required(1, "int_col", Types.IntegerType.get()),
          required(2, "long_col", Types.LongType.get()),
          required(3, "decimal_col", Types.DecimalType.of(10, 10)),
          required(4, "date_col", Types.DateType.get()),
          required(5, "timestamp_col", Types.TimestampType.withZone()),
          required(6, "timestamp_tz_col", Types.TimestampType.withZone()),
          required(7, "str_col", Types.StringType.get()));

  @Test
  public void testBoundsWithDefaultMetricsConfig() {
    MetricsConfig metricsConfig = MetricsConfig.getDefault();
    Metrics metrics =
        FileGenerationUtil.generateRandomMetrics(
            SCHEMA,
            metricsConfig,
            ImmutableMap.of() /* no known lower bounds */,
            ImmutableMap.of() /* no known upper bounds */);

    assertThat(metrics.lowerBounds()).hasSize(SCHEMA.columns().size());
    assertThat(metrics.upperBounds()).hasSize(SCHEMA.columns().size());

    checkBounds(metrics, metricsConfig);
  }

  @Test
  public void testBoundsWithSpecificValues() {
    MetricsConfig metricsConfig = MetricsConfig.getDefault();
    NestedField intField = SCHEMA.findField("int_col");
    PrimitiveType type = intField.type().asPrimitiveType();
    ByteBuffer intLower = Conversions.toByteBuffer(type, 0);
    ByteBuffer intUpper = Conversions.toByteBuffer(type, 0);
    Metrics metrics =
        FileGenerationUtil.generateRandomMetrics(
            SCHEMA,
            metricsConfig,
            ImmutableMap.of(intField.fieldId(), intLower),
            ImmutableMap.of(intField.fieldId(), intUpper));

    assertThat(metrics.lowerBounds()).hasSize(SCHEMA.columns().size());
    assertThat(metrics.upperBounds()).hasSize(SCHEMA.columns().size());

    checkBounds(metrics, metricsConfig);

    ByteBuffer actualIntLower = metrics.lowerBounds().get(intField.fieldId());
    ByteBuffer actualIntUpper = metrics.upperBounds().get(intField.fieldId());
    assertThat(actualIntLower).isEqualTo(intLower);
    assertThat(actualIntUpper).isEqualTo(intUpper);
  }

  private void checkBounds(Metrics metrics, MetricsConfig metricsConfig) {
    for (NestedField field : SCHEMA.columns()) {
      MetricsMode mode = metricsConfig.columnMode(field.name());
      ByteBuffer lowerBuffer = metrics.lowerBounds().get(field.fieldId());
      ByteBuffer upperBuffer = metrics.upperBounds().get(field.fieldId());
      if (mode.equals(MetricsModes.None.get()) || mode.equals(MetricsModes.Counts.get())) {
        assertThat(lowerBuffer).isNull();
        assertThat(upperBuffer).isNull();
      } else {
        checkBounds(field.type().asPrimitiveType(), lowerBuffer, upperBuffer);
      }
    }
  }

  private void checkBounds(PrimitiveType type, ByteBuffer lowerBuffer, ByteBuffer upperBuffer) {
    Object lower = Conversions.fromByteBuffer(type, lowerBuffer);
    Object upper = Conversions.fromByteBuffer(type, upperBuffer);
    Comparator<Object> cmp = Comparators.forType(type);
    assertThat(cmp.compare(lower, upper)).isLessThanOrEqualTo(0);
  }
}
