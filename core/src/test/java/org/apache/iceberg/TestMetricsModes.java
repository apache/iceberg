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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.MetricsModes.Counts;
import org.apache.iceberg.MetricsModes.Full;
import org.apache.iceberg.MetricsModes.None;
import org.apache.iceberg.MetricsModes.Truncate;
import org.junit.jupiter.api.Test;

public class TestMetricsModes {
  @Test
  public void testMetricsModeParsing() {
    assertThat(MetricsModes.fromString("none")).isEqualTo(None.get());
    assertThat(MetricsModes.fromString("nOnE")).isEqualTo(None.get());
    assertThat(MetricsModes.fromString("counts")).isEqualTo(Counts.get());
    assertThat(MetricsModes.fromString("coUntS")).isEqualTo(Counts.get());
    assertThat(MetricsModes.fromString("truncate(1)")).isEqualTo(Truncate.withLength(1));
    assertThat(MetricsModes.fromString("truNcAte(10)")).isEqualTo(Truncate.withLength(10));
    assertThat(MetricsModes.fromString("full")).isEqualTo(Full.get());
    assertThat(MetricsModes.fromString("FULL")).isEqualTo(Full.get());
  }

  @Test
  public void testInvalidTruncationLength() {
    assertThatThrownBy(() -> MetricsModes.fromString("truncate(0)"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Truncate length should be positive");
  }
}
