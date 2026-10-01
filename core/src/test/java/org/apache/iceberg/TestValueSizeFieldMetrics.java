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

import org.junit.jupiter.api.Test;

class TestValueSizeFieldMetrics {

  @Test
  void averageValueSize() {
    ValueSizeFieldMetrics.Builder builder = new ValueSizeFieldMetrics.Builder(2);
    builder.addValueSize(21);
    builder.addValueSize(42);

    FieldMetrics<?> metrics = builder.build();

    assertThat(metrics.id()).isEqualTo(2);
    assertThat(metrics.valueCount()).isEqualTo(2);
    assertThat(metrics.nullValueCount()).isZero();
    assertThat(metrics.nanValueCount()).isEqualTo(-1);
    assertThat(metrics.avgValueSizeInBytes()).isEqualTo(31);
  }

  @Test
  void noValues() {
    FieldMetrics<?> metrics = new ValueSizeFieldMetrics.Builder(2).build();

    assertThat(metrics.valueCount()).isZero();
    assertThat(metrics.avgValueSizeInBytes()).isNull();
  }

  @Test
  void rejectsNegativeValueSize() {
    ValueSizeFieldMetrics.Builder builder = new ValueSizeFieldMetrics.Builder(2);

    assertThatThrownBy(() -> builder.addValueSize(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid value size: -1");
  }
}
