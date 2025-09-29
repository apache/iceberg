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
package org.apache.iceberg.stats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestFieldStats {

  @Test
  public void validFieldStats() {
    assertThat(BaseFieldStats.builder().build()).isNotNull();
    assertThat(BaseFieldStats.builder().fieldId(1).build()).isNotNull();
    assertThat(BaseFieldStats.builder().columnSize(3L).build()).isNotNull();
    assertThat(BaseFieldStats.builder().valueCount(3L).build()).isNotNull();
    assertThat(BaseFieldStats.builder().nullValueCount(3L).build()).isNotNull();
    assertThat(BaseFieldStats.builder().nanValueCount(3L).build()).isNotNull();
    assertThat(BaseFieldStats.builder().type(Types.IntegerType.get()).build()).isNotNull();

    assertThat(BaseFieldStats.builder().type(Types.LongType.get()).lowerBound(3L).build())
        .isNotNull();
    assertThat(BaseFieldStats.builder().type(Types.LongType.get()).upperBound(10L).build())
        .isNotNull();
    assertThat(
            BaseFieldStats.builder()
                .type(Types.LongType.get())
                .lowerBound(3L)
                .upperBound(10L)
                .build())
        .isNotNull();
    assertThat(
            BaseFieldStats.<Long>builder()
                .type(Types.LongType.get())
                .lowerBound(3L)
                .upperBound(10L)
                .build())
        .isNotNull();
  }

  @Test
  public void missingTypeWithUpperOrLowerBound() {
    assertThatThrownBy(() -> BaseFieldStats.builder().lowerBound(3).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid type (required when lower bound is set): null");
    assertThatThrownBy(() -> BaseFieldStats.builder().upperBound(3).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid type (required when lower bound is set): null");
  }

  @Test
  public void invalidType() {
    assertThatThrownBy(
            () -> BaseFieldStats.builder().type(Types.LongType.get()).lowerBound(3).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid lower bound type, expected a subtype of java.lang.Long: java.lang.Integer");
    assertThatThrownBy(
            () ->
                BaseFieldStats.<Integer>builder().type(Types.LongType.get()).lowerBound(3).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid lower bound type, expected a subtype of java.lang.Long: java.lang.Integer");

    assertThatThrownBy(
            () -> BaseFieldStats.builder().type(Types.LongType.get()).upperBound(3).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid upper bound type, expected a subtype of java.lang.Long: java.lang.Integer");
    assertThatThrownBy(
            () ->
                BaseFieldStats.<Integer>builder().type(Types.LongType.get()).upperBound(3).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid upper bound type, expected a subtype of java.lang.Long: java.lang.Integer");
  }
}
