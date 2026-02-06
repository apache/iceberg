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
package org.apache.iceberg.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.junit.jupiter.api.Test;

public class TestCounterResultParser {

  @Test
  public void nullCounter() {
    assertThatThrownBy(() -> CounterResultParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from null object");

    assertThatThrownBy(() -> CounterResultParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid counter: null");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> CounterResultParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: unit");

    assertThatThrownBy(() -> CounterResultParser.fromJson("{\"unit\":\"bytes\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long: value");
  }

  @Test
  public void extraFields() {
    assertThat(
            CounterResultParser.fromJson("{\"unit\":\"bytes\",\"value\":23,\"extra\": \"value\"}"))
        .isEqualTo(CounterResult.of(Unit.BYTES, 23L));
  }

  @Test
  public void unsupportedUnit() {
    assertThatThrownBy(() -> CounterResultParser.fromJson("{\"unit\":\"unknown\",\"value\":23}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid unit: unknown");
  }

  @Test
  public void invalidValue() {
    assertThatThrownBy(
            () -> CounterResultParser.fromJson("{\"unit\":\"count\",\"value\":\"illegal\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: value: \"illegal\"");
  }

  @Test
  public void roundTripSerde() {
    CounterResult counter = CounterResult.of(Unit.BYTES, Long.MAX_VALUE);

    String json = CounterResultParser.toJson(counter);
    assertThat(CounterResultParser.fromJson(json)).isEqualTo(counter);
    assertThat(json).isEqualTo("{\"unit\":\"bytes\",\"value\":9223372036854775807}");
  }
}
