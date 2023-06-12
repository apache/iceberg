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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCounterResultParser {

  @Test
  public void nullCounter() {
    Assertions.assertThatThrownBy(() -> CounterResultParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse counter from null object");

    Assertions.assertThatThrownBy(() -> CounterResultParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid counter: null");
  }

  @Test
  public void missingFields() {
    Assertions.assertThatThrownBy(() -> CounterResultParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: unit");

    Assertions.assertThatThrownBy(() -> CounterResultParser.fromJson("{\"unit\":\"bytes\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long: value");
  }

  @Test
  public void extraFields() {
    Assertions.assertThat(
            CounterResultParser.fromJson("{\"unit\":\"bytes\",\"value\":23,\"extra\": \"value\"}"))
        .isEqualTo(CounterResult.of(Unit.BYTES, 23L));
  }

  @Test
  public void unsupportedUnit() {
    Assertions.assertThatThrownBy(
            () -> CounterResultParser.fromJson("{\"unit\":\"unknown\",\"value\":23}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid unit: unknown");
  }

  @Test
  public void invalidValue() {
    Assertions.assertThatThrownBy(
            () -> CounterResultParser.fromJson("{\"unit\":\"count\",\"value\":\"illegal\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: value: \"illegal\"");
  }

  @Test
  public void roundTripSerde() {
    CounterResult counter = CounterResult.of(Unit.BYTES, Long.MAX_VALUE);

    String json = CounterResultParser.toJson(counter);
    Assertions.assertThat(CounterResultParser.fromJson(json)).isEqualTo(counter);
    Assertions.assertThat(json).isEqualTo("{\"unit\":\"bytes\",\"value\":9223372036854775807}");
  }
}
