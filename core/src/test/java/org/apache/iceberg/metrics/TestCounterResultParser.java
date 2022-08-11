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
import org.apache.iceberg.metrics.ScanReport.CounterResult;
import org.assertj.core.api.Assertions;
import org.junit.Test;

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
        .hasMessage("Cannot parse missing string name");

    Assertions.assertThatThrownBy(() -> CounterResultParser.fromJson("{\"name\": \"some-name\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string unit");

    Assertions.assertThatThrownBy(
            () -> CounterResultParser.fromJson("{\"name\": \"some-name\", \"unit\":\"bytes\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long value");
  }

  @Test
  public void extraFields() {
    Assertions.assertThat(
            CounterResultParser.fromJson(
                "{\"name\":\"example\",\"unit\":\"bytes\",\"value\":23,\"extra\": \"value\"}"))
        .isEqualTo(new CounterResult("example", Unit.BYTES, 23L));
  }

  @Test
  public void unsupportedUnit() {
    Assertions.assertThatThrownBy(
            () ->
                CounterResultParser.fromJson(
                    "{\"name\":\"example\",\"unit\":\"unknown\",\"value\":23}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No enum constant org.apache.iceberg.metrics.MetricsContext.Unit.UNKNOWN");
  }

  @Test
  public void invalidValue() {
    Assertions.assertThatThrownBy(
            () ->
                CounterResultParser.fromJson(
                    "{\"name\":\"example\",\"unit\":\"count\",\"value\":\"illegal\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse value to a long value: \"illegal\"");
  }

  @Test
  public void invalidName() {
    Assertions.assertThatThrownBy(
            () -> CounterResultParser.fromJson("{\"name\":23,\"unit\":\"count\",\"value\":45}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse name to a string value: 23");
  }

  @Test
  public void roundTripSerde() {
    CounterResult counter = new CounterResult("counter-example", Unit.BYTES, Long.MAX_VALUE);

    String expectedJson =
        "{\n"
            + "  \"name\" : \"counter-example\",\n"
            + "  \"unit\" : \"bytes\",\n"
            + "  \"value\" : 9223372036854775807\n"
            + "}";

    String json = CounterResultParser.toJson(counter, true);
    Assertions.assertThat(CounterResultParser.fromJson(json)).isEqualTo(counter);
    Assertions.assertThat(json).isEqualTo(expectedJson);
  }
}
