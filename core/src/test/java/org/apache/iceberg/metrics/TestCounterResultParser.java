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
        .hasMessage("Cannot parse missing string type");

    Assertions.assertThatThrownBy(
            () -> CounterResultParser.fromJson("{\"type\":\"java.lang.Integer\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string name");

    Assertions.assertThatThrownBy(
            () ->
                CounterResultParser.fromJson(
                    "{\"type\":\"java.lang.Integer\", \"name\": \"some-name\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string unit");

    Assertions.assertThatThrownBy(
            () ->
                CounterResultParser.fromJson(
                    "{\"type\":\"java.lang.Integer\", \"name\": \"some-name\", \"unit\":\"BYTES\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing int value");
  }

  @Test
  public void extraFields() {
    Assertions.assertThat(
            CounterResultParser.fromJson(
                "{\"name\":\"example\",\"unit\":\"BYTES\",\"value\":23,\"type\":\"java.lang.Integer\",\"extra\": \"value\"}"))
        .isEqualTo(new CounterResult<Integer>("example", Unit.BYTES, 23));
  }

  @Test
  public void unsupportedType() {
    Assertions.assertThatThrownBy(
            () ->
                CounterResultParser.fromJson(
                    "{\"name\":\"example\",\"unit\":\"BYTES\",\"value\":23,\"type\":\"java.lang.Double\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot parse to long counter: {\"name\":\"example\",\"unit\":\"BYTES\",\"value\":23,\"type\":\"java.lang.Double\"}");
  }

  @Test
  public void unsupportedUnit() {
    Assertions.assertThatThrownBy(
            () ->
                CounterResultParser.fromJson(
                    "{\"name\":\"example\",\"unit\":\"UNKNOWN\",\"value\":23,\"type\":\"java.lang.Long\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot determine unit from display name: UNKNOWN");
  }

  @Test
  public void invalidValue() {
    Assertions.assertThatThrownBy(
            () ->
                CounterResultParser.fromJson(
                    "{\"name\":\"example\",\"unit\":\"COUNT\",\"value\":\"illegal\",\"type\":\"java.lang.Long\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse value to a long value: \"illegal\"");
  }

  @Test
  public void invalidName() {
    Assertions.assertThatThrownBy(
            () ->
                CounterResultParser.fromJson(
                    "{\"name\":23,\"unit\":\"COUNT\",\"value\":45,\"type\":\"java.lang.Long\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse name to a string value: 23");
  }

  @Test
  public void roundTripSerde() {
    CounterResult<Integer> counter = new CounterResult<>("intExample", Unit.BYTES, 23);
    Assertions.assertThat(CounterResultParser.fromJson(CounterResultParser.toJson(counter)))
        .isEqualTo(counter);

    CounterResult<Long> longCounter = new CounterResult<>("longExample", Unit.COUNT, 23L);
    Assertions.assertThat(CounterResultParser.fromJson(CounterResultParser.toJson(longCounter)))
        .isEqualTo(longCounter);
  }
}
