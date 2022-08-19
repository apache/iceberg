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
package org.apache.iceberg.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestJsonUtil {

  @Test
  public void get() throws JsonProcessingException {
    Assertions.assertThatThrownBy(() -> JsonUtil.get("x", JsonUtil.mapper().readTree("{}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: x");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.get("x", JsonUtil.mapper().readTree("{\"x\": null}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: x");

    Assertions.assertThat(JsonUtil.get("x", JsonUtil.mapper().readTree("{\"x\": \"23\"}")).asText())
        .isEqualTo("23");
  }

  @Test
  public void getInt() throws JsonProcessingException {
    Assertions.assertThatThrownBy(() -> JsonUtil.getInt("x", JsonUtil.mapper().readTree("{}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing int: x");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getInt("x", JsonUtil.mapper().readTree("{\"x\": null}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to an integer value: x: null");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getInt("x", JsonUtil.mapper().readTree("{\"x\": \"23\"}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to an integer value: x: \"23\"");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getInt("x", JsonUtil.mapper().readTree("{\"x\": 23.0}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to an integer value: x: 23.0");

    Assertions.assertThat(JsonUtil.getInt("x", JsonUtil.mapper().readTree("{\"x\": 23}")))
        .isEqualTo(23);
  }

  @Test
  public void getIntOrNull() throws JsonProcessingException {
    Assertions.assertThat(JsonUtil.getIntOrNull("x", JsonUtil.mapper().readTree("{}"))).isNull();
    Assertions.assertThat(JsonUtil.getIntOrNull("x", JsonUtil.mapper().readTree("{\"x\": 23}")))
        .isEqualTo(23);
    Assertions.assertThat(JsonUtil.getIntOrNull("x", JsonUtil.mapper().readTree("{\"x\": null}")))
        .isNull();

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getIntOrNull("x", JsonUtil.mapper().readTree("{\"x\": \"23\"}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to an integer value: x: \"23\"");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getIntOrNull("x", JsonUtil.mapper().readTree("{\"x\": 23.0}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to an integer value: x: 23.0");
  }

  @Test
  public void getLong() throws JsonProcessingException {
    Assertions.assertThatThrownBy(() -> JsonUtil.getLong("x", JsonUtil.mapper().readTree("{}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long: x");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getLong("x", JsonUtil.mapper().readTree("{\"x\": null}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: x: null");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getLong("x", JsonUtil.mapper().readTree("{\"x\": \"23\"}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: x: \"23\"");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getLong("x", JsonUtil.mapper().readTree("{\"x\": 23.0}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: x: 23.0");

    Assertions.assertThat(JsonUtil.getLong("x", JsonUtil.mapper().readTree("{\"x\": 23}")))
        .isEqualTo(23);
  }

  @Test
  public void getLongOrNull() throws JsonProcessingException {
    Assertions.assertThat(JsonUtil.getLongOrNull("x", JsonUtil.mapper().readTree("{}"))).isNull();
    Assertions.assertThat(JsonUtil.getLongOrNull("x", JsonUtil.mapper().readTree("{\"x\": 23}")))
        .isEqualTo(23);
    Assertions.assertThat(JsonUtil.getLongOrNull("x", JsonUtil.mapper().readTree("{\"x\": null}")))
        .isNull();

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getLongOrNull("x", JsonUtil.mapper().readTree("{\"x\": \"23\"}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: x: \"23\"");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getLongOrNull("x", JsonUtil.mapper().readTree("{\"x\": 23.0}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: x: 23.0");
  }

  @Test
  public void getString() throws JsonProcessingException {
    Assertions.assertThatThrownBy(() -> JsonUtil.getString("x", JsonUtil.mapper().readTree("{}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: x");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getString("x", JsonUtil.mapper().readTree("{\"x\": null}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: x: null");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getString("x", JsonUtil.mapper().readTree("{\"x\": 23}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: x: 23");

    Assertions.assertThat(JsonUtil.getString("x", JsonUtil.mapper().readTree("{\"x\": \"23\"}")))
        .isEqualTo("23");
  }

  @Test
  public void getStringOrNull() throws JsonProcessingException {
    Assertions.assertThat(JsonUtil.getStringOrNull("x", JsonUtil.mapper().readTree("{}"))).isNull();
    Assertions.assertThat(
            JsonUtil.getStringOrNull("x", JsonUtil.mapper().readTree("{\"x\": \"23\"}")))
        .isEqualTo("23");
    Assertions.assertThat(
            JsonUtil.getStringOrNull("x", JsonUtil.mapper().readTree("{\"x\": null}")))
        .isNull();

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getStringOrNull("x", JsonUtil.mapper().readTree("{\"x\": 23}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: x: 23");
  }

  @Test
  public void getBool() throws JsonProcessingException {
    Assertions.assertThatThrownBy(() -> JsonUtil.getBool("x", JsonUtil.mapper().readTree("{}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing boolean: x");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getBool("x", JsonUtil.mapper().readTree("{\"x\": null}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a boolean value: x: null");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getBool("x", JsonUtil.mapper().readTree("{\"x\": \"23\"}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a boolean value: x: \"23\"");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getBool("x", JsonUtil.mapper().readTree("{\"x\": \"true\"}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a boolean value: x: \"true\"");

    Assertions.assertThat(JsonUtil.getBool("x", JsonUtil.mapper().readTree("{\"x\": true}")))
        .isTrue();
    Assertions.assertThat(JsonUtil.getBool("x", JsonUtil.mapper().readTree("{\"x\": false}")))
        .isFalse();
  }
}
