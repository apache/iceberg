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
package org.apache.iceberg.rest.responses;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

public class TestConfigResponseParser {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> ConfigResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid config response: null");

    assertThatThrownBy(() -> ConfigResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse config response from null object");

    ConfigResponse actual = ConfigResponseParser.fromJson("{}");
    ConfigResponse expected = ConfigResponse.builder().build();
    // ConfigResponse doesn't implement hashCode/equals
    assertThat(actual.defaults()).isEqualTo(expected.defaults()).isEmpty();
    assertThat(actual.overrides()).isEqualTo(expected.overrides()).isEmpty();
  }

  @Test
  public void unknownFields() {
    ConfigResponse actual = ConfigResponseParser.fromJson("{\"x\": \"val\", \"y\": \"val2\"}");
    ConfigResponse expected = ConfigResponse.builder().build();
    // ConfigResponse doesn't implement hashCode/equals
    assertThat(actual.defaults()).isEqualTo(expected.defaults()).isEmpty();
    assertThat(actual.overrides()).isEqualTo(expected.overrides()).isEmpty();
  }

  @Test
  public void defaultsOnly() {
    Map<String, String> defaults = Maps.newHashMap();
    defaults.put("a", "1");
    defaults.put("b", null);
    defaults.put("c", "2");
    defaults.put("d", null);

    ConfigResponse response = ConfigResponse.builder().withDefaults(defaults).build();
    String expectedJson =
        "{\n"
            + "  \"defaults\" : {\n"
            + "    \"a\" : \"1\",\n"
            + "    \"b\" : null,\n"
            + "    \"c\" : \"2\",\n"
            + "    \"d\" : null\n"
            + "  },\n"
            + "  \"overrides\" : { }\n"
            + "}";

    String json = ConfigResponseParser.toJson(response, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(ConfigResponseParser.toJson(ConfigResponseParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }

  @Test
  public void overridesOnly() {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put("a", "1");
    overrides.put("b", null);
    overrides.put("c", "2");
    overrides.put("d", null);

    ConfigResponse response = ConfigResponse.builder().withOverrides(overrides).build();
    String expectedJson =
        "{\n"
            + "  \"defaults\" : { },\n"
            + "  \"overrides\" : {\n"
            + "    \"a\" : \"1\",\n"
            + "    \"b\" : null,\n"
            + "    \"c\" : \"2\",\n"
            + "    \"d\" : null\n"
            + "  }\n"
            + "}";

    String json = ConfigResponseParser.toJson(response, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(ConfigResponseParser.toJson(ConfigResponseParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }

  @Test
  public void roundTripSerde() {
    Map<String, String> defaults = Maps.newHashMap();
    defaults.put("key1", "1");
    defaults.put("key2", null);

    Map<String, String> overrides = Maps.newHashMap();
    overrides.put("key3", "23");
    overrides.put("key4", null);

    ConfigResponse response =
        ConfigResponse.builder().withDefaults(defaults).withOverrides(overrides).build();
    String expectedJson =
        "{\n"
            + "  \"defaults\" : {\n"
            + "    \"key1\" : \"1\",\n"
            + "    \"key2\" : null\n"
            + "  },\n"
            + "  \"overrides\" : {\n"
            + "    \"key3\" : \"23\",\n"
            + "    \"key4\" : null\n"
            + "  }\n"
            + "}";

    String json = ConfigResponseParser.toJson(response, true);
    assertThat(json).isEqualTo(expectedJson);
    assertThat(ConfigResponseParser.toJson(ConfigResponseParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }
}
