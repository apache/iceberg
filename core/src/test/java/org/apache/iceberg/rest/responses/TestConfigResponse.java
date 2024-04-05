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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RequestResponseTestBase;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestConfigResponse extends RequestResponseTestBase<ConfigResponse> {

  private static final Map<String, String> DEFAULTS =
      ImmutableMap.of("warehouse", "s3://bucket/warehouse");
  private static final Map<String, String> OVERRIDES = ImmutableMap.of("clients", "5");

  private static final Map<String, String> DEFAULTS_WITH_NULL_VALUE = Maps.newHashMap();
  private static final Map<String, String> OVERRIDES_WITH_NULL_VALUE = Maps.newHashMap();

  @BeforeAll
  public static void beforeAllForRestCatalogConfig() {
    DEFAULTS_WITH_NULL_VALUE.put("warehouse", null);
    OVERRIDES_WITH_NULL_VALUE.put("clients", null);
  }

  @Test
  // Test cases that are JSON that can be created via the Builder
  public void testRoundTripSerDe() throws JsonProcessingException {
    // Both fields have values without nulls
    String fullJson =
        "{\"defaults\":{\"warehouse\":\"s3://bucket/warehouse\"},\"overrides\":{\"clients\":\"5\"}}";
    assertRoundTripSerializesEquallyFrom(
        fullJson, ConfigResponse.builder().withOverrides(OVERRIDES).withDefaults(DEFAULTS).build());
    assertRoundTripSerializesEquallyFrom(
        fullJson,
        ConfigResponse.builder()
            .withOverride("clients", "5")
            .withDefault("warehouse", "s3://bucket/warehouse")
            .build());

    // `defaults` is empty
    String jsonEmptyDefaults = "{\"defaults\":{},\"overrides\":{\"clients\":\"5\"}}";
    assertRoundTripSerializesEquallyFrom(
        jsonEmptyDefaults, ConfigResponse.builder().withOverrides(OVERRIDES).build());
    assertRoundTripSerializesEquallyFrom(
        jsonEmptyDefaults,
        ConfigResponse.builder().withOverrides(OVERRIDES).withDefaults(ImmutableMap.of()).build());
    assertRoundTripSerializesEquallyFrom(
        jsonEmptyDefaults, ConfigResponse.builder().withOverride("clients", "5").build());

    // `overrides` is empty
    String jsonEmptyOverrides =
        "{\"defaults\":{\"warehouse\":\"s3://bucket/warehouse\"},\"overrides\":{}}";
    assertRoundTripSerializesEquallyFrom(
        jsonEmptyOverrides, ConfigResponse.builder().withDefaults(DEFAULTS).build());
    assertRoundTripSerializesEquallyFrom(
        jsonEmptyOverrides,
        ConfigResponse.builder().withDefault("warehouse", "s3://bucket/warehouse").build());
    assertRoundTripSerializesEquallyFrom(
        jsonEmptyOverrides,
        ConfigResponse.builder().withDefaults(DEFAULTS).withOverrides(ImmutableMap.of()).build());

    // Both are empty
    String emptyJson = "{\"defaults\":{},\"overrides\":{}}";
    assertRoundTripSerializesEquallyFrom(emptyJson, ConfigResponse.builder().build());
    assertRoundTripSerializesEquallyFrom(
        emptyJson,
        ConfigResponse.builder()
            .withOverrides(ImmutableMap.of())
            .withDefaults(ImmutableMap.of())
            .build());
  }

  @Test
  // Test cases that cannot be built with our builder, but that are accepted when parsed
  public void testCanDeserializeWithoutDefaultValues() throws JsonProcessingException {
    ConfigResponse noOverrides = ConfigResponse.builder().withDefaults(DEFAULTS).build();
    String jsonMissingOverrides = "{\"defaults\":{\"warehouse\":\"s3://bucket/warehouse\"}}";
    assertEquals(deserialize(jsonMissingOverrides), noOverrides);
    String jsonNullOverrides =
        "{\"defaults\":{\"warehouse\":\"s3://bucket/warehouse\"},\"overrides\":null}";
    assertEquals(deserialize(jsonNullOverrides), noOverrides);

    ConfigResponse noDefaults = ConfigResponse.builder().withOverrides(OVERRIDES).build();
    String jsonMissingDefaults = "{\"overrides\":{\"clients\":\"5\"}}";
    assertEquals(deserialize(jsonMissingDefaults), noDefaults);
    String jsonNullDefaults = "{\"defaults\":null,\"overrides\":{\"clients\":\"5\"}}";
    assertEquals(deserialize(jsonNullDefaults), noDefaults);

    ConfigResponse noValues = ConfigResponse.builder().build();
    String jsonEmptyObject = "{}";
    assertEquals(deserialize(jsonEmptyObject), noValues);
    String jsonNullForAllFields = "{\"defaults\":null,\"overrides\":null}";
    assertEquals(deserialize(jsonNullForAllFields), noValues);
  }

  @Test
  public void testCanUseNullAsPropertyValue() throws JsonProcessingException {
    String jsonNullValueInDefaults =
        "{\"defaults\":{\"warehouse\":null},\"overrides\":{\"clients\":\"5\"}}";
    assertRoundTripSerializesEquallyFrom(
        jsonNullValueInDefaults,
        ConfigResponse.builder()
            .withDefaults(DEFAULTS_WITH_NULL_VALUE)
            .withOverrides(OVERRIDES)
            .build());
    assertRoundTripSerializesEquallyFrom(
        jsonNullValueInDefaults,
        ConfigResponse.builder().withDefault("warehouse", null).withOverrides(OVERRIDES).build());

    String jsonNullValueInOverrides =
        "{\"defaults\":{\"warehouse\":\"s3://bucket/warehouse\"},\"overrides\":{\"clients\":null}}";
    assertRoundTripSerializesEquallyFrom(
        jsonNullValueInOverrides,
        ConfigResponse.builder()
            .withDefaults(DEFAULTS)
            .withOverrides(OVERRIDES_WITH_NULL_VALUE)
            .build());
    assertRoundTripSerializesEquallyFrom(
        jsonNullValueInOverrides,
        ConfigResponse.builder().withDefaults(DEFAULTS).withOverride("clients", null).build());
  }

  @Test
  public void testDeserializeInvalidResponse() {
    String jsonDefaultsHasWrongType =
        "{\"defaults\":[\"warehouse\",\"s3://bucket/warehouse\"],\"overrides\":{\"clients\":\"5\"}}";
    Assertions.assertThatThrownBy(() -> deserialize(jsonDefaultsHasWrongType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot parse string map from non-object value: defaults: [\"warehouse\",\"s3://bucket/warehouse\"]");

    String jsonOverridesHasWrongType =
        "{\"defaults\":{\"warehouse\":\"s3://bucket/warehouse\"},\"overrides\":\"clients\"}";
    Assertions.assertThatThrownBy(() -> deserialize(jsonOverridesHasWrongType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot parse string map from non-object value: overrides: \"clients\"");

    Assertions.assertThatThrownBy(() -> deserialize(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("argument \"content\" is null");
  }

  @Test
  public void testBuilderDoesNotCreateInvalidObjects() {
    Assertions.assertThatThrownBy(() -> ConfigResponse.builder().withOverride(null, "100").build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid override property: null");

    Assertions.assertThatThrownBy(() -> ConfigResponse.builder().withDefault(null, "100").build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid default property: null");

    Assertions.assertThatThrownBy(() -> ConfigResponse.builder().withOverrides(null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid override properties map: null");

    Assertions.assertThatThrownBy(() -> ConfigResponse.builder().withDefaults(null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid default properties map: null");

    Map<String, String> mapWithNullKey = Maps.newHashMap();
    mapWithNullKey.put(null, "a");
    mapWithNullKey.put("b", "b");
    Assertions.assertThatThrownBy(
            () -> ConfigResponse.builder().withDefaults(mapWithNullKey).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid default property: null");

    Assertions.assertThatThrownBy(
            () -> ConfigResponse.builder().withOverrides(mapWithNullKey).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid override property: null");
  }

  @Test
  public void testMergeStripsNullValuedEntries() {
    Map<String, String> mapWithNullValue = Maps.newHashMap();
    mapWithNullValue.put("a", null);
    mapWithNullValue.put("b", "from_overrides");

    Map<String, String> overrides = mapWithNullValue;
    Map<String, String> defaults = ImmutableMap.of("a", "from_defaults");
    Map<String, String> clientConfig = ImmutableMap.of("a", "from_client", "c", "from_client");

    ConfigResponse resp =
        ConfigResponse.builder().withOverrides(overrides).withDefaults(defaults).build();

    // "a" isn't present as it was marked as `null` in the overrides, so the provided client
    // configuration is discarded
    Map<String, String> merged = resp.merge(clientConfig);
    Map<String, String> expected =
        ImmutableMap.of(
            "b", "from_overrides",
            "c", "from_client");

    Assertions.assertThat(merged)
        .as(
            "The merged properties map should use values from defaults, then client config, and finally overrides")
        .isEqualTo(expected);
    Assertions.assertThat(merged)
        .as("The merged properties map should omit keys with null values")
        .doesNotContainValue(null);
  }

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"defaults", "overrides"};
  }

  @Override
  public ConfigResponse createExampleInstance() {
    return ConfigResponse.builder().withDefaults(DEFAULTS).withOverrides(OVERRIDES).build();
  }

  @Override
  public void assertEquals(ConfigResponse actual, ConfigResponse expected) {
    Assertions.assertThat(actual.defaults())
        .as("Config properties to use as defaults should be equal")
        .isEqualTo(expected.defaults());
    Assertions.assertThat(actual.overrides())
        .as("Config properties to use as overrides should be equal")
        .isEqualTo(expected.overrides());
  }

  @Override
  public ConfigResponse deserialize(String json) throws JsonProcessingException {
    ConfigResponse resp = mapper().readValue(json, ConfigResponse.class);
    resp.validate();
    return resp;
  }
}
