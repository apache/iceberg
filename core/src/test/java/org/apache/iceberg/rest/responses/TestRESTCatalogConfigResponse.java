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
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RequestResponseTestBase;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRESTCatalogConfigResponse extends RequestResponseTestBase<RESTCatalogConfigResponse> {

  private static final Map<String, String> DEFAULTS = ImmutableMap.of("warehouse", "s3://bucket/warehouse");
  private static final Map<String, String> OVERRIDES = ImmutableMap.of("clients", "5");

  private static final Map<String, String> DEFAULTS_WITH_NULL_VALUE = Maps.newHashMap();
  private static final Map<String, String> OVERRIDES_WITH_NULL_VALUE = Maps.newHashMap();

  @BeforeClass
  public static void beforeAllForRestCatalogConfig() {
    DEFAULTS_WITH_NULL_VALUE.put("warehouse", null);
    OVERRIDES_WITH_NULL_VALUE.put("clients", null);
  }

  @Test
  // Test cases that are JSON that can be created via the Builder
  public void testRoundTripSerDe() throws JsonProcessingException {
    // Both fields have values without nulls
    String fullJson = "{\"defaults\":{\"warehouse\":\"s3://bucket/warehouse\"},\"overrides\":{\"clients\":\"5\"}}";
    assertRoundTripSerializesEquallyFrom(
        fullJson,
        RESTCatalogConfigResponse.builder()
            .withOverrides(OVERRIDES).withDefaults(DEFAULTS).build());
    assertRoundTripSerializesEquallyFrom(
        fullJson,
        RESTCatalogConfigResponse.builder()
            .withOverride("clients", "5").withDefault("warehouse", "s3://bucket/warehouse").build());

    // `defaults` is empty
    String jsonEmptyDefaults = "{\"defaults\":{},\"overrides\":{\"clients\":\"5\"}}";
    assertRoundTripSerializesEquallyFrom(
        jsonEmptyDefaults,
        RESTCatalogConfigResponse.builder().withOverrides(OVERRIDES).build());
    assertRoundTripSerializesEquallyFrom(
        jsonEmptyDefaults,
        RESTCatalogConfigResponse.builder().withOverrides(OVERRIDES).withDefaults(ImmutableMap.of()).build());
    assertRoundTripSerializesEquallyFrom(
        jsonEmptyDefaults,
        RESTCatalogConfigResponse.builder().withOverride("clients", "5").build());

    // `overrides` is empty
    String jsonEmptyOverrides = "{\"defaults\":{\"warehouse\":\"s3://bucket/warehouse\"},\"overrides\":{}}";
    assertRoundTripSerializesEquallyFrom(
        jsonEmptyOverrides,
        RESTCatalogConfigResponse.builder().withDefaults(DEFAULTS).build());
    assertRoundTripSerializesEquallyFrom(
        jsonEmptyOverrides,
        RESTCatalogConfigResponse.builder().withDefault("warehouse", "s3://bucket/warehouse").build());
    assertRoundTripSerializesEquallyFrom(
        jsonEmptyOverrides,
        RESTCatalogConfigResponse.builder().withDefaults(DEFAULTS).withOverrides(ImmutableMap.of()).build());

    // Both are empty
    String emptyJson = "{\"defaults\":{},\"overrides\":{}}";
    assertRoundTripSerializesEquallyFrom(
        emptyJson,
        RESTCatalogConfigResponse.builder().build());
    assertRoundTripSerializesEquallyFrom(
        emptyJson,
        RESTCatalogConfigResponse.builder().withOverrides(ImmutableMap.of()).withDefaults(ImmutableMap.of()).build());
  }

  @Test
  // Test cases that cannot be built with our builder, but that are accepted when parsed
  public void testCanDeserializeWithoutDefaultValues() throws JsonProcessingException {
    RESTCatalogConfigResponse noOverrides = RESTCatalogConfigResponse.builder().withDefaults(DEFAULTS).build();
    String jsonMissingOverrides = "{\"defaults\":{\"warehouse\":\"s3://bucket/warehouse\"}}";
    assertEquals(deserialize(jsonMissingOverrides), noOverrides);
    String jsonNullOverrides = "{\"defaults\":{\"warehouse\":\"s3://bucket/warehouse\"},\"overrides\":null}";
    assertEquals(deserialize(jsonNullOverrides), noOverrides);

    RESTCatalogConfigResponse noDefaults = RESTCatalogConfigResponse.builder().withOverrides(OVERRIDES).build();
    String jsonMissingDefaults = "{\"overrides\":{\"clients\":\"5\"}}";
    assertEquals(deserialize(jsonMissingDefaults), noDefaults);
    String jsonNullDefaults = "{\"defaults\":null,\"overrides\":{\"clients\":\"5\"}}";
    assertEquals(deserialize(jsonNullDefaults), noDefaults);

    RESTCatalogConfigResponse noValues = RESTCatalogConfigResponse.builder().build();
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
        RESTCatalogConfigResponse.builder()
            .withDefaults(DEFAULTS_WITH_NULL_VALUE).withOverrides(OVERRIDES).build());
    assertRoundTripSerializesEquallyFrom(
        jsonNullValueInDefaults,
        RESTCatalogConfigResponse.builder()
            .withDefault("warehouse", null).withOverrides(OVERRIDES).build());

    String jsonNullValueInOverrides =
        "{\"defaults\":{\"warehouse\":\"s3://bucket/warehouse\"},\"overrides\":{\"clients\":null}}";
    assertRoundTripSerializesEquallyFrom(
        jsonNullValueInOverrides,
        RESTCatalogConfigResponse.builder()
            .withDefaults(DEFAULTS).withOverrides(OVERRIDES_WITH_NULL_VALUE).build());
    assertRoundTripSerializesEquallyFrom(
        jsonNullValueInOverrides,
        RESTCatalogConfigResponse.builder()
            .withDefaults(DEFAULTS).withOverride("clients", null).build());
  }

  @Test
  public void testDeserializeInvalidResponse() {
    String jsonDefaultsHasWrongType =
        "{\"defaults\":[\"warehouse\",\"s3://bucket/warehouse\"],\"overrides\":{\"clients\":\"5\"}}";
    AssertHelpers.assertThrows(
        "A JSON response with the wrong type for the defaults field should fail to deserialize",
        JsonProcessingException.class,
        () -> deserialize(jsonDefaultsHasWrongType)
    );

    String jsonOverridesHasWrongType =
        "{\"defaults\":{\"warehouse\":\"s3://bucket/warehouse\"},\"overrides\":\"clients\"}";
    AssertHelpers.assertThrows(
        "A JSON response with the wrong type for the overrides field should fail to deserialize",
        JsonProcessingException.class,
        () -> deserialize(jsonOverridesHasWrongType)
    );

    String jsonMisspelledKeys =
        "{\"defaultzzzzzzz\":{\"warehouse\":\"s3://bucket/warehouse\"},\"overrrrrrrrides\":{\"clients\":\"5\"}}";
    AssertHelpers.assertThrows(
        "A JSON response with the keys spelled incorrectly should fail to deserialize and validate",
        JsonProcessingException.class,
        () -> deserialize(jsonMisspelledKeys)
    );

    AssertHelpers.assertThrows(
        "A null JSON response body should fail to deserialize",
        IllegalArgumentException.class,
        () -> deserialize(null)
    );
  }

  @Test
  public void testBuilderDoesNotCreateInvalidObjects() {
    AssertHelpers.assertThrows(
        "The builder should not allow using null as a key in the properties to override",
        NullPointerException.class,
        "Invalid config: null",
        () -> RESTCatalogConfigResponse.builder().withOverride(null, "100").build()
    );

    AssertHelpers.assertThrows(
        "The builder should not allow using null as a key in the default properties",
        NullPointerException.class,
        "Invalid config: null",
        () -> RESTCatalogConfigResponse.builder().withDefault(null, "100").build()
    );

    AssertHelpers.assertThrows(
        "The builder should not allow passing a null map of config properties to override",
        NullPointerException.class,
        "Invalid configuration properties map: null",
        () -> RESTCatalogConfigResponse.builder().withOverrides(null).build()
    );

    AssertHelpers.assertThrows(
        "The builder should not allow passing a null map of default config properties",
        NullPointerException.class,
        "Invalid configuration properties map: null",
        () -> RESTCatalogConfigResponse.builder().withDefaults(null).build()
    );

    Map<String, String> mapWithNullKey = Maps.newHashMap();
    mapWithNullKey.put(null, "a");
    mapWithNullKey.put("b", "b");
    AssertHelpers.assertThrows(
        "The builder should not allow passing a map of default config properties with a null key",
        IllegalArgumentException.class,
        "Invalid config: null",
        () -> RESTCatalogConfigResponse.builder().withDefaults(mapWithNullKey).build()
    );

    AssertHelpers.assertThrows(
        "The builder should not allow passing a map of properties to override with a null key",
        IllegalArgumentException.class,
        "Invalid config: null",
        () -> RESTCatalogConfigResponse.builder().withOverrides(mapWithNullKey).build()
    );
  }

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"defaults", "overrides"};
  }

  @Override
  public RESTCatalogConfigResponse createExampleInstance() {
    return RESTCatalogConfigResponse.builder()
        .withDefaults(DEFAULTS)
        .withOverrides(OVERRIDES)
        .build();
  }

  @Override
  public void assertEquals(RESTCatalogConfigResponse actual, RESTCatalogConfigResponse expected) {
    Assert.assertEquals("Config properties to use as defaults should be equal",
        actual.defaults(), expected.defaults());
    Assert.assertEquals("Config properties to use as overrides should be equal",
        actual.overrides(), expected.overrides());
  }

  @Override
  public RESTCatalogConfigResponse deserialize(String json) throws JsonProcessingException {
    return mapper().readValue(json, RESTCatalogConfigResponse.class).validate();
  }
}
