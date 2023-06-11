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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RequestResponseTestBase;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCreateNamespaceResponse extends RequestResponseTestBase<CreateNamespaceResponse> {

  /* Values used to fill in response fields */
  private static final Namespace NAMESPACE = Namespace.of("accounting", "tax");
  private static final Map<String, String> PROPERTIES = ImmutableMap.of("owner", "Hank");
  private static final Map<String, String> EMPTY_PROPERTIES = ImmutableMap.of();

  @Test
  // Test cases that are JSON that can be created via the Builder
  public void testRoundTripSerDe() throws JsonProcessingException {
    String fullJson =
        "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":{\"owner\":\"Hank\"}}";
    CreateNamespaceResponse req =
        CreateNamespaceResponse.builder()
            .withNamespace(NAMESPACE)
            .setProperties(PROPERTIES)
            .build();
    assertRoundTripSerializesEquallyFrom(fullJson, req);

    String jsonEmptyProperties = "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":{}}";
    CreateNamespaceResponse responseWithExplicitEmptyProperties =
        CreateNamespaceResponse.builder()
            .withNamespace(NAMESPACE)
            .setProperties(EMPTY_PROPERTIES)
            .build();
    assertRoundTripSerializesEquallyFrom(jsonEmptyProperties, responseWithExplicitEmptyProperties);

    CreateNamespaceResponse responseWithImplicitEmptyProperties =
        CreateNamespaceResponse.builder().withNamespace(NAMESPACE).build();
    assertRoundTripSerializesEquallyFrom(jsonEmptyProperties, responseWithImplicitEmptyProperties);

    String jsonEmptyNamespace = "{\"namespace\":[],\"properties\":{}}";
    CreateNamespaceResponse responseWithEmptyNamespace =
        CreateNamespaceResponse.builder().withNamespace(Namespace.empty()).build();
    assertRoundTripSerializesEquallyFrom(jsonEmptyNamespace, responseWithEmptyNamespace);
  }

  @Test
  public void testCanDeserializeWithoutDefaultValues() throws JsonProcessingException {
    CreateNamespaceResponse noProperties =
        CreateNamespaceResponse.builder().withNamespace(NAMESPACE).build();
    String jsonWithMissingProperties = "{\"namespace\":[\"accounting\",\"tax\"]}";
    assertEquals(deserialize(jsonWithMissingProperties), noProperties);

    String jsonWithNullProperties = "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":null}";
    assertEquals(deserialize(jsonWithNullProperties), noProperties);

    String jsonEmptyNamespaceMissingProperties = "{\"namespace\":[]}";
    CreateNamespaceResponse responseWithEmptyNamespace =
        CreateNamespaceResponse.builder().withNamespace(Namespace.empty()).build();
    assertEquals(deserialize(jsonEmptyNamespaceMissingProperties), responseWithEmptyNamespace);
  }

  @Test
  public void testDeserializeInvalidResponse() {
    String jsonResponseMalformedNamespaceValue =
        "{\"namespace\":\"accounting%1Ftax\",\"properties\":null}";
    Assertions.assertThatThrownBy(() -> deserialize(jsonResponseMalformedNamespaceValue))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageContaining("Cannot parse string array from non-array");

    String jsonResponsePropertiesHasWrongType =
        "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":[]}";
    Assertions.assertThatThrownBy(() -> deserialize(jsonResponsePropertiesHasWrongType))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageContaining(
            "Cannot deserialize value of type `java.util.LinkedHashMap<java.lang.String,java.lang.String>`");

    Assertions.assertThatThrownBy(() -> deserialize("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid namespace: null");

    String jsonMisspelledKeys =
        "{\"namepsace\":[\"accounting\",\"tax\"],\"propertiezzzz\":{\"owner\":\"Hank\"}}";
    Assertions.assertThatThrownBy(() -> deserialize(jsonMisspelledKeys))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid namespace: null");

    Assertions.assertThatThrownBy(() -> deserialize(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("argument \"content\" is null");
  }

  @Test
  public void testBuilderDoesNotBuildInvalidRequests() {
    Assertions.assertThatThrownBy(
            () -> CreateNamespaceResponse.builder().withNamespace(null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid namespace: null");

    Assertions.assertThatThrownBy(
            () -> CreateNamespaceResponse.builder().setProperties(null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid collection of properties: null");

    Map<String, String> mapWithNullKey = Maps.newHashMap();
    mapWithNullKey.put(null, "hello");
    Assertions.assertThatThrownBy(
            () -> CreateNamespaceResponse.builder().setProperties(mapWithNullKey).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid property to set: null");

    Map<String, String> mapWithMultipleNullValues = Maps.newHashMap();
    mapWithMultipleNullValues.put("a", null);
    mapWithMultipleNullValues.put("b", "b");
    Assertions.assertThatThrownBy(
            () ->
                CreateNamespaceResponse.builder().setProperties(mapWithMultipleNullValues).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid value to set for properties [a]: null");
  }

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"namespace", "properties"};
  }

  @Override
  public CreateNamespaceResponse createExampleInstance() {
    return CreateNamespaceResponse.builder()
        .withNamespace(NAMESPACE)
        .setProperties(PROPERTIES)
        .build();
  }

  @Override
  public void assertEquals(CreateNamespaceResponse actual, CreateNamespaceResponse expected) {
    Assertions.assertThat(actual.namespace()).isEqualTo(expected.namespace());
    Assertions.assertThat(actual.properties()).isEqualTo(expected.properties());
  }

  @Override
  public CreateNamespaceResponse deserialize(String json) throws JsonProcessingException {
    CreateNamespaceResponse response = mapper().readValue(json, CreateNamespaceResponse.class);
    response.validate();
    return response;
  }
}
