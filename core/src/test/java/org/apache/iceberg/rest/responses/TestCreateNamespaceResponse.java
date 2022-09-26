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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RequestResponseTestBase;
import org.junit.Assert;
import org.junit.Test;

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
    AssertHelpers.assertThrows(
        "A JSON response with the wrong type for the namespace field should fail to deserialize",
        JsonProcessingException.class,
        "Cannot parse string array from non-array",
        () -> deserialize(jsonResponseMalformedNamespaceValue));

    String jsonResponsePropertiesHasWrongType =
        "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":[]}";
    AssertHelpers.assertThrows(
        "A JSON response with the wrong type for the properties field should fail to deserialize",
        JsonProcessingException.class,
        () -> deserialize(jsonResponsePropertiesHasWrongType));

    AssertHelpers.assertThrows(
        "An empty JSON response should fail to deserialize",
        IllegalArgumentException.class,
        "Invalid namespace: null",
        () -> deserialize("{}"));

    String jsonMisspelledKeys =
        "{\"namepsace\":[\"accounting\",\"tax\"],\"propertiezzzz\":{\"owner\":\"Hank\"}}";
    AssertHelpers.assertThrows(
        "A JSON response with the keys spelled incorrectly should fail to deserialize and validate",
        IllegalArgumentException.class,
        "Invalid namespace: null",
        () -> deserialize(jsonMisspelledKeys));

    AssertHelpers.assertThrows(
        "A null JSON response body should fail to deserialize",
        IllegalArgumentException.class,
        () -> deserialize(null));
  }

  @Test
  public void testBuilderDoesNotBuildInvalidRequests() {
    AssertHelpers.assertThrows(
        "The builder should not allow using null for the namespace",
        NullPointerException.class,
        "Invalid namespace: null",
        () -> CreateNamespaceResponse.builder().withNamespace(null).build());

    AssertHelpers.assertThrows(
        "The builder should not allow passing a null collection of properties",
        NullPointerException.class,
        "Invalid collection of properties: null",
        () -> CreateNamespaceResponse.builder().setProperties(null).build());

    Map<String, String> mapWithNullKey = Maps.newHashMap();
    mapWithNullKey.put(null, "hello");
    AssertHelpers.assertThrows(
        "The builder should not allow using null as a key in the properties to set",
        IllegalArgumentException.class,
        "Invalid property to set: null",
        () -> CreateNamespaceResponse.builder().setProperties(mapWithNullKey).build());

    Map<String, String> mapWithMultipleNullValues = Maps.newHashMap();
    mapWithMultipleNullValues.put("a", null);
    mapWithMultipleNullValues.put("b", "b");
    AssertHelpers.assertThrows(
        "The builder should not allow using null as a value in the properties to set",
        IllegalArgumentException.class,
        "Invalid value to set for properties [a]: null",
        () -> CreateNamespaceResponse.builder().setProperties(mapWithMultipleNullValues).build());
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
    Assert.assertEquals("Namespaces should be equal", actual.namespace(), expected.namespace());
    Assert.assertEquals("Properties should be equal", actual.properties(), expected.properties());
  }

  @Override
  public CreateNamespaceResponse deserialize(String json) throws JsonProcessingException {
    CreateNamespaceResponse response = mapper().readValue(json, CreateNamespaceResponse.class);
    response.validate();
    return response;
  }
}
