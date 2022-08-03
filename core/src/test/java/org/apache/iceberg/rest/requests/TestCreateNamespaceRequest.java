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
package org.apache.iceberg.rest.requests;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RequestResponseTestBase;
import org.junit.Assert;
import org.junit.Test;

public class TestCreateNamespaceRequest extends RequestResponseTestBase<CreateNamespaceRequest> {

  /* Values used to fill in request fields */
  private static final Namespace NAMESPACE = Namespace.of("accounting", "tax");
  private static final Map<String, String> PROPERTIES = ImmutableMap.of("owner", "Hank");
  private static final Map<String, String> EMPTY_PROPERTIES = ImmutableMap.of();

  @Test
  // Test cases that are JSON that can be created via the Builder
  public void testRoundTripSerDe() throws JsonProcessingException {
    String fullJson =
        "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":{\"owner\":\"Hank\"}}";
    CreateNamespaceRequest req =
        CreateNamespaceRequest.builder().withNamespace(NAMESPACE).setProperties(PROPERTIES).build();
    assertRoundTripSerializesEquallyFrom(fullJson, req);

    String jsonEmptyProperties = "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":{}}";
    CreateNamespaceRequest reqWithExplicitEmptyProperties =
        CreateNamespaceRequest.builder()
            .withNamespace(NAMESPACE)
            .setProperties(EMPTY_PROPERTIES)
            .build();
    assertRoundTripSerializesEquallyFrom(jsonEmptyProperties, reqWithExplicitEmptyProperties);

    CreateNamespaceRequest reqWithImplicitEmptyProperties =
        CreateNamespaceRequest.builder().withNamespace(NAMESPACE).build();
    assertRoundTripSerializesEquallyFrom(jsonEmptyProperties, reqWithImplicitEmptyProperties);

    String jsonWithEmptyNamespace = "{\"namespace\":[],\"properties\":{}}";
    CreateNamespaceRequest reqUsingEmptyNamespace =
        CreateNamespaceRequest.builder().withNamespace(Namespace.empty()).build();
    assertRoundTripSerializesEquallyFrom(jsonWithEmptyNamespace, reqUsingEmptyNamespace);
  }

  @Test
  // Test cases that can't be constructed with our Builder class but that will parse correctly
  public void testCanDeserializeWithoutDefaultValues() throws JsonProcessingException {
    CreateNamespaceRequest req = CreateNamespaceRequest.builder().withNamespace(NAMESPACE).build();
    String jsonWithNullProperties = "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":null}";
    assertEquals(deserialize(jsonWithNullProperties), req);

    String jsonWithMissingProperties = "{\"namespace\":[\"accounting\",\"tax\"]}";
    assertEquals(deserialize(jsonWithMissingProperties), req);
  }

  @Test
  public void testDeserializeInvalidRequest() {
    String jsonIncorrectTypeForNamespace =
        "{\"namespace\":\"accounting%1Ftax\",\"properties\":null}";
    AssertHelpers.assertThrows(
        "A JSON request with incorrect types for fields should fail to deserialize and validate",
        JsonProcessingException.class,
        "Cannot parse string array from non-array",
        () -> deserialize(jsonIncorrectTypeForNamespace));

    String jsonIncorrectTypeForProperties =
        "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":[]}";
    AssertHelpers.assertThrows(
        "A JSON request with incorrect types for fields should fail to parse and validate",
        JsonProcessingException.class,
        () -> deserialize(jsonIncorrectTypeForProperties));

    String jsonMisspelledKeys =
        "{\"namepsace\":[\"accounting\",\"tax\"],\"propertiezzzz\":{\"owner\":\"Hank\"}}";
    AssertHelpers.assertThrows(
        "A JSON request with the keys spelled incorrectly should fail to deserialize and validate",
        IllegalArgumentException.class,
        "Invalid namespace: null",
        () -> deserialize(jsonMisspelledKeys));

    String emptyJson = "{}";
    AssertHelpers.assertThrows(
        "An empty JSON object should not parse into a CreateNamespaceRequest instance that passes validation",
        IllegalArgumentException.class,
        "Invalid namespace: null",
        () -> deserialize(emptyJson));

    AssertHelpers.assertThrows(
        "An empty JSON request should fail to deserialize",
        IllegalArgumentException.class,
        () -> deserialize(null));
  }

  @Test
  public void testBuilderDoesNotBuildInvalidRequests() {
    AssertHelpers.assertThrows(
        "The builder should not allow using null for the namespace",
        NullPointerException.class,
        "Invalid namespace: null",
        () -> CreateNamespaceRequest.builder().withNamespace(null).build());

    AssertHelpers.assertThrows(
        "The builder should not allow passing a null collection of properties",
        NullPointerException.class,
        "Invalid collection of properties: null",
        () -> CreateNamespaceRequest.builder().setProperties(null).build());

    Map<String, String> mapWithNullKey = Maps.newHashMap();
    mapWithNullKey.put(null, "hello");
    AssertHelpers.assertThrows(
        "The builder should not allow using null as a key in the properties to set",
        IllegalArgumentException.class,
        "Invalid property: null",
        () -> CreateNamespaceRequest.builder().setProperties(mapWithNullKey).build());

    Map<String, String> mapWithMultipleNullValues = Maps.newHashMap();
    mapWithMultipleNullValues.put("a", null);
    mapWithMultipleNullValues.put("b", "b");
    AssertHelpers.assertThrows(
        "The builder should not allow using null as a value in the properties to set",
        IllegalArgumentException.class,
        "Invalid value for properties [a]: null",
        () -> CreateNamespaceRequest.builder().setProperties(mapWithMultipleNullValues).build());
  }

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"namespace", "properties"};
  }

  @Override
  public CreateNamespaceRequest createExampleInstance() {
    return CreateNamespaceRequest.builder()
        .withNamespace(NAMESPACE)
        .setProperties(PROPERTIES)
        .build();
  }

  @Override
  public void assertEquals(CreateNamespaceRequest actual, CreateNamespaceRequest expected) {
    Assert.assertEquals("Namespaces should be equal", actual.namespace(), expected.namespace());
    Assert.assertEquals("Properties should be equal", actual.properties(), expected.properties());
  }

  @Override
  public CreateNamespaceRequest deserialize(String json) throws JsonProcessingException {
    CreateNamespaceRequest request = mapper().readValue(json, CreateNamespaceRequest.class);
    request.validate();
    return request;
  }
}
