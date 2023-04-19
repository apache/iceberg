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

public class TestGetNamespaceResponse extends RequestResponseTestBase<GetNamespaceResponse> {

  /* Values used to fill in request fields */
  private static final Namespace NAMESPACE = Namespace.of("accounting", "tax");
  private static final Map<String, String> PROPERTIES = ImmutableMap.of("owner", "Hank");
  private static final Map<String, String> EMPTY_PROPERTIES = ImmutableMap.of();

  @Test
  // Test cases that are JSON that can be created via the Builder
  public void testRoundTripSerDe() throws JsonProcessingException {
    String fullJson =
        "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":{\"owner\":\"Hank\"}}";
    GetNamespaceResponse fullValue =
        GetNamespaceResponse.builder().withNamespace(NAMESPACE).setProperties(PROPERTIES).build();
    assertRoundTripSerializesEquallyFrom(fullJson, fullValue);

    String emptyProps = "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":{}}";
    assertRoundTripSerializesEquallyFrom(
        emptyProps, GetNamespaceResponse.builder().withNamespace(NAMESPACE).build());
    assertRoundTripSerializesEquallyFrom(
        emptyProps,
        GetNamespaceResponse.builder()
            .withNamespace(NAMESPACE)
            .setProperties(EMPTY_PROPERTIES)
            .build());
  }

  @Test
  // Test cases that can't be constructed with our Builder class but that will parse correctly
  public void testCanDeserializeWithoutDefaultValues() throws JsonProcessingException {
    GetNamespaceResponse withoutProps =
        GetNamespaceResponse.builder().withNamespace(NAMESPACE).build();
    String jsonWithNullProperties = "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":null}";
    assertEquals(deserialize(jsonWithNullProperties), withoutProps);
  }

  @Test
  public void testDeserializeInvalidResponse() {
    String jsonNamespaceHasWrongType = "{\"namespace\":\"accounting%1Ftax\",\"properties\":null}";
    AssertHelpers.assertThrows(
        "A JSON response with the wrong type for a field should fail to deserialize",
        JsonProcessingException.class,
        "Cannot parse string array from non-array",
        () -> deserialize(jsonNamespaceHasWrongType));

    String jsonPropertiesHasWrongType =
        "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":[]}";
    AssertHelpers.assertThrows(
        "A JSON response with the wrong type for a field should fail to deserialize",
        JsonProcessingException.class,
        () -> deserialize(jsonPropertiesHasWrongType));

    String emptyJson = "{}";
    AssertHelpers.assertThrows(
        "An empty JSON request should fail to deserialize after validation",
        IllegalArgumentException.class,
        "Invalid namespace: null",
        () -> deserialize(emptyJson));

    String jsonWithKeysSpelledIncorrectly =
        "{\"namepsace\":[\"accounting\",\"tax\"],\"propertiezzzz\":{\"owner\":\"Hank\"}}";
    AssertHelpers.assertThrows(
        "A JSON response with the keys spelled incorrectly should fail to deserialize",
        IllegalArgumentException.class,
        "Invalid namespace: null",
        () -> deserialize(jsonWithKeysSpelledIncorrectly));

    String nullJson = null;
    AssertHelpers.assertThrows(
        "An empty JSON request should fail to deserialize",
        IllegalArgumentException.class,
        () -> deserialize(nullJson));
  }

  @Test
  public void testBuilderDoesNotBuildInvalidRequests() {
    AssertHelpers.assertThrows(
        "The builder should not allow using null for the namespace",
        NullPointerException.class,
        "Invalid namespace: null",
        () -> GetNamespaceResponse.builder().withNamespace(null).build());

    AssertHelpers.assertThrows(
        "The builder should not allow passing a null collection of properties",
        NullPointerException.class,
        "Invalid properties map: null",
        () -> GetNamespaceResponse.builder().setProperties(null).build());

    Map<String, String> mapWithNullKey = Maps.newHashMap();
    mapWithNullKey.put(null, "hello");
    AssertHelpers.assertThrows(
        "The builder should not allow using null as a key in the properties to set",
        IllegalArgumentException.class,
        "Invalid property: null",
        () -> GetNamespaceResponse.builder().setProperties(mapWithNullKey).build());

    Map<String, String> mapWithMultipleNullValues = Maps.newHashMap();
    mapWithMultipleNullValues.put("a", null);
    mapWithMultipleNullValues.put("b", "b");
    AssertHelpers.assertThrows(
        "The builder should not allow using null as a value in the properties to set",
        IllegalArgumentException.class,
        "Invalid value for properties [a]: null",
        () -> GetNamespaceResponse.builder().setProperties(mapWithMultipleNullValues).build());
  }

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"namespace", "properties"};
  }

  @Override
  public GetNamespaceResponse createExampleInstance() {
    return GetNamespaceResponse.builder()
        .withNamespace(NAMESPACE)
        .setProperties(PROPERTIES)
        .build();
  }

  @Override
  public void assertEquals(GetNamespaceResponse actual, GetNamespaceResponse expected) {
    Assert.assertEquals("Namespace should be equal", actual.namespace(), expected.namespace());
    Assert.assertEquals("Properties should be equal", actual.properties(), expected.properties());
  }

  @Override
  public GetNamespaceResponse deserialize(String json) throws JsonProcessingException {
    GetNamespaceResponse resp = mapper().readValue(json, GetNamespaceResponse.class);
    resp.validate();
    return resp;
  }
}
