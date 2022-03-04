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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RequestResponseTestBase;
import org.assertj.core.api.Assertions;
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
    String fullJson = "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":{\"owner\":\"Hank\"}}";
    CreateNamespaceRequest req = CreateNamespaceRequest.of(NAMESPACE, PROPERTIES);
    assertRoundTripSerializesEquallyFrom(fullJson, req);

    String jsonEmptyProperties = "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":{}}";
    CreateNamespaceRequest reqWithExplicitEmptyProperties = CreateNamespaceRequest.of(NAMESPACE, EMPTY_PROPERTIES);
    assertRoundTripSerializesEquallyFrom(jsonEmptyProperties, reqWithExplicitEmptyProperties);

    CreateNamespaceRequest reqWithImplicitEmptyProperties = CreateNamespaceRequest.of(NAMESPACE);
    assertRoundTripSerializesEquallyFrom(jsonEmptyProperties, reqWithImplicitEmptyProperties);

    String jsonWithEmptyNamespace = "{\"namespace\":[],\"properties\":{}}";
    CreateNamespaceRequest reqUsingEmptyNamespace = CreateNamespaceRequest.of(Namespace.empty());
    assertRoundTripSerializesEquallyFrom(jsonWithEmptyNamespace, reqUsingEmptyNamespace);
  }

  @Test
  // Test cases that can't be constructed with our Builder class but that will parse correctly
  public void testCanDeserializeWithoutDefaultValues() throws JsonProcessingException {
    CreateNamespaceRequest req = CreateNamespaceRequest.of(NAMESPACE);
    String jsonWithNullProperties = "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":null}";
    assertEquals(deserialize(jsonWithNullProperties), req);

    String jsonWithMissingProperties = "{\"namespace\":[\"accounting\",\"tax\"]}";
    assertEquals(deserialize(jsonWithMissingProperties), req);
  }

  @Test
  public void testDeserializeInvalidRequest() {
    String jsonIncorrectTypeForNamespace = "{\"namespace\":\"accounting%00tax\",\"properties\":null}";
    Assertions.assertThatThrownBy(() -> deserialize(jsonIncorrectTypeForNamespace))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageContaining("Cannot parse string array from non-array");

    String jsonIncorrectTypeForProperties = "{\"namespace\":[\"accounting\",\"tax\"],\"properties\":[]}";
    Assertions.assertThatThrownBy(() -> deserialize(jsonIncorrectTypeForProperties))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageContaining("Cannot deserialize instance of");

    String jsonMisspelledNamespace = "{\"namepsace\":[\"accounting\",\"tax\"],\"properties\":{\"owner\":\"Hank\"}}";
    Assertions.assertThatThrownBy(() -> deserialize(jsonMisspelledNamespace))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageContaining("Unrecognized field \"namepsace\"");

    String jsonMisspelledProperties = "{\"namespace\":[\"accounting\",\"tax\"],\"propertiezzzz\":{\"owner\":\"Hank\"}}";
    Assertions.assertThatThrownBy(() -> deserialize(jsonMisspelledProperties))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageContaining("Unrecognized field \"propertiezzzz\"");

    String emptyJson = "{}";
    Assertions.assertThatThrownBy(() -> deserialize(emptyJson))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageContaining("Cannot build CreateNamespaceRequest, " +
                "some of required attributes are not set [namespace]");

    Assertions.assertThatThrownBy(() -> deserialize(null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testBuilderDoesNotBuildInvalidRequests() {
    Assertions.assertThatThrownBy(() -> CreateNamespaceRequest.of(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("namespace");

    Assertions.assertThat(CreateNamespaceRequest.of(NAMESPACE, null))
        .extracting("properties")
        .isEqualTo(ImmutableMap.of());

    Map<String, String> mapWithNullKey = Maps.newHashMap();
    mapWithNullKey.put(null, "hello");
    Assertions.assertThatThrownBy(() -> CreateNamespaceRequest.of(NAMESPACE, mapWithNullKey))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("properties key");

    Map<String, String> mapWithMultipleNullValues = Maps.newHashMap();
    mapWithMultipleNullValues.put("a", null);
    mapWithMultipleNullValues.put("b", "b");
    Assertions.assertThatThrownBy(() -> CreateNamespaceRequest.of(NAMESPACE, mapWithMultipleNullValues))
         .isInstanceOf(NullPointerException.class)
         .hasMessage("properties value");
  }

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"namespace", "properties"};
  }

  @Override
  public CreateNamespaceRequest createExampleInstance() {
    return CreateNamespaceRequest.of(NAMESPACE, PROPERTIES);
  }

  @Override
  public void assertEquals(CreateNamespaceRequest actual, CreateNamespaceRequest expected) {
    Assert.assertEquals("Namespaces should be equal", actual.namespace(), expected.namespace());
    Assert.assertEquals("Properties should be equal", actual.properties(), expected.properties());
  }

  @Override
  public CreateNamespaceRequest deserialize(String json) throws JsonProcessingException {
    return mapper().readValue(json, CreateNamespaceRequest.class);
  }
}
