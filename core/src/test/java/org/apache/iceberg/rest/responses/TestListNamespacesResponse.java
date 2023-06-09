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
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.RequestResponseTestBase;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestListNamespacesResponse extends RequestResponseTestBase<ListNamespacesResponse> {

  private static final List<Namespace> NAMESPACES =
      ImmutableList.of(Namespace.of("accounting"), Namespace.of("tax"));

  @Test
  public void testRoundTripSerDe() throws JsonProcessingException {
    String fullJson = "{\"namespaces\":[[\"accounting\"],[\"tax\"]]}";
    ListNamespacesResponse fullValue = ListNamespacesResponse.builder().addAll(NAMESPACES).build();
    assertRoundTripSerializesEquallyFrom(fullJson, fullValue);

    String emptyNamespaces = "{\"namespaces\":[]}";
    assertRoundTripSerializesEquallyFrom(emptyNamespaces, ListNamespacesResponse.builder().build());
  }

  @Test
  public void testDeserializeInvalidResponseThrows() {
    String jsonNamespacesHasWrongType = "{\"namespaces\":\"accounting\"}";
    Assertions.assertThatThrownBy(() -> deserialize(jsonNamespacesHasWrongType))
        .as("A malformed JSON response with the wrong type for a field should fail to deserialize")
        .isInstanceOf(JsonProcessingException.class);

    String emptyJson = "{}";
    Assertions.assertThatThrownBy(() -> deserialize(emptyJson))
        .as("An empty JSON response will deserialize, but not into a valid object")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid namespace: null");

    String jsonWithKeysSpelledIncorrectly = "{\"namepsacezz\":[\"accounting\",\"tax\"]}";
    Assertions.assertThatThrownBy(() -> deserialize(jsonWithKeysSpelledIncorrectly))
        .as("A JSON response with the keys spelled incorrectly should fail to deserialize")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid namespace: null");

    String nullJson = null;
    Assertions.assertThatThrownBy(() -> deserialize(nullJson))
        .as("A null JSON response should fail to deserialize")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testBuilderDoesNotCreateInvalidObjects() {
    Assertions.assertThatThrownBy(() -> ListNamespacesResponse.builder().add(null).build())
        .as("The builder should not allow using null as a namespace to add to the list")
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Invalid namespace: null");

    Assertions.assertThatThrownBy(() -> ListNamespacesResponse.builder().addAll(null).build())
        .as("The builder should not allow passing a null list of namespaces to add")
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Invalid namespace list: null");

    List<Namespace> listWithNullElement = Lists.newArrayList(Namespace.of("a"), null);
    Assertions.assertThatThrownBy(
            () -> ListNamespacesResponse.builder().addAll(listWithNullElement).build())
        .as(
            "The builder should not allow passing a collection of namespaces with a null element in it")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid namespace: null");
  }

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"namespaces"};
  }

  @Override
  public ListNamespacesResponse createExampleInstance() {
    return ListNamespacesResponse.builder().addAll(NAMESPACES).build();
  }

  @Override
  public void assertEquals(ListNamespacesResponse actual, ListNamespacesResponse expected) {
    Assertions.assertThat(actual.namespaces())
        .as("Namespaces list should be equal")
        .hasSize(expected.namespaces().size())
        .containsExactlyInAnyOrderElementsOf(expected.namespaces());
  }

  @Override
  public ListNamespacesResponse deserialize(String json) throws JsonProcessingException {
    ListNamespacesResponse resp = mapper().readValue(json, ListNamespacesResponse.class);
    resp.validate();
    return resp;
  }
}
