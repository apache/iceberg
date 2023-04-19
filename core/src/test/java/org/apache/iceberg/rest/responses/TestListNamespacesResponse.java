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
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.RequestResponseTestBase;
import org.junit.Assert;
import org.junit.Test;

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
    AssertHelpers.assertThrows(
        "A malformed JSON response with the wrong type for a field should fail to deserialize",
        JsonProcessingException.class,
        () -> deserialize(jsonNamespacesHasWrongType));

    String emptyJson = "{}";
    AssertHelpers.assertThrows(
        "An empty JSON response will deserialize, but not into a valid object",
        IllegalArgumentException.class,
        "Invalid namespace: null",
        () -> deserialize(emptyJson));

    String jsonWithKeysSpelledIncorrectly = "{\"namepsacezz\":[\"accounting\",\"tax\"]}";
    AssertHelpers.assertThrows(
        "A JSON response with the keys spelled incorrectly should fail to deserialize",
        IllegalArgumentException.class,
        "Invalid namespace: null",
        () -> deserialize(jsonWithKeysSpelledIncorrectly));

    String nullJson = null;
    AssertHelpers.assertThrows(
        "A null JSON response should fail to deserialize",
        IllegalArgumentException.class,
        () -> deserialize(nullJson));
  }

  @Test
  public void testBuilderDoesNotCreateInvalidObjects() {
    AssertHelpers.assertThrows(
        "The builder should not allow using null as a namespace to add to the list",
        NullPointerException.class,
        "Invalid namespace: null",
        () -> ListNamespacesResponse.builder().add(null).build());

    AssertHelpers.assertThrows(
        "The builder should not allow passing a null list of namespaces to add",
        NullPointerException.class,
        "Invalid namespace list: null",
        () -> ListNamespacesResponse.builder().addAll(null).build());

    List<Namespace> listWithNullElement = Lists.newArrayList(Namespace.of("a"), null);
    AssertHelpers.assertThrows(
        "The builder should not allow passing a collection of namespaces with a null element in it",
        IllegalArgumentException.class,
        "Invalid namespace: null",
        () -> ListNamespacesResponse.builder().addAll(listWithNullElement).build());
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
    Assert.assertTrue(
        "Namespaces list should be equal",
        actual.namespaces().size() == expected.namespaces().size()
            && Sets.newHashSet(actual.namespaces()).equals(Sets.newHashSet(expected.namespaces())));
  }

  @Override
  public ListNamespacesResponse deserialize(String json) throws JsonProcessingException {
    ListNamespacesResponse resp = mapper().readValue(json, ListNamespacesResponse.class);
    resp.validate();
    return resp;
  }
}
