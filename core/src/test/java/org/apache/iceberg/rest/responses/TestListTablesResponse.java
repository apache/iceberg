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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.RequestResponseTestBase;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestListTablesResponse extends RequestResponseTestBase<ListTablesResponse> {

  private static final List<TableIdentifier> IDENTIFIERS =
      ImmutableList.of(TableIdentifier.of(Namespace.of("accounting", "tax"), "paid"));

  @Test
  public void testRoundTripSerDe() throws JsonProcessingException {
    String fullJson =
        "{\"identifiers\":[{\"namespace\":[\"accounting\",\"tax\"],\"name\":\"paid\"}]}";
    assertRoundTripSerializesEquallyFrom(
        fullJson, ListTablesResponse.builder().addAll(IDENTIFIERS).build());

    String emptyIdentifiers = "{\"identifiers\":[]}";
    assertRoundTripSerializesEquallyFrom(emptyIdentifiers, ListTablesResponse.builder().build());
  }

  @Test
  public void testDeserializeInvalidResponsesThrows() {
    String identifiersHasWrongType = "{\"identifiers\":\"accounting%1Ftax\"}";
    Assertions.assertThatThrownBy(() -> deserialize(identifiersHasWrongType))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageContaining(
            "Cannot deserialize value of type `java.util.ArrayList<org.apache.iceberg.catalog.TableIdentifier>`");

    String emptyJson = "{}";
    Assertions.assertThatThrownBy(() -> deserialize(emptyJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid identifier list: null");

    String jsonWithKeysSpelledIncorrectly =
        "{\"identifyrezzzz\":[{\"namespace\":[\"accounting\",\"tax\"],\"name\":\"paid\"}]}";
    Assertions.assertThatThrownBy(() -> deserialize(jsonWithKeysSpelledIncorrectly))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid identifier list: null");

    String jsonWithInvalidIdentifiersInList =
        "{\"identifiers\":[{\"namespace\":\"accounting.tax\",\"name\":\"paid\"}]}";
    Assertions.assertThatThrownBy(() -> deserialize(jsonWithInvalidIdentifiersInList))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageContaining(
            "Cannot parse JSON array from non-array value: namespace: \"accounting.tax\"");

    String jsonWithInvalidIdentifiersInList2 =
        "{\"identifiers\":[{\"namespace\":[\"accounting\",\"tax\"],\"name\":\"paid\"},\"accounting.tax.paid\"]}";
    Assertions.assertThatThrownBy(() -> deserialize(jsonWithInvalidIdentifiersInList2))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageContaining("Cannot parse missing or non-object table identifier");

    String jsonWithInvalidTypeForNamePartOfIdentifier =
        "{\"identifiers\":[{\"namespace\":[\"accounting\",\"tax\"],\"name\":true}]}";
    Assertions.assertThatThrownBy(() -> deserialize(jsonWithInvalidTypeForNamePartOfIdentifier))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageContaining("Cannot parse to a string value");

    String nullJson = null;
    Assertions.assertThatThrownBy(() -> deserialize(nullJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("argument \"content\" is null");
  }

  @Test
  public void testBuilderDoesNotCreateInvalidObjects() {
    Assertions.assertThatThrownBy(() -> ListTablesResponse.builder().add(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid table identifier: null");

    Assertions.assertThatThrownBy(() -> ListTablesResponse.builder().addAll(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid table identifier list: null");

    List<TableIdentifier> listWithNullElement =
        Lists.newArrayList(TableIdentifier.of(Namespace.of("foo"), "bar"), null);
    Assertions.assertThatThrownBy(() -> ListTablesResponse.builder().addAll(listWithNullElement))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table identifier: null");
  }

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"identifiers"};
  }

  @Override
  public ListTablesResponse createExampleInstance() {
    return ListTablesResponse.builder().addAll(IDENTIFIERS).build();
  }

  @Override
  public void assertEquals(ListTablesResponse actual, ListTablesResponse expected) {
    Assertions.assertThat(actual.identifiers())
        .as("Identifiers should be equal")
        .hasSameSizeAs(expected.identifiers())
        .containsExactlyInAnyOrderElementsOf(expected.identifiers());
  }

  @Override
  public ListTablesResponse deserialize(String json) throws JsonProcessingException {
    ListTablesResponse resp = mapper().readValue(json, ListTablesResponse.class);
    resp.validate();
    return resp;
  }
}
