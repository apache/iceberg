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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.RequestResponseTestBase;
import org.junit.Assert;
import org.junit.Test;

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
    AssertHelpers.assertThrows(
        "A JSON response with the incorrect type for the field identifiers should fail to parse",
        JsonProcessingException.class,
        () -> deserialize(identifiersHasWrongType));

    String emptyJson = "{}";
    AssertHelpers.assertThrows(
        "An empty JSON response should fail to deserialize",
        IllegalArgumentException.class,
        "Invalid identifier list: null",
        () -> deserialize(emptyJson));

    String jsonWithKeysSpelledIncorrectly =
        "{\"identifyrezzzz\":[{\"namespace\":[\"accounting\",\"tax\"],\"name\":\"paid\"}]}";
    AssertHelpers.assertThrows(
        "A JSON response with the keys spelled incorrectly should fail to deserialize",
        IllegalArgumentException.class,
        "Invalid identifier list: null",
        () -> deserialize(jsonWithKeysSpelledIncorrectly));

    String jsonWithInvalidIdentifiersInList =
        "{\"identifiers\":[{\"namespace\":\"accounting.tax\",\"name\":\"paid\"}]}";
    AssertHelpers.assertThrows(
        "A JSON response with an invalid identifier in the list of identifiers should fail to parse",
        JsonProcessingException.class,
        () -> deserialize(jsonWithInvalidIdentifiersInList));

    String jsonWithInvalidIdentifiersInList2 =
        "{\"identifiers\":[{\"namespace\":[\"accounting\",\"tax\"],\"name\":\"paid\"},\"accounting.tax.paid\"]}";
    AssertHelpers.assertThrows(
        "A JSON response with an invalid identifier in the list of identifiers should fail to parse",
        JsonProcessingException.class,
        () -> deserialize(jsonWithInvalidIdentifiersInList2));

    String jsonWithInvalidTypeForNamePartOfIdentifier =
        "{\"identifiers\":[{\"namespace\":[\"accounting\",\"tax\"],\"name\":true}]}";
    AssertHelpers.assertThrows(
        "A JSON response with an invalid identifier in the list of identifiers should fail to parse",
        JsonProcessingException.class,
        () -> deserialize(jsonWithInvalidTypeForNamePartOfIdentifier));

    String nullJson = null;
    AssertHelpers.assertThrows(
        "A null JSON response should fail to deserialize",
        IllegalArgumentException.class,
        () -> deserialize(nullJson));
  }

  @Test
  public void testBuilderDoesNotCreateInvalidObjects() {
    AssertHelpers.assertThrows(
        "The builder should not allow using null as a table identifier to add to the list",
        NullPointerException.class,
        "Invalid table identifier: null",
        () -> ListTablesResponse.builder().add(null).build());

    AssertHelpers.assertThrows(
        "The builder should not allow passing a null list of table identifiers to add",
        NullPointerException.class,
        "Invalid table identifier list: null",
        () -> ListTablesResponse.builder().addAll(null).build());

    List<TableIdentifier> listWithNullElement =
        Lists.newArrayList(TableIdentifier.of(Namespace.of("foo"), "bar"), null);
    AssertHelpers.assertThrows(
        "The builder should not allow passing a collection of table identifiers with a null element in it",
        IllegalArgumentException.class,
        "Invalid table identifier: null",
        () -> ListTablesResponse.builder().addAll(listWithNullElement).build());
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
    Assert.assertTrue(
        "Identifiers should be equal",
        actual.identifiers().size() == expected.identifiers().size()
            && Sets.newHashSet(actual.identifiers())
                .equals(Sets.newHashSet(expected.identifiers())));
  }

  @Override
  public ListTablesResponse deserialize(String json) throws JsonProcessingException {
    ListTablesResponse resp = mapper().readValue(json, ListTablesResponse.class);
    resp.validate();
    return resp;
  }
}
