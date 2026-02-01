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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import org.apache.iceberg.catalog.IndexIdentifier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.RequestResponseTestBase;
import org.junit.jupiter.api.Test;

public class TestListIndexesResponse extends RequestResponseTestBase<ListIndexesResponse> {

  private static final List<IndexIdentifier> IDENTIFIERS =
      ImmutableList.of(
          IndexIdentifier.of(Namespace.of("accounting", "tax"), "paid", "customer_idx"));

  private static final String FULL_JSON =
      """
      {
        "identifiers": [
          {
            "namespace": ["accounting", "tax"],
            "table": "paid",
            "name": "customer_idx"
          }
        ],
        "next-page-token": null
      }
      """
          .replaceAll("\\s+", "");

  private static final String EMPTY_IDENTIFIERS_JSON =
      """
      {
        "identifiers": [],
        "next-page-token": null
      }
      """
          .replaceAll("\\s+", "");

  @Test
  public void testRoundTripSerDe() throws JsonProcessingException {
    assertRoundTripSerializesEquallyFrom(
        FULL_JSON, ListIndexesResponse.builder().addAll(IDENTIFIERS).build());

    assertRoundTripSerializesEquallyFrom(
        EMPTY_IDENTIFIERS_JSON, ListIndexesResponse.builder().build());
  }

  @Test
  public void testDeserializeInvalidResponsesThrows() {
    assertThatThrownBy(() -> deserialize("{\"identifiers\":\"accounting%1Ftax\"}"))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageContaining("Cannot deserialize");

    assertThatThrownBy(() -> deserialize("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid identifier list: null");

    String jsonWithKeysSpelledIncorrectly =
        """
        {
          "identifyrezzzz": [
            {
              "namespace": ["accounting", "tax"],
              "table": "paid",
              "name": "customer_idx"
            }
          ]
        }"""
            .replaceAll("\\s+", "");
    assertThatThrownBy(() -> deserialize(jsonWithKeysSpelledIncorrectly))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid identifier list: null");

    String jsonWithInvalidIdentifiersInList =
        """
        {
          "identifiers": [
            {
              "namespace": "accounting.tax",
              "table": "paid",
              "name": "customer_idx"
            }
          ]
        }"""
            .replaceAll("\\s+", "");
    assertThatThrownBy(() -> deserialize(jsonWithInvalidIdentifiersInList))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageContaining("Cannot parse JSON array from non-array value");

    String jsonWithInvalidIdentifiersInList2 =
        """
        {
          "identifiers": [
            {
              "namespace": ["accounting", "tax"],
              "table": "paid",
              "name": "customer_idx"
            },
            "accounting.tax.paid.customer_idx"
          ]
        }"""
            .replaceAll("\\s+", "");
    assertThatThrownBy(() -> deserialize(jsonWithInvalidIdentifiersInList2))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageContaining("Cannot parse missing or non-object index identifier");

    String jsonWithInvalidTypeForNamePartOfIdentifier =
        """
        {
          "identifiers": [
            {
              "namespace": ["accounting", "tax"],
              "table": "paid",
              "name": true
            }
          ]
        }"""
            .replaceAll("\\s+", "");
    assertThatThrownBy(() -> deserialize(jsonWithInvalidTypeForNamePartOfIdentifier))
        .isInstanceOf(JsonProcessingException.class)
        .hasMessageContaining("Cannot parse to a string value");

    assertThatThrownBy(() -> deserialize(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("argument \"content\" is null");
  }

  @Test
  public void testBuilderDoesNotCreateInvalidObjects() {
    assertThatThrownBy(() -> ListIndexesResponse.builder().add(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid index identifier: null");

    assertThatThrownBy(() -> ListIndexesResponse.builder().addAll(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid index identifier list: null");

    List<IndexIdentifier> listWithNullElement =
        Lists.newArrayList(IndexIdentifier.of(Namespace.of("foo"), "bar", "idx"), null);
    assertThatThrownBy(() -> ListIndexesResponse.builder().addAll(listWithNullElement))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid index identifier: null");
  }

  @Test
  public void testWithNullPaginationToken() throws JsonProcessingException {
    ListIndexesResponse response =
        ListIndexesResponse.builder().addAll(IDENTIFIERS).nextPageToken(null).build();
    assertRoundTripSerializesEquallyFrom(FULL_JSON, response);
    assertThat(response.nextPageToken()).isNull();
    assertThat(response.identifiers()).isEqualTo(IDENTIFIERS);
  }

  @Test
  public void testWithPaginationToken() throws JsonProcessingException {
    String pageToken = "token";
    String jsonWithPageToken =
        """
        {
          "identifiers": [
            {
              "namespace": ["accounting", "tax"],
              "table": "paid",
              "name": "customer_idx"
            }
          ],
          "next-page-token": "token"
        }
        """
            .replaceAll("\\s+", "");
    ListIndexesResponse response =
        ListIndexesResponse.builder().addAll(IDENTIFIERS).nextPageToken(pageToken).build();
    assertRoundTripSerializesEquallyFrom(jsonWithPageToken, response);
    assertThat(response.nextPageToken()).isEqualTo("token");
    assertThat(response.identifiers()).isEqualTo(IDENTIFIERS);
  }

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"identifiers", "next-page-token"};
  }

  @Override
  public ListIndexesResponse createExampleInstance() {
    return ListIndexesResponse.builder().addAll(IDENTIFIERS).build();
  }

  @Override
  public void assertEquals(ListIndexesResponse actual, ListIndexesResponse expected) {
    assertThat(actual.identifiers())
        .as("Identifiers should be equal")
        .hasSameSizeAs(expected.identifiers())
        .containsExactlyInAnyOrderElementsOf(expected.identifiers());
  }

  @Override
  public ListIndexesResponse deserialize(String json) throws JsonProcessingException {
    ListIndexesResponse resp = mapper().readValue(json, ListIndexesResponse.class);
    resp.validate();
    return resp;
  }
}
