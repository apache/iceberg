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
package org.apache.iceberg.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

class TestCatalogObjectIdentifierParser {

  @Test
  void testToJson() {
    CatalogObjectIdentifier id = CatalogObjectIdentifier.of(Namespace.of("db", "schema"), "tbl");
    String expected = "{\"namespace\":[\"db\",\"schema\"],\"name\":\"tbl\"}";
    assertThat(CatalogObjectIdentifierParser.toJson(id)).isEqualTo(expected);
  }

  @Test
  void testToJsonWithEmptyNamespace() {
    CatalogObjectIdentifier id = CatalogObjectIdentifier.of(Namespace.empty(), "tbl");
    String expected = "{\"namespace\":[],\"name\":\"tbl\"}";
    assertThat(CatalogObjectIdentifierParser.toJson(id)).isEqualTo(expected);
  }

  @Test
  void testToJsonPretty() {
    CatalogObjectIdentifier id = CatalogObjectIdentifier.of(Namespace.of("db"), "tbl");
    String expected = "{\n" + "  \"namespace\" : [ \"db\" ],\n" + "  \"name\" : \"tbl\"\n" + "}";
    assertThat(CatalogObjectIdentifierParser.toJson(id, true)).isEqualTo(expected);
  }

  @Test
  void testToJsonWithNull() {
    assertThatThrownBy(() -> CatalogObjectIdentifierParser.toJson(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid catalog object identifier: null");
  }

  @Test
  void testFromJson() {
    String json = "{\"namespace\":[\"db\",\"schema\"],\"name\":\"tbl\"}";
    CatalogObjectIdentifier expected =
        CatalogObjectIdentifier.of(Namespace.of("db", "schema"), "tbl");
    assertThat(CatalogObjectIdentifierParser.fromJson(json)).isEqualTo(expected);
  }

  @Test
  void testFromJsonWithEmptyNamespace() {
    String json = "{\"namespace\":[],\"name\":\"tbl\"}";
    CatalogObjectIdentifier expected = CatalogObjectIdentifier.of(Namespace.empty(), "tbl");
    assertThat(CatalogObjectIdentifierParser.fromJson(json)).isEqualTo(expected);
  }

  @Test
  void testFromJsonWithMissingNamespace() {
    String json = "{\"name\":\"tbl\"}";
    CatalogObjectIdentifier expected = CatalogObjectIdentifier.of(Namespace.empty(), "tbl");
    assertThat(CatalogObjectIdentifierParser.fromJson(json)).isEqualTo(expected);
  }

  @Test
  void testFromJsonWithNullString() {
    assertThatThrownBy(() -> CatalogObjectIdentifierParser.fromJson((String) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse catalog object identifier from invalid JSON: null");
  }

  @Test
  void testFromJsonWithEmptyString() {
    assertThatThrownBy(() -> CatalogObjectIdentifierParser.fromJson(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse catalog object identifier from invalid JSON: ''");
  }

  @Test
  void testFromJsonWithNullNode() {
    assertThatThrownBy(() -> CatalogObjectIdentifierParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing or non-object catalog object identifier: null");
  }

  @Test
  void testFromJsonWithMissingName() {
    assertThatThrownBy(() -> CatalogObjectIdentifierParser.fromJson("{\"namespace\":[\"db\"]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: name");
  }

  @Test
  void testRoundTrip() {
    String json = "{\"namespace\":[\"db\",\"schema\"],\"name\":\"tbl\"}";
    assertThat(CatalogObjectIdentifierParser.toJson(CatalogObjectIdentifierParser.fromJson(json)))
        .isEqualTo(json);

    String jsonEmptyNs = "{\"namespace\":[],\"name\":\"tbl\"}";
    assertThat(
            CatalogObjectIdentifierParser.toJson(
                CatalogObjectIdentifierParser.fromJson(jsonEmptyNs)))
        .isEqualTo(jsonEmptyNs);
  }
}
