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
package org.apache.iceberg.rest.events.parsers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.events.CatalogOperationParser;
import org.apache.iceberg.rest.events.operations.CatalogOperation;
import org.junit.jupiter.api.Test;

public class TestUpdateNamespacePropertiesOperationParser {
  @Test
  void testToJson() {
    CatalogOperation.UpdateNamespaceProperties op =
        new CatalogOperation.UpdateNamespaceProperties(
            Namespace.of("a"), List.of("k2"), List.of("k1"), null);
    String expected =
        "{\"operation-type\":\"update-namespace-properties\",\"namespace\":[\"a\"],\"updated\":[\"k1\"],\"removed\":[\"k2\"]}";
    assertThat(CatalogOperationParser.toJson(op)).isEqualTo(expected);
  }

  @Test
  void testToJsonPretty() {
    CatalogOperation.UpdateNamespaceProperties op =
        new CatalogOperation.UpdateNamespaceProperties(
            Namespace.of("a"), List.of("k2"), List.of("k1"), null);
    String expected =
        "{\n"
            + "  \"operation-type\" : \"update-namespace-properties\",\n"
            + "  \"namespace\" : [ \"a\" ],\n"
            + "  \"updated\" : [ \"k1\" ],\n"
            + "  \"removed\" : [ \"k2\" ]\n"
            + "}";
    assertThat(CatalogOperationParser.toJson(op, true)).isEqualTo(expected);
  }

  @Test
  void testToJsonWithNullOperation() {
    assertThatNullPointerException()
        .isThrownBy(() -> CatalogOperationParser.toJson(null))
        .withMessage("Invalid operation: null");
  }

  @Test
  void testToJsonWithOptionalProperties() {
    CatalogOperation.UpdateNamespaceProperties op =
        new CatalogOperation.UpdateNamespaceProperties(
            Namespace.of("a"), List.of("k2"), List.of("k1"), List.of("k3"));
    String expected =
        "{\"operation-type\":\"update-namespace-properties\",\"namespace\":[\"a\"],\"updated\":[\"k1\"],\"removed\":[\"k2\"],\"missing\":[\"k3\"]}";
    assertThat(CatalogOperationParser.toJson(op)).isEqualTo(expected);
  }

  @Test
  void testFromJson() {
    String json =
        "{\"operation-type\":\"update-namespace-properties\",\"namespace\":[\"a\"],\"updated\":[\"k1\"],\"removed\":[\"k2\"]}";
    CatalogOperation.UpdateNamespaceProperties expected =
        new CatalogOperation.UpdateNamespaceProperties(
            Namespace.of("a"), List.of("k2"), List.of("k1"), null);
    assertThat(CatalogOperationParser.fromJson(json)).isEqualTo(expected);
  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatNullPointerException()
        .isThrownBy(() -> CatalogOperationParser.fromJson((JsonNode) null))
        .withMessage("Cannot parse catalog operation from null object");
  }

  @Test
  void testFromJsonWithMissingProperties() {
    String missingNamespace =
        "{\"operation-type\":\"update-namespace-properties\",\"updated\":[\"k1\"],\"removed\":[\"k2\"]}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(missingNamespace));

    String missingUpdated =
        "{\"operation-type\":\"update-namespace-properties\",\"namespace\":[\"a\"],\"removed\":[\"k2\"]}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(missingUpdated));

    String missingRemoved =
        "{\"operation-type\":\"update-namespace-properties\",\"namespace\":[\"a\"],\"updated\":[\"k1\"]}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(missingRemoved));
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    String invalidNamespace =
        "{\"operation-type\":\"update-namespace-properties\",\"namespace\":\"a\",\"updated\":[\"k1\"],\"removed\":[\"k2\"]}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(invalidNamespace));

    String invalidUpdated =
        "{\"operation-type\":\"update-namespace-properties\",\"namespace\":[\"a\"],\"updated\":\"not-array\",\"removed\":[\"k2\"]}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(invalidUpdated));

    String invalidRemoved =
        "{\"operation-type\":\"update-namespace-properties\",\"namespace\":[\"a\"],\"updated\":[\"k1\"],\"removed\":123}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(invalidRemoved));

    String invalidMissing =
        "{\"operation-type\":\"update-namespace-properties\",\"namespace\":[\"a\"],\"updated\":[\"k1\"],\"removed\":[\"k2\"],\"missing\":123}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(invalidMissing));
  }
}
