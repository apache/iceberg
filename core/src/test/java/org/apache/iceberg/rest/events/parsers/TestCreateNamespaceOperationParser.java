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
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.events.CatalogOperationParser;
import org.apache.iceberg.rest.events.operations.CatalogOperation;
import org.junit.jupiter.api.Test;

public class TestCreateNamespaceOperationParser {
  @Test
  void testToJson() {
    CatalogOperation.CreateNamespace createNamespaceOperation =
        new CatalogOperation.CreateNamespace(Namespace.of("a", "b"), Map.of());
    String createNamespaceOperationJson =
        "{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}";

    assertThat(CatalogOperationParser.toJson(createNamespaceOperation))
        .isEqualTo(createNamespaceOperationJson);
  }

  @Test
  void testToJsonPretty() {
    CatalogOperation.CreateNamespace createNamespaceOperation =
        new CatalogOperation.CreateNamespace(Namespace.of("a", "b"), Map.of());
    String createNamespaceOperationJson =
        "{\n"
            + "  \"operation-type\" : \"create-namespace\",\n"
            + "  \"namespace\" : [ \"a\", \"b\" ]\n"
            + "}";

    assertThat(CatalogOperationParser.toJson(createNamespaceOperation, true))
        .isEqualTo(createNamespaceOperationJson);
  }

  @Test
  void testToJsonWithNullOperation() {
    assertThatNullPointerException()
        .isThrownBy(() -> CatalogOperationParser.toJson(null))
        .withMessage("Invalid operation: null");
  }

  @Test
  void testToJsonWithOptionalProperties() {
    CatalogOperation.CreateNamespace createNamespaceOperation =
        new CatalogOperation.CreateNamespace(Namespace.of("a", "b"), Map.of("key", "value"));
    String createNamespaceOperationJson =
        "{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"],\"properties\":{\"key\":\"value\"}}";

    assertThat(CatalogOperationParser.toJson(createNamespaceOperation))
        .isEqualTo(createNamespaceOperationJson);
  }

  @Test
  void testFromJson() {
    String createNamespaceOperationJson =
        "{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}";
    CatalogOperation.CreateNamespace expectedOperation =
        new CatalogOperation.CreateNamespace(Namespace.of("a", "b"), Map.of());

    assertThat(CatalogOperationParser.fromJson(createNamespaceOperationJson))
        .isEqualTo(expectedOperation);
  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatNullPointerException()
        .isThrownBy(() -> CatalogOperationParser.fromJson((JsonNode) null))
        .withMessage("Cannot parse catalog operation from null object");
  }

  @Test
  void testFromJsonWithMissingProperties() {
    String missingNamespace = "{\"operation-type\":\"create-namespace\"}";

    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(missingNamespace));
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    // namespace present but not an array
    String invalidNamespace = "{\"operation-type\":\"create-namespace\",\"namespace\":\"a\"}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(invalidNamespace));

    // properties present but not an object
    String invalidProperties =
        "{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\"],\"properties\":\"not-an-object\"}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(invalidProperties));
  }

  @Test
  void testFromJsonWithOptionalProperties() {
    String createNamespaceOperationJson =
        "{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"],\"properties\":{\"key\":\"value\"}}";
    CatalogOperation.CreateNamespace expectedOperation =
        new CatalogOperation.CreateNamespace(Namespace.of("a", "b"), Map.of("key", "value"));

    assertThat(CatalogOperationParser.fromJson(createNamespaceOperationJson))
        .isEqualTo(expectedOperation);
  }

  @Test
  void testRoundTrip() {
    String json = "{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(json)))
        .isEqualTo(json);

    String jsonWithProps =
        "{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"],\"properties\":{\"key\":\"value\"}}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(jsonWithProps)))
        .isEqualTo(jsonWithProps);
  }
}
