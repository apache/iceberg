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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.events.operations.CreateNamespaceOperation;
import org.apache.iceberg.rest.events.operations.ImmutableCreateNamespaceOperation;
import org.junit.jupiter.api.Test;

public class TestCreateNamespaceOperationParser {
  @Test
  void testToJson() {
    CreateNamespaceOperation createNamespaceOperation =
        ImmutableCreateNamespaceOperation.builder().namespace(Namespace.of("a", "b")).build();
    String createNamespaceOperationJson =
        "{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}";

    assertThat(CreateNamespaceOperationParser.toJson(createNamespaceOperation))
        .isEqualTo(createNamespaceOperationJson);
  }

  @Test
  void testToJsonPretty() {
    CreateNamespaceOperation createNamespaceOperation =
        ImmutableCreateNamespaceOperation.builder().namespace(Namespace.of("a", "b")).build();
    String createNamespaceOperationJson =
        "{\n"
            + "  \"operation-type\" : \"create-namespace\",\n"
            + "  \"namespace\" : [ \"a\", \"b\" ]\n"
            + "}";

    assertThat(CreateNamespaceOperationParser.toJsonPretty(createNamespaceOperation))
        .isEqualTo(createNamespaceOperationJson);
  }

  @Test
  void testToJsonWithNullOperation() {
    assertThatThrownBy(() -> CreateNamespaceOperationParser.toJson(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid create namespace operation: null");
  }

  @Test
  void testToJsonWithOptionalProperties() {
    CreateNamespaceOperation createNamespaceOperation =
        ImmutableCreateNamespaceOperation.builder()
            .namespace(Namespace.of("a", "b"))
            .putProperties("key", "value")
            .build();
    String createNamespaceOperationJson =
        "{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"],\"properties\":{\"key\":\"value\"}}";

    assertThat(CreateNamespaceOperationParser.toJson(createNamespaceOperation))
        .isEqualTo(createNamespaceOperationJson);
  }

  @Test
  void testFromJson() {
    String createNamespaceOperationJson =
        "{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"]}";
    CreateNamespaceOperation expectedOperation =
        ImmutableCreateNamespaceOperation.builder().namespace(Namespace.of("a", "b")).build();

    assertThat(CreateNamespaceOperationParser.fromJson(createNamespaceOperationJson))
        .isEqualTo(expectedOperation);
  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatThrownBy(() -> CreateNamespaceOperationParser.fromJson((JsonNode) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot parse create namespace operation from null object");
  }

  @Test
  void testFromJsonWithMissingProperties() {
    String missingNamespace = "{\"operation-type\":\"create-namespace\"}";

    assertThatThrownBy(() -> CreateNamespaceOperationParser.fromJson(missingNamespace))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    // namespace present but not an array
    String invalidNamespace = "{\"operation-type\":\"create-namespace\",\"namespace\":\"a\"}";
    assertThatThrownBy(() -> CreateNamespaceOperationParser.fromJson(invalidNamespace))
        .isInstanceOf(IllegalArgumentException.class);

    // properties present but not an object
    String invalidProperties =
        "{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\"],\"properties\":\"not-an-object\"}";
    assertThatThrownBy(() -> CreateNamespaceOperationParser.fromJson(invalidProperties))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testFromJsonWithOptionalProperties() {
    String createNamespaceOperationJson =
        "{\"operation-type\":\"create-namespace\",\"namespace\":[\"a\",\"b\"],\"properties\":{\"key\":\"value\"}}";
    CreateNamespaceOperation expectedOperation =
        ImmutableCreateNamespaceOperation.builder()
            .namespace(Namespace.of("a", "b"))
            .putProperties("key", "value")
            .build();

    assertThat(CreateNamespaceOperationParser.fromJson(createNamespaceOperationJson))
        .isEqualTo(expectedOperation);
  }
}
