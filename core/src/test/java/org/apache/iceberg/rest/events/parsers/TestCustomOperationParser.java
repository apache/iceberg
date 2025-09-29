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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.events.operations.CustomOperation;
import org.apache.iceberg.rest.events.operations.ImmutableCustomOperation;
import org.apache.iceberg.rest.events.operations.OperationType.CustomOperationType;
import org.junit.jupiter.api.Test;

public class TestCustomOperationParser {
  @Test
  void testToJson() {
    CustomOperation op =
        ImmutableCustomOperation.builder()
            .customOperationType(new CustomOperationType("x-op"))
            .build();
    String json = "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\"}";
    assertThat(CustomOperationParser.toJson(op)).isEqualTo(json);
  }

  @Test
  void testToJsonPretty() {
    CustomOperation op =
        ImmutableCustomOperation.builder()
            .customOperationType(new CustomOperationType("x-op"))
            .build();
    String json =
        "{\n" + "  \"operation-type\" : \"custom\",\n" + "  \"custom-type\" : \"x-op\"\n" + "}";
    assertThat(CustomOperationParser.toJsonPretty(op)).isEqualTo(json);
  }

  @Test
  void testToJsonWithNullOperation() {
    assertThatThrownBy(() -> CustomOperationParser.toJson(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid custom operation: null");
  }

  @Test
  void testToJsonWithOptionalProperties() {
    CustomOperation op =
        ImmutableCustomOperation.builder()
            .customOperationType(new CustomOperationType("x-op"))
            .identifier(TableIdentifier.of(Namespace.of("a"), "t"))
            .namespace(Namespace.of("a", "b"))
            .tableUuid("t-uuid")
            .viewUuid("v-uuid")
            .build();
    String json =
        "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"},\"namespace\":[\"a\",\"b\"],\"table-uuid\":\"t-uuid\",\"view-uuid\":\"v-uuid\"}";
    assertThat(CustomOperationParser.toJson(op)).isEqualTo(json);
  }

  @Test
  void testFromJson() {
    String json = "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\"}";
    CustomOperation expected =
        ImmutableCustomOperation.builder()
            .customOperationType(new CustomOperationType("x-op"))
            .build();
    assertThat(CustomOperationParser.fromJson(json)).isEqualTo(expected);
  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatThrownBy(() -> CustomOperationParser.fromJson((JsonNode) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot parse custom operation from null object");
  }

  @Test
  void testFromJsonWithMissingProperties() {
    String json = "{\"operation-type\":\"custom\"}";
    assertThatThrownBy(() -> CustomOperationParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    // custom-type present but not a string matching pattern
    String jsonInvalidCustomType = "{\"operation-type\":\"custom\",\"custom-type\":123}";
    assertThatThrownBy(() -> CustomOperationParser.fromJson(jsonInvalidCustomType))
        .isInstanceOf(IllegalArgumentException.class);

    // identifier present but not an object
    String jsonInvalidIdentifier =
        "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\",\"identifier\":{}}";
    assertThatThrownBy(() -> CustomOperationParser.fromJson(jsonInvalidIdentifier))
        .isInstanceOf(IllegalArgumentException.class);

    // namespace present but not an array
    String jsonInvalidNamespace =
        "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\",\"namespace\":\"a\"}";
    assertThatThrownBy(() -> CustomOperationParser.fromJson(jsonInvalidNamespace))
        .isInstanceOf(IllegalArgumentException.class);

    // table-uuid present but not a string
    String jsonInvalidTableUuid =
        "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\",\"table-uuid\":123}";
    assertThatThrownBy(() -> CustomOperationParser.fromJson(jsonInvalidTableUuid))
        .isInstanceOf(IllegalArgumentException.class);

    // view-uuid present but not a string
    String jsonInvalidViewUuid =
        "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\",\"view-uuid\":true}";
    assertThatThrownBy(() -> CustomOperationParser.fromJson(jsonInvalidViewUuid))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testFromJsonWithOptionalProperties() {
    String json =
        "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"},\"namespace\":[\"a\",\"b\"],\"table-uuid\":\"t-uuid\",\"view-uuid\":\"v-uuid\"}";
    CustomOperation expected =
        ImmutableCustomOperation.builder()
            .customOperationType(new CustomOperationType("x-op"))
            .identifier(TableIdentifier.of(Namespace.of("a"), "t"))
            .namespace(Namespace.of("a", "b"))
            .tableUuid("t-uuid")
            .viewUuid("v-uuid")
            .build();
    assertThat(CustomOperationParser.fromJson(json)).isEqualTo(expected);
  }
}
