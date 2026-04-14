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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.events.CatalogOperationParser;
import org.apache.iceberg.rest.events.operations.CatalogOperation;
import org.apache.iceberg.rest.events.operations.OperationType.CustomOperationType;
import org.junit.jupiter.api.Test;

public class TestCustomOperationParser {
  @Test
  void testToJson() {
    CatalogOperation.Custom op =
        new CatalogOperation.Custom(
            new CustomOperationType("x-op"), null, null, null, null, ImmutableMap.of());
    String json = "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\"}";
    assertThat(CatalogOperationParser.toJson(op)).isEqualTo(json);
  }

  @Test
  void testToJsonPretty() {
    CatalogOperation.Custom op =
        new CatalogOperation.Custom(
            new CustomOperationType("x-op"), null, null, null, null, ImmutableMap.of());
    String json =
        "{\n" + "  \"operation-type\" : \"custom\",\n" + "  \"custom-type\" : \"x-op\"\n" + "}";
    assertThat(CatalogOperationParser.toJson(op, true)).isEqualTo(json);
  }

  @Test
  void testToJsonWithNullOperation() {
    assertThatNullPointerException()
        .isThrownBy(() -> CatalogOperationParser.toJson(null))
        .withMessage("Invalid operation: null");
  }

  @Test
  void testToJsonWithOptionalProperties() {
    CatalogOperation.Custom op =
        new CatalogOperation.Custom(
            new CustomOperationType("x-op"),
            TableIdentifier.of(Namespace.of("a"), "t"),
            Namespace.of("a", "b"),
            "t-uuid",
            "v-uuid",
            ImmutableMap.of());
    String json =
        "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"},\"namespace\":[\"a\",\"b\"],\"table-uuid\":\"t-uuid\",\"view-uuid\":\"v-uuid\"}";
    assertThat(CatalogOperationParser.toJson(op)).isEqualTo(json);
  }

  @Test
  void testFromJson() {
    String json = "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\"}";
    CatalogOperation.Custom expected =
        new CatalogOperation.Custom(
            new CustomOperationType("x-op"), null, null, null, null, ImmutableMap.of());
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
    String json = "{\"operation-type\":\"custom\"}";
    assertThatIllegalArgumentException().isThrownBy(() -> CatalogOperationParser.fromJson(json));
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    // custom-type present but not a string matching pattern
    String jsonInvalidCustomType = "{\"operation-type\":\"custom\",\"custom-type\":123}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(jsonInvalidCustomType));

    // identifier present but not an object
    String jsonInvalidIdentifier =
        "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\",\"identifier\":{}}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(jsonInvalidIdentifier));

    // namespace present but not an array
    String jsonInvalidNamespace =
        "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\",\"namespace\":\"a\"}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(jsonInvalidNamespace));

    // table-uuid present but not a string
    String jsonInvalidTableUuid =
        "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\",\"table-uuid\":123}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(jsonInvalidTableUuid));

    // view-uuid present but not a string
    String jsonInvalidViewUuid =
        "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\",\"view-uuid\":true}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(jsonInvalidViewUuid));
  }

  @Test
  void testFromJsonWithOptionalProperties() {
    String json =
        "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"},\"namespace\":[\"a\",\"b\"],\"table-uuid\":\"t-uuid\",\"view-uuid\":\"v-uuid\"}";
    CatalogOperation.Custom expected =
        new CatalogOperation.Custom(
            new CustomOperationType("x-op"),
            TableIdentifier.of(Namespace.of("a"), "t"),
            Namespace.of("a", "b"),
            "t-uuid",
            "v-uuid",
            ImmutableMap.of());
    assertThat(CatalogOperationParser.fromJson(json)).isEqualTo(expected);
  }

  @Test
  void testRoundTrip() {
    String json = "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\"}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(json)))
        .isEqualTo(json);

    String jsonWithAll =
        "{\"operation-type\":\"custom\",\"custom-type\":\"x-op\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"},\"namespace\":[\"a\",\"b\"],\"table-uuid\":\"t-uuid\",\"view-uuid\":\"v-uuid\"}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(jsonWithAll)))
        .isEqualTo(jsonWithAll);
  }
}
