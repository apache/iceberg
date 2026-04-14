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
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.events.CatalogOperationParser;
import org.apache.iceberg.rest.events.operations.CatalogOperation;
import org.junit.jupiter.api.Test;

public class TestRegisterTableOperationParser {
  @Test
  void testToJson() {
    CatalogOperation.RegisterTable op =
        new CatalogOperation.RegisterTable(
            TableIdentifier.of(Namespace.empty(), "table"), "uuid", List.of());
    String json =
        "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\"}";
    assertThat(CatalogOperationParser.toJson(op)).isEqualTo(json);
  }

  @Test
  void testToJsonPretty() {
    CatalogOperation.RegisterTable op =
        new CatalogOperation.RegisterTable(
            TableIdentifier.of(Namespace.empty(), "table"), "uuid", List.of());
    String json =
        "{\n"
            + "  \"operation-type\" : \"register-table\",\n"
            + "  \"identifier\" : {\n"
            + "    \"namespace\" : [ ],\n"
            + "    \"name\" : \"table\"\n"
            + "  },\n"
            + "  \"table-uuid\" : \"uuid\"\n"
            + "}";
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
    CatalogOperation.RegisterTable op =
        new CatalogOperation.RegisterTable(
            TableIdentifier.of(Namespace.empty(), "table"),
            "uuid",
            List.of(new MetadataUpdate.AssignUUID("uuid")));
    String json =
        "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    assertThat(CatalogOperationParser.toJson(op)).isEqualTo(json);
  }

  @Test
  void testFromJson() {
    String json =
        "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\"}";
    CatalogOperation.RegisterTable expected =
        new CatalogOperation.RegisterTable(
            TableIdentifier.of(Namespace.empty(), "table"), "uuid", List.of());
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
    String missingIdentifier = "{\"operation-type\":\"register-table\",\"table-uuid\":\"uuid\"}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(missingIdentifier));

    String missingUuid =
        "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"}}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(missingUuid));
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    // identifier present but not an object
    String invalidIdentifier =
        "{\"operation-type\":\"register-table\",\"identifier\":\"not-an-object\",\"table-uuid\":\"uuid\"}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(invalidIdentifier));

    // table-uuid present but not a string
    String invalidUuid =
        "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":123}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(invalidUuid));

    // updates present but not an array
    String invalidUpdates =
        "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":\"not-an-array\"}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(invalidUpdates));
  }

  @Test
  void testFromJsonWithOptionalProperties() {
    String json =
        "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    CatalogOperation.RegisterTable expected =
        new CatalogOperation.RegisterTable(
            TableIdentifier.of(Namespace.of("a"), "t"),
            "uuid",
            List.of(new MetadataUpdate.AssignUUID("uuid")));

    CatalogOperation.RegisterTable actual =
        (CatalogOperation.RegisterTable) CatalogOperationParser.fromJson(json);
    assertThat(actual.operationType()).isEqualTo(expected.operationType());
    assertThat(actual.identifier()).isEqualTo(expected.identifier());
    assertThat(actual.tableUuid()).isEqualTo(expected.tableUuid());
    assertThat(actual.updates()).hasSize(1);
    assertThat(((MetadataUpdate.AssignUUID) actual.updates().get(0)).uuid())
        .isEqualTo(((MetadataUpdate.AssignUUID) expected.updates().get(0)).uuid());
  }

  @Test
  void testRoundTrip() {
    String json =
        "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\"}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(json)))
        .isEqualTo(json);

    String jsonWithUpdates =
        "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(jsonWithUpdates)))
        .isEqualTo(jsonWithUpdates);
  }
}
