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

import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.rest.events.operations.ImmutableRegisterTableOperation;
import org.apache.iceberg.rest.events.operations.RegisterTableOperation;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRegisterTableOperationParser {
  @Test
  void testToJson() {
    RegisterTableOperation op = ImmutableRegisterTableOperation.builder()
        .identifier(TableIdentifier.of(Namespace.empty(), "table"))
        .tableUuid("uuid")
        .build();
    String json = "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\"}";
    assertThat(RegisterTableOperationParser.toJson(op)).isEqualTo(json);
  }

  @Test
  void testToJsonPretty() {
    RegisterTableOperation op = ImmutableRegisterTableOperation.builder()
        .identifier(TableIdentifier.of(Namespace.empty(), "table"))
        .tableUuid("uuid")
        .build();
    String json = "{\n" +
        "  \"operation-type\" : \"register-table\",\n" +
        "  \"identifier\" : {\n" +
        "    \"namespace\" : [ ],\n" +
        "    \"name\" : \"table\"\n" +
        "  },\n" +
        "  \"table-uuid\" : \"uuid\"\n" +
        "}";
    assertThat(RegisterTableOperationParser.toJsonPretty(op)).isEqualTo(json);
  }

  @Test
  void testToJsonWithNullOperation() {
    assertThatThrownBy(() -> RegisterTableOperationParser.toJson(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid register table operation: null");
  }

  @Test
  void testToJsonWithOptionalProperties() {
    RegisterTableOperation op = ImmutableRegisterTableOperation.builder()
        .identifier(TableIdentifier.of(Namespace.empty(), "table"))
        .tableUuid("uuid")
        .addUpdates(new MetadataUpdate.AssignUUID("uuid"))
        .build();
    String json = "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    assertThat(RegisterTableOperationParser.toJson(op)).isEqualTo(json);
  }

  @Test
  void testFromJson() {
    String json = "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\"}";
    RegisterTableOperation expected = ImmutableRegisterTableOperation.builder()
        .identifier(TableIdentifier.of(Namespace.empty(), "table"))
        .tableUuid("uuid")
        .build();
    assertThat(RegisterTableOperationParser.fromJson(json)).isEqualTo(expected);
  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatThrownBy(() -> RegisterTableOperationParser.fromJson((JsonNode) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot parse register table operation from null object");
  }

  @Test
  void testFromJsonWithMissingProperties() {
    String missingIdentifier = "{\"operation-type\":\"register-table\",\"table-uuid\":\"uuid\"}";
    assertThatThrownBy(() -> RegisterTableOperationParser.fromJson(missingIdentifier))
        .isInstanceOf(IllegalArgumentException.class);

    String missingUuid = "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"}}";
    assertThatThrownBy(() -> RegisterTableOperationParser.fromJson(missingUuid))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    // identifier present but not an object
    String invalidIdentifier = "{\"operation-type\":\"register-table\",\"identifier\":\"not-an-object\",\"table-uuid\":\"uuid\"}";
    assertThatThrownBy(() -> RegisterTableOperationParser.fromJson(invalidIdentifier))
        .isInstanceOf(IllegalArgumentException.class);

    // table-uuid present but not a string
    String invalidUuid = "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":123}";
    assertThatThrownBy(() -> RegisterTableOperationParser.fromJson(invalidUuid))
        .isInstanceOf(IllegalArgumentException.class);

    // updates present but not an array
    String invalidUpdates = "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":\"not-an-array\"}";
    assertThatThrownBy(() -> RegisterTableOperationParser.fromJson(invalidUpdates))
        .isInstanceOf(IllegalArgumentException.class);
  }
  
  @Test
  void testFromJsonWithOptionalProperties() {
    String json = "{\"operation-type\":\"register-table\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    RegisterTableOperation expected = ImmutableRegisterTableOperation.builder()
        .identifier(TableIdentifier.of(Namespace.of("a"), "t"))
        .tableUuid("uuid")
        .addUpdates(new MetadataUpdate.AssignUUID("uuid"))
        .build();

    RegisterTableOperation actual = RegisterTableOperationParser.fromJson(json);
    assertThat(actual.operationType()).isEqualTo(expected.operationType());
    assertThat(actual.identifier()).isEqualTo(expected.identifier());
    assertThat(actual.tableUuid()).isEqualTo(expected.tableUuid());
    assertThat(actual.updates()).hasSize(1);
    assertThat(((MetadataUpdate.AssignUUID) actual.updates().get(0)).uuid())
        .isEqualTo(((MetadataUpdate.AssignUUID) expected.updates().get(0)).uuid());
  }
}
