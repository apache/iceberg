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
import org.apache.iceberg.rest.events.operations.CreateTableOperation;
import org.apache.iceberg.rest.events.operations.ImmutableCreateTableOperation;
import org.junit.jupiter.api.Test;

public class TestCreateTableOperationParser {
  @Test
  void testToJson() {
    CreateTableOperation createTableOperation =
        ImmutableCreateTableOperation.builder()
            .identifier(TableIdentifier.of(Namespace.empty(), "table"))
            .tableUuid("uuid")
            .updates(List.of(new MetadataUpdate.AssignUUID("uuid")))
            .build();
    String createTableOperationJson =
        "{\"operation-type\":\"create-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";

    assertThat(CreateTableOperationParser.toJson(createTableOperation))
        .isEqualTo(createTableOperationJson);
  }

  @Test
  void testToJsonPretty() {
    CreateTableOperation createTableOperation =
        ImmutableCreateTableOperation.builder()
            .identifier(TableIdentifier.of(Namespace.empty(), "table"))
            .tableUuid("uuid")
            .updates(List.of(new MetadataUpdate.AssignUUID("uuid")))
            .build();
    String createTableOperationJson =
        "{\n"
            + "  \"operation-type\" : \"create-table\",\n"
            + "  \"identifier\" : {\n"
            + "    \"namespace\" : [ ],\n"
            + "    \"name\" : \"table\"\n"
            + "  },\n"
            + "  \"table-uuid\" : \"uuid\",\n"
            + "  \"updates\" : [ {\n"
            + "    \"action\" : \"assign-uuid\",\n"
            + "    \"uuid\" : \"uuid\"\n"
            + "  } ]\n"
            + "}";

    assertThat(CreateTableOperationParser.toJsonPretty(createTableOperation))
        .isEqualTo(createTableOperationJson);
  }

  @Test
  void testToJsonWithNullOperation() {
    assertThatNullPointerException()
        .isThrownBy(() -> CreateTableOperationParser.toJson(null))
        .withMessage("Invalid create table operation: null");
  }

  @Test
  void testFromJson() {
    CreateTableOperation createTableOperation =
        ImmutableCreateTableOperation.builder()
            .identifier(TableIdentifier.of(Namespace.empty(), "table"))
            .tableUuid("uuid")
            .updates(List.of(new MetadataUpdate.AssignUUID("uuid")))
            .build();
    String createTableOperationJson =
        "{\"operation-type\":\"create-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";

    CreateTableOperation actualCreateTableOperation =
        CreateTableOperationParser.fromJson(createTableOperationJson);
    assertThat(actualCreateTableOperation.operationType())
        .isEqualTo(createTableOperation.operationType());
    assertThat(actualCreateTableOperation.identifier())
        .isEqualTo(createTableOperation.identifier());
    assertThat(actualCreateTableOperation.tableUuid()).isEqualTo(createTableOperation.tableUuid());

    assertThat(actualCreateTableOperation.updates()).hasSize(1);
    assertThat(actualCreateTableOperation.updates().get(0))
        .isInstanceOf(MetadataUpdate.AssignUUID.class);
    assertThat(((MetadataUpdate.AssignUUID) actualCreateTableOperation.updates().get(0)).uuid())
        .isEqualTo(
            ((MetadataUpdate.AssignUUID) actualCreateTableOperation.updates().get(0)).uuid());
  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatNullPointerException()
        .isThrownBy(() -> CreateTableOperationParser.fromJson((JsonNode) null))
        .withMessage("Cannot parse create table operation from null object");
  }

  @Test
  void testFromJsonWithMissingProperties() {
    // missing identifier
    String missingIdentifier =
        "{\"operation-type\":\"create-table\",\"table-uuid\":\"uuid\",\"updates\":[]}";
    assertThatIllegalArgumentException()
        .isThrownBy((() -> CreateTableOperationParser.fromJson(missingIdentifier)));

    // missing table-uuid
    String missingUuid =
        "{\"operation-type\":\"create-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"updates\":[]}";
    assertThatIllegalArgumentException()
        .isThrownBy((() -> CreateTableOperationParser.fromJson(missingUuid)));

    // missing updates
    String missingUpdates =
        "{\"operation-type\":\"create-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\"}";
    assertThatIllegalArgumentException()
        .isThrownBy((() -> CreateTableOperationParser.fromJson(missingUpdates)));
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    // identifier present but not an object
    String invalidIdentifier =
        "{\"operation-type\":\"create-table\",\"identifier\":\"not-an-object\",\"table-uuid\":\"uuid\",\"updates\":[]}";
    assertThatIllegalArgumentException()
        .isThrownBy((() -> CreateTableOperationParser.fromJson(invalidIdentifier)));

    // table-uuid present but not a string
    String invalidUuid =
        "{\"operation-type\":\"create-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":123,\"updates\":[]}";
    assertThatIllegalArgumentException()
        .isThrownBy((() -> CreateTableOperationParser.fromJson(invalidUuid)));

    // updates present but not an array
    String invalidUpdates =
        "{\"operation-type\":\"create-table\",\"identifier\":{\"namespace\":[],\"name\":\"table\"},\"table-uuid\":\"uuid\",\"updates\":\"not-an-array\"}";
    assertThatIllegalArgumentException()
        .isThrownBy((() -> CreateTableOperationParser.fromJson(invalidUpdates)));
  }
}
