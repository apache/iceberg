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
import java.util.List;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.events.operations.ImmutableUpdateTableOperation;
import org.apache.iceberg.rest.events.operations.UpdateTableOperation;
import org.junit.jupiter.api.Test;

public class TestUpdateTableOperationParser {
  @Test
  void testToJson() {
    UpdateTableOperation op =
        ImmutableUpdateTableOperation.builder()
            .identifier(TableIdentifier.of(Namespace.empty(), "t"))
            .tableUuid("uuid")
            .updates(List.of(new MetadataUpdate.AssignUUID("uuid")))
            .build();
    String expected =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    assertThat(UpdateTableOperationParser.toJson(op)).isEqualTo(expected);
  }

  @Test
  void testToJsonPretty() {
    UpdateTableOperation op =
        ImmutableUpdateTableOperation.builder()
            .identifier(TableIdentifier.of(Namespace.empty(), "t"))
            .tableUuid("uuid")
            .updates(List.of(new MetadataUpdate.AssignUUID("uuid")))
            .build();
    String expected =
        "{\n"
            + "  \"operation-type\" : \"update-table\",\n"
            + "  \"identifier\" : {\n"
            + "    \"namespace\" : [ ],\n"
            + "    \"name\" : \"t\"\n"
            + "  },\n"
            + "  \"table-uuid\" : \"uuid\",\n"
            + "  \"updates\" : [ {\n"
            + "    \"action\" : \"assign-uuid\",\n"
            + "    \"uuid\" : \"uuid\"\n"
            + "  } ]\n"
            + "}";
    assertThat(UpdateTableOperationParser.toJsonPretty(op)).isEqualTo(expected);
  }

  @Test
  void testToJsonWithNullOperation() {
    assertThatThrownBy(() -> UpdateTableOperationParser.toJson(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid update table operation: null");
  }

  @Test
  void testToJsonWithOptionalProperties() {
    UpdateTableOperation op =
        ImmutableUpdateTableOperation.builder()
            .identifier(TableIdentifier.of(Namespace.empty(), "t"))
            .tableUuid("uuid")
            .updates(List.of(new MetadataUpdate.AssignUUID("uuid")))
            .requirements(List.of(new UpdateRequirement.AssertRefSnapshotID("main", 5L)))
            .build();
    String expected =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}],\"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":5}]}";
    assertThat(UpdateTableOperationParser.toJson(op)).isEqualTo(expected);
  }

  @Test
  void testFromJson() {
    UpdateTableOperation op =
        ImmutableUpdateTableOperation.builder()
            .identifier(TableIdentifier.of(Namespace.empty(), "t"))
            .tableUuid("uuid")
            .updates(List.of(new MetadataUpdate.AssignUUID("uuid")))
            .build();
    String json =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    UpdateTableOperation parsed = UpdateTableOperationParser.fromJson(json);
    assertThat(parsed.identifier()).isEqualTo(op.identifier());
    assertThat(parsed.tableUuid()).isEqualTo(op.tableUuid());
    assertThat(parsed.updates()).hasSize(1);
    assertThat(((MetadataUpdate.AssignUUID) parsed.updates().get(0)).uuid())
        .isEqualTo(((MetadataUpdate.AssignUUID) op.updates().get(0)).uuid());
  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatThrownBy(() -> UpdateTableOperationParser.fromJson((JsonNode) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot parse update table operation from null object");
  }

  @Test
  void testFromJsonWithMissingProperties() {
    String missingIdentifier =
        "{\"operation-type\":\"update-table\",\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    assertThatThrownBy(() -> UpdateTableOperationParser.fromJson(missingIdentifier))
        .isInstanceOf(IllegalArgumentException.class);

    String missingUuid =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    assertThatThrownBy(() -> UpdateTableOperationParser.fromJson(missingUuid))
        .isInstanceOf(IllegalArgumentException.class);

    String missingUpdates =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":\"uuid\"}";
    assertThatThrownBy(() -> UpdateTableOperationParser.fromJson(missingUpdates))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    String invalidIdentifier =
        "{\"operation-type\":\"update-table\",\"identifier\":\"not-obj\",\"table-uuid\":\"uuid\",\"updates\":[]}";
    assertThatThrownBy(() -> UpdateTableOperationParser.fromJson(invalidIdentifier))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidUuid =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":123,\"updates\":[]}";
    assertThatThrownBy(() -> UpdateTableOperationParser.fromJson(invalidUuid))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidUpdates =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"updates\":\"not-array\"}";
    assertThatThrownBy(() -> UpdateTableOperationParser.fromJson(invalidUpdates))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidRequirements =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"updates\":[],\"requirements\":{}}";
    assertThatThrownBy(() -> UpdateTableOperationParser.fromJson(invalidRequirements))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testFromJsonWithOptionalProperties() {
    UpdateTableOperation op =
        ImmutableUpdateTableOperation.builder()
            .identifier(TableIdentifier.of(Namespace.empty(), "t"))
            .tableUuid("uuid")
            .updates(List.of(new MetadataUpdate.AssignUUID("uuid")))
            .requirements(List.of(new UpdateRequirement.AssertRefSnapshotID("main", 5L)))
            .build();
    String json =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}],\"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":5}]}";

    UpdateTableOperation parsed = UpdateTableOperationParser.fromJson(json);
    assertThat(parsed.identifier()).isEqualTo(op.identifier());
    assertThat(parsed.tableUuid()).isEqualTo(op.tableUuid());
    assertThat(parsed.updates()).hasSize(1);
    assertThat(((MetadataUpdate.AssignUUID) parsed.updates().get(0)).uuid())
        .isEqualTo(((MetadataUpdate.AssignUUID) op.updates().get(0)).uuid());
    assertThat(parsed.requirements()).hasSize(1);
    assertThat(((UpdateRequirement.AssertRefSnapshotID) parsed.requirements().get(0)).refName())
        .isEqualTo(((UpdateRequirement.AssertRefSnapshotID) op.requirements().get(0)).refName());
    assertThat(((UpdateRequirement.AssertRefSnapshotID) parsed.requirements().get(0)).snapshotId())
        .isEqualTo(((UpdateRequirement.AssertRefSnapshotID) op.requirements().get(0)).snapshotId());
  }
}
