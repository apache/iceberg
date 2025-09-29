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
import org.apache.iceberg.rest.events.operations.ImmutableUpdateViewOperation;
import org.apache.iceberg.rest.events.operations.UpdateViewOperation;
import org.junit.jupiter.api.Test;

public class TestUpdateViewOperationParser {
  @Test
  void testToJson() {
    UpdateViewOperation op =
        ImmutableUpdateViewOperation.builder()
            .identifier(TableIdentifier.of(Namespace.empty(), "v"))
            .viewUuid("uuid")
            .updates(List.of(new MetadataUpdate.AssignUUID("uuid")))
            .build();
    String expected =
        "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"view-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    assertThat(UpdateViewOperationParser.toJson(op)).isEqualTo(expected);
  }

  @Test
  void testToJsonPretty() {
    UpdateViewOperation op =
        ImmutableUpdateViewOperation.builder()
            .identifier(TableIdentifier.of(Namespace.empty(), "v"))
            .viewUuid("uuid")
            .updates(List.of(new MetadataUpdate.AssignUUID("uuid")))
            .build();
    String expected =
        "{\n"
            + "  \"operation-type\" : \"update-view\",\n"
            + "  \"identifier\" : {\n"
            + "    \"namespace\" : [ ],\n"
            + "    \"name\" : \"v\"\n"
            + "  },\n"
            + "  \"view-uuid\" : \"uuid\",\n"
            + "  \"updates\" : [ {\n"
            + "    \"action\" : \"assign-uuid\",\n"
            + "    \"uuid\" : \"uuid\"\n"
            + "  } ]\n"
            + "}";
    assertThat(UpdateViewOperationParser.toJsonPretty(op)).isEqualTo(expected);
  }

  @Test
  void testToJsonWithNullOperation() {
    assertThatThrownBy(() -> UpdateViewOperationParser.toJson(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid update view operation: null");
  }

  @Test
  void testToJsonWithOptionalProperties() {
    UpdateViewOperation op =
        ImmutableUpdateViewOperation.builder()
            .identifier(TableIdentifier.of(Namespace.empty(), "v"))
            .viewUuid("uuid")
            .updates(List.of(new MetadataUpdate.AssignUUID("uuid")))
            .requirements(List.of(new UpdateRequirement.AssertRefSnapshotID("main", 5L)))
            .build();
    String expected =
        "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"view-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}],\"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":5}]}";
    assertThat(UpdateViewOperationParser.toJson(op)).isEqualTo(expected);
  }

  @Test
  void testFromJson() {
    UpdateViewOperation op =
        ImmutableUpdateViewOperation.builder()
            .identifier(TableIdentifier.of(Namespace.empty(), "v"))
            .viewUuid("uuid")
            .updates(List.of(new MetadataUpdate.AssignUUID("uuid")))
            .build();
    String json =
        "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"view-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    UpdateViewOperation parsed = UpdateViewOperationParser.fromJson(json);
    assertThat(parsed.identifier()).isEqualTo(TableIdentifier.of(Namespace.empty(), "v"));
    assertThat(parsed.viewUuid()).isEqualTo("uuid");
    assertThat(parsed.updates()).hasSize(1);
    assertThat(((MetadataUpdate.AssignUUID) parsed.updates().get(0)).uuid())
        .isEqualTo(((MetadataUpdate.AssignUUID) op.updates().get(0)).uuid());
  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatThrownBy(() -> UpdateViewOperationParser.fromJson((JsonNode) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot parse update view operation from null object");
  }

  @Test
  void testFromJsonWithMissingProperties() {
    String missingIdentifier =
        "{\"operation-type\":\"update-view\",\"view-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    assertThatThrownBy(() -> UpdateViewOperationParser.fromJson(missingIdentifier))
        .isInstanceOf(IllegalArgumentException.class);

    String missingUuid =
        "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    assertThatThrownBy(() -> UpdateViewOperationParser.fromJson(missingUuid))
        .isInstanceOf(IllegalArgumentException.class);

    String missingUpdates =
        "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"view-uuid\":\"uuid\"}";
    assertThatThrownBy(() -> UpdateViewOperationParser.fromJson(missingUpdates))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    String invalidIdentifier =
        "{\"operation-type\":\"update-view\",\"identifier\":\"not-obj\",\"view-uuid\":\"uuid\",\"updates\":[]}";
    assertThatThrownBy(() -> UpdateViewOperationParser.fromJson(invalidIdentifier))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidUuid =
        "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"view-uuid\":123,\"updates\":[]}";
    assertThatThrownBy(() -> UpdateViewOperationParser.fromJson(invalidUuid))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidUpdates =
        "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"view-uuid\":\"uuid\",\"updates\":\"not-array\"}";
    assertThatThrownBy(() -> UpdateViewOperationParser.fromJson(invalidUpdates))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidRequirements =
        "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"view-uuid\":\"uuid\",\"updates\":[],\"requirements\":{}}";
    assertThatThrownBy(() -> UpdateViewOperationParser.fromJson(invalidRequirements))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testFromJsonWithOptionalProperties() {
    UpdateViewOperation op =
        ImmutableUpdateViewOperation.builder()
            .identifier(TableIdentifier.of(Namespace.empty(), "v"))
            .viewUuid("uuid")
            .updates(List.of(new MetadataUpdate.AssignUUID("uuid")))
            .requirements(List.of(new UpdateRequirement.AssertRefSnapshotID("main", 5L)))
            .build();
    String json =
        "{\"operation-type\":\"update-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"view-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}],\"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":5}]}";

    UpdateViewOperation parsed = UpdateViewOperationParser.fromJson(json);
    assertThat(parsed.identifier()).isEqualTo(op.identifier());
    assertThat(parsed.viewUuid()).isEqualTo(op.viewUuid());
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
