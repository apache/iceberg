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
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.events.CatalogOperationParser;
import org.apache.iceberg.rest.events.operations.CatalogOperation;
import org.junit.jupiter.api.Test;

public class TestUpdateTableOperationParser {
  @Test
  void testToJson() {
    CatalogOperation.UpdateTable op =
        new CatalogOperation.UpdateTable(
            TableIdentifier.of(Namespace.empty(), "t"),
            "uuid",
            List.of(new MetadataUpdate.AssignUUID("uuid")),
            null);
    String expected =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    assertThat(CatalogOperationParser.toJson(op)).isEqualTo(expected);
  }

  @Test
  void testToJsonPretty() {
    CatalogOperation.UpdateTable op =
        new CatalogOperation.UpdateTable(
            TableIdentifier.of(Namespace.empty(), "t"),
            "uuid",
            List.of(new MetadataUpdate.AssignUUID("uuid")),
            null);
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
    CatalogOperation.UpdateTable op =
        new CatalogOperation.UpdateTable(
            TableIdentifier.of(Namespace.empty(), "t"),
            "uuid",
            List.of(new MetadataUpdate.AssignUUID("uuid")),
            List.of(new UpdateRequirement.AssertRefSnapshotID("main", 5L)));
    String expected =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}],\"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":5}]}";
    assertThat(CatalogOperationParser.toJson(op)).isEqualTo(expected);
  }

  @Test
  void testFromJson() {
    CatalogOperation.UpdateTable op =
        new CatalogOperation.UpdateTable(
            TableIdentifier.of(Namespace.empty(), "t"),
            "uuid",
            List.of(new MetadataUpdate.AssignUUID("uuid")),
            null);
    String json =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    CatalogOperation.UpdateTable parsed =
        (CatalogOperation.UpdateTable) CatalogOperationParser.fromJson(json);
    assertThat(parsed.identifier()).isEqualTo(op.identifier());
    assertThat(parsed.tableUuid()).isEqualTo(op.tableUuid());
    assertThat(parsed.updates()).hasSize(1);
    assertThat(((MetadataUpdate.AssignUUID) parsed.updates().get(0)).uuid())
        .isEqualTo(((MetadataUpdate.AssignUUID) op.updates().get(0)).uuid());
  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatNullPointerException()
        .isThrownBy(() -> CatalogOperationParser.fromJson((JsonNode) null))
        .withMessage("Cannot parse catalog operation from null object");
  }

  @Test
  void testFromJsonWithMissingProperties() {
    String missingIdentifier =
        "{\"operation-type\":\"update-table\",\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(missingIdentifier));

    String missingUuid =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(missingUuid));

    String missingUpdates =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":\"uuid\"}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(missingUpdates));
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    String invalidIdentifier =
        "{\"operation-type\":\"update-table\",\"identifier\":\"not-obj\",\"table-uuid\":\"uuid\",\"updates\":[]}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(invalidIdentifier));

    String invalidUuid =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":123,\"updates\":[]}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(invalidUuid));

    String invalidUpdates =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"updates\":\"not-array\"}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(invalidUpdates));

    String invalidRequirements =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"updates\":[],\"requirements\":{}}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(invalidRequirements));
  }

  @Test
  void testFromJsonWithOptionalProperties() {
    CatalogOperation.UpdateTable op =
        new CatalogOperation.UpdateTable(
            TableIdentifier.of(Namespace.empty(), "t"),
            "uuid",
            List.of(new MetadataUpdate.AssignUUID("uuid")),
            List.of(new UpdateRequirement.AssertRefSnapshotID("main", 5L)));
    String json =
        "{\"operation-type\":\"update-table\",\"identifier\":{\"namespace\":[],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}],\"requirements\":[{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":5}]}";

    CatalogOperation.UpdateTable parsed =
        (CatalogOperation.UpdateTable) CatalogOperationParser.fromJson(json);
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
