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
import org.apache.iceberg.rest.events.operations.CreateViewOperation;
import org.apache.iceberg.rest.events.operations.ImmutableCreateViewOperation;
import org.junit.jupiter.api.Test;

public class TestCreateViewOperationParser {
  @Test
  void testToJson() {
    CreateViewOperation op =
        ImmutableCreateViewOperation.builder()
            .identifier(TableIdentifier.of(Namespace.empty(), "view"))
            .viewUuid("uuid")
            .updates(List.of(new MetadataUpdate.AssignUUID("uuid")))
            .build();
    String json =
        "{\"operation-type\":\"create-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    assertThat(CreateViewOperationParser.toJson(op)).isEqualTo(json);
  }

  @Test
  void testToJsonPretty() {
    CreateViewOperation op =
        ImmutableCreateViewOperation.builder()
            .identifier(TableIdentifier.of(Namespace.empty(), "view"))
            .viewUuid("uuid")
            .updates(List.of(new MetadataUpdate.AssignUUID("uuid")))
            .build();
    String json =
        "{\n"
            + "  \"operation-type\" : \"create-view\",\n"
            + "  \"identifier\" : {\n"
            + "    \"namespace\" : [ ],\n"
            + "    \"name\" : \"view\"\n"
            + "  },\n"
            + "  \"view-uuid\" : \"uuid\",\n"
            + "  \"updates\" : [ {\n"
            + "    \"action\" : \"assign-uuid\",\n"
            + "    \"uuid\" : \"uuid\"\n"
            + "  } ]\n"
            + "}";
    assertThat(CreateViewOperationParser.toJsonPretty(op)).isEqualTo(json);
  }

  @Test
  void testToJsonWithNullOperation() {
    assertThatNullPointerException()
        .isThrownBy(() -> CreateViewOperationParser.toJson(null))
        .withMessage("Invalid create view operation: null");
  }

  @Test
  void testFromJson() {
    String json =
        "{\"operation-type\":\"create-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":\"uuid\",\"updates\":[{\"action\":\"assign-uuid\",\"uuid\":\"uuid\"}]}";
    CreateViewOperation expected =
        ImmutableCreateViewOperation.builder()
            .identifier(TableIdentifier.of(Namespace.empty(), "view"))
            .viewUuid("uuid")
            .updates(List.of(new MetadataUpdate.AssignUUID("uuid")))
            .build();
    CreateViewOperation actual = CreateViewOperationParser.fromJson(json);

    assertThat(actual.operationType())
        .isEqualTo(expected.operationType());
    assertThat(actual.identifier())
        .isEqualTo(expected.identifier());
    assertThat(actual.viewUuid()).isEqualTo(expected.viewUuid());

    assertThat(actual.updates()).hasSize(1);
    assertThat(actual.updates().get(0))
        .isInstanceOf(MetadataUpdate.AssignUUID.class);
    assertThat(((MetadataUpdate.AssignUUID) actual.updates().get(0)).uuid())
        .isEqualTo(
            ((MetadataUpdate.AssignUUID) expected.updates().get(0)).uuid());

  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatNullPointerException()
        .isThrownBy(() -> CreateViewOperationParser.fromJson((JsonNode) null))
        .withMessage("Cannot parse create view operation from null object");
  }

  @Test
  void testFromJsonWithMissingProperties() {
    String missingIdentifier = "{\"operation-type\":\"create-view\",\"view-uuid\":\"uuid\"}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CreateViewOperationParser.fromJson(missingIdentifier));

    String missingUuid =
        "{\"operation-type\":\"create-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"}}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CreateViewOperationParser.fromJson(missingUuid));

    String missingUpdates =
        "{\"operation-type\":\"create-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":\"uuid\"}";
    assertThatIllegalArgumentException()
        .isThrownBy((() -> CreateViewOperationParser.fromJson(missingUpdates)));
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    // identifier present but not an object
    String invalidIdentifier =
        "{\"operation-type\":\"create-view\",\"identifier\":\"not-an-object\",\"view-uuid\":\"uuid\"}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CreateViewOperationParser.fromJson(invalidIdentifier));

    // view-uuid present but not a string
    String invalidUuid =
        "{\"operation-type\":\"create-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":123}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CreateViewOperationParser.fromJson(invalidUuid));

    // updates present but not an array
    String invalidUpdates =
        "{\"operation-type\":\"create-view\",\"identifier\":{\"namespace\":[],\"name\":\"view\"},\"view-uuid\":\"uuid\",\"updates\":\"not-an-array\"}";
    assertThatIllegalArgumentException()
        .isThrownBy((() -> CreateViewOperationParser.fromJson(invalidUpdates)));
  }
}
