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
import org.apache.iceberg.rest.events.operations.DropViewOperation;
import org.apache.iceberg.rest.events.operations.ImmutableDropViewOperation;
import org.junit.jupiter.api.Test;

public class TestDropViewOperationParser {
  @Test
  void testToJson() {
    DropViewOperation op =
        ImmutableDropViewOperation.builder()
            .identifier(TableIdentifier.of(Namespace.empty(), "v"))
            .viewUuid("uuid")
            .build();
    String expected =
        "{\"operation-type\":\"drop-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"view-uuid\":\"uuid\"}";
    assertThat(DropViewOperationParser.toJson(op)).isEqualTo(expected);
  }

  @Test
  void testToJsonPretty() {
    DropViewOperation op =
        ImmutableDropViewOperation.builder()
            .identifier(TableIdentifier.of(Namespace.empty(), "v"))
            .viewUuid("uuid")
            .build();
    String expected =
        "{\n"
            + "  \"operation-type\" : \"drop-view\",\n"
            + "  \"identifier\" : {\n"
            + "    \"namespace\" : [ ],\n"
            + "    \"name\" : \"v\"\n"
            + "  },\n"
            + "  \"view-uuid\" : \"uuid\"\n"
            + "}";
    assertThat(DropViewOperationParser.toJsonPretty(op)).isEqualTo(expected);
  }

  @Test
  void testToJsonWithNullOperation() {
    assertThatThrownBy(() -> DropViewOperationParser.toJson(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid drop view operation: null");
  }

  @Test
  void testFromJson() {
    String json =
        "{\"operation-type\":\"drop-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"view-uuid\":\"uuid\"}";
    DropViewOperation expected =
        ImmutableDropViewOperation.builder()
            .identifier(TableIdentifier.of(Namespace.empty(), "v"))
            .viewUuid("uuid")
            .build();
    assertThat(DropViewOperationParser.fromJson(json)).isEqualTo(expected);
  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatThrownBy(() -> DropViewOperationParser.fromJson((JsonNode) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot parse drop view operation from null object");
  }

  @Test
  void testFromJsonWithMissingProperties() {
    String missingIdentifier = "{\"operation-type\":\"drop-view\",\"view-uuid\":\"uuid\"}";
    assertThatThrownBy(() -> DropViewOperationParser.fromJson(missingIdentifier))
        .isInstanceOf(IllegalArgumentException.class);

    String missingUuid =
        "{\"operation-type\":\"drop-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"}}";
    assertThatThrownBy(() -> DropViewOperationParser.fromJson(missingUuid))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    String invalidIdentifier =
        "{\"operation-type\":\"drop-view\",\"identifier\":\"not-obj\",\"view-uuid\":\"uuid\"}";
    assertThatThrownBy(() -> DropViewOperationParser.fromJson(invalidIdentifier))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidUuid =
        "{\"operation-type\":\"drop-view\",\"identifier\":{\"namespace\":[],\"name\":\"v\"},\"view-uuid\":123}";
    assertThatThrownBy(() -> DropViewOperationParser.fromJson(invalidUuid))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
