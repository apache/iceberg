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
import org.apache.iceberg.rest.events.operations.DropTableOperation;
import org.apache.iceberg.rest.events.operations.ImmutableDropTableOperation;
import org.junit.jupiter.api.Test;

public class TestDropTableOperationParser {
  @Test
  void testToJson() {
    DropTableOperation op =
        ImmutableDropTableOperation.builder()
            .identifier(TableIdentifier.of(Namespace.of("a"), "t"))
            .tableUuid("uuid")
            .build();
    String expected =
        "{\"operation-type\":\"drop-table\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"},\"table-uuid\":\"uuid\"}";
    assertThat(DropTableOperationParser.toJson(op)).isEqualTo(expected);
  }

  @Test
  void testToJsonPretty() {
    DropTableOperation op =
        ImmutableDropTableOperation.builder()
            .identifier(TableIdentifier.of(Namespace.of("a"), "t"))
            .tableUuid("uuid")
            .build();
    String expected =
        "{\n"
            + "  \"operation-type\" : \"drop-table\",\n"
            + "  \"identifier\" : {\n"
            + "    \"namespace\" : [ \"a\" ],\n"
            + "    \"name\" : \"t\"\n"
            + "  },\n"
            + "  \"table-uuid\" : \"uuid\"\n"
            + "}";
    assertThat(DropTableOperationParser.toJsonPretty(op)).isEqualTo(expected);
  }

  @Test
  void testToJsonWithNullOperation() {
    assertThatNullPointerException()
        .isThrownBy(() -> DropTableOperationParser.toJson(null))
        .withMessage("Invalid drop table operation: null");
  }

  @Test
  void testToJsonWithOptionalProperties() {
    DropTableOperation op =
        ImmutableDropTableOperation.builder()
            .identifier(TableIdentifier.of(Namespace.of("a"), "t"))
            .tableUuid("uuid")
            .purge(true)
            .build();
    String expected =
        "{\"operation-type\":\"drop-table\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"purge\":true}";
    assertThat(DropTableOperationParser.toJson(op)).isEqualTo(expected);
  }

  @Test
  void testFromJson() {
    String json =
        "{\"operation-type\":\"drop-table\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"},\"table-uuid\":\"uuid\"}";
    DropTableOperation expected =
        ImmutableDropTableOperation.builder()
            .identifier(TableIdentifier.of(Namespace.of("a"), "t"))
            .tableUuid("uuid")
            .build();
    assertThat(DropTableOperationParser.fromJson(json)).isEqualTo(expected);
  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatNullPointerException()
        .isThrownBy(() -> DropTableOperationParser.fromJson((JsonNode) null))
        .withMessage("Cannot parse drop table operation from null object");
  }

  @Test
  void testFromJsonWithMissingProperties() {
    String missingIdentifier = "{\"operation-type\":\"drop-table\",\"table-uuid\":\"uuid\"}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> DropTableOperationParser.fromJson(missingIdentifier));

    String missingUuid =
        "{\"operation-type\":\"drop-table\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"}}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> DropTableOperationParser.fromJson(missingUuid));
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    // identifier present but not an object
    String invalidIdentifier =
        "{\"operation-type\":\"drop-table\",\"identifier\":\"not-obj\",\"table-uuid\":\"uuid\"}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> DropTableOperationParser.fromJson(invalidIdentifier));

    // table-uuid present but not a string
    String invalidUuid =
        "{\"operation-type\":\"drop-table\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"},\"table-uuid\":123}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> DropTableOperationParser.fromJson(invalidUuid));

    // purge present but not a boolean
    String invalidPurge =
        "{\"operation-type\":\"drop-table\",\"identifier\":{\"namespace\":[\"a\"],\"name\":\"t\"},\"table-uuid\":\"uuid\",\"purge\":\"yes\"}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> DropTableOperationParser.fromJson(invalidPurge));
  }
}
