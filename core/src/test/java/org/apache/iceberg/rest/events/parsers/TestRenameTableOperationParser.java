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
import org.apache.iceberg.rest.events.operations.ImmutableRenameTableOperation;
import org.apache.iceberg.rest.events.operations.RenameTableOperation;
import org.junit.jupiter.api.Test;

public class TestRenameTableOperationParser {
  @Test
  void testToJson() {
    RenameTableOperation op =
        ImmutableRenameTableOperation.builder()
            .tableUuid("uuid")
            .sourceIdentifier(TableIdentifier.of(Namespace.empty(), "s"))
            .destIdentifier(TableIdentifier.of(Namespace.of("a"), "d"))
            .build();
    String expected =
        "{\"operation-type\":\"rename-table\",\"table-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"s\"},\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}";
    assertThat(RenameTableOperationParser.toJson(op)).isEqualTo(expected);
  }

  @Test
  void testToJsonPretty() {
    RenameTableOperation op =
        ImmutableRenameTableOperation.builder()
            .tableUuid("uuid")
            .sourceIdentifier(TableIdentifier.of(Namespace.empty(), "s"))
            .destIdentifier(TableIdentifier.of(Namespace.of("a"), "d"))
            .build();
    String expected =
        "{\n"
            + "  \"operation-type\" : \"rename-table\",\n"
            + "  \"table-uuid\" : \"uuid\",\n"
            + "  \"source\" : {\n"
            + "    \"namespace\" : [ ],\n"
            + "    \"name\" : \"s\"\n"
            + "  },\n"
            + "  \"destination\" : {\n"
            + "    \"namespace\" : [ \"a\" ],\n"
            + "    \"name\" : \"d\"\n"
            + "  }\n"
            + "}";
    assertThat(RenameTableOperationParser.toJsonPretty(op)).isEqualTo(expected);
  }

  @Test
  void testToJsonWithNullOperation() {
    assertThatNullPointerException()
        .isThrownBy(() -> RenameTableOperationParser.toJson(null))
        .withMessage("Invalid rename table operation: null");
  }

  @Test
  void testFromJson() {
    String json =
        "{\"operation-type\":\"rename-table\",\"table-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"s\"},\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}";
    RenameTableOperation expected =
        ImmutableRenameTableOperation.builder()
            .tableUuid("uuid")
            .sourceIdentifier(TableIdentifier.of(Namespace.empty(), "s"))
            .destIdentifier(TableIdentifier.of(Namespace.of("a"), "d"))
            .build();
    assertThat(RenameTableOperationParser.fromJson(json)).isEqualTo(expected);
  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatNullPointerException()
        .isThrownBy(() -> RenameTableOperationParser.fromJson((JsonNode) null))
        .withMessage("Cannot parse rename table operation from null object");
  }

  @Test
  void testFromJsonWithMissingProperties() {
    String missingUuid =
        "{\"operation-type\":\"rename-table\",\"source\":{\"namespace\":[],\"name\":\"s\"},\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> RenameTableOperationParser.fromJson(missingUuid));

    String missingSource =
        "{\"operation-type\":\"rename-table\",\"table-uuid\":\"uuid\",\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> RenameTableOperationParser.fromJson(missingSource));

    String missingDest =
        "{\"operation-type\":\"rename-table\",\"table-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"s\"}}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> RenameTableOperationParser.fromJson(missingDest));
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    String invalidUuid =
        "{\"operation-type\":\"rename-table\",\"table-uuid\":123,\"source\":{\"namespace\":[],\"name\":\"s\"},\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> RenameTableOperationParser.fromJson(invalidUuid));

    String invalidSource =
        "{\"operation-type\":\"rename-table\",\"table-uuid\":\"uuid\",\"source\":\"not-obj\",\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> RenameTableOperationParser.fromJson(invalidSource));

    String invalidDest =
        "{\"operation-type\":\"rename-table\",\"table-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"s\"},\"destination\":123}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> RenameTableOperationParser.fromJson(invalidDest));
  }
}
