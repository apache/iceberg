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
import org.apache.iceberg.rest.events.CatalogOperationParser;
import org.apache.iceberg.rest.events.operations.CatalogOperation;
import org.junit.jupiter.api.Test;

public class TestRenameViewOperationParser {
  @Test
  void testToJson() {
    CatalogOperation.RenameView op =
        new CatalogOperation.RenameView(
            TableIdentifier.of(Namespace.empty(), "s"),
            TableIdentifier.of(Namespace.of("a"), "d"),
            "uuid");
    String expected =
        "{\"operation-type\":\"rename-view\",\"view-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"s\"},\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}";
    assertThat(CatalogOperationParser.toJson(op)).isEqualTo(expected);
  }

  @Test
  void testToJsonPretty() {
    CatalogOperation.RenameView op =
        new CatalogOperation.RenameView(
            TableIdentifier.of(Namespace.empty(), "s"),
            TableIdentifier.of(Namespace.of("a"), "d"),
            "uuid");
    String expected =
        "{\n"
            + "  \"operation-type\" : \"rename-view\",\n"
            + "  \"view-uuid\" : \"uuid\",\n"
            + "  \"source\" : {\n"
            + "    \"namespace\" : [ ],\n"
            + "    \"name\" : \"s\"\n"
            + "  },\n"
            + "  \"destination\" : {\n"
            + "    \"namespace\" : [ \"a\" ],\n"
            + "    \"name\" : \"d\"\n"
            + "  }\n"
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
  void testFromJson() {
    String json =
        "{\"operation-type\":\"rename-view\",\"view-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"s\"},\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}";
    CatalogOperation.RenameView expected =
        new CatalogOperation.RenameView(
            TableIdentifier.of(Namespace.empty(), "s"),
            TableIdentifier.of(Namespace.of("a"), "d"),
            "uuid");
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
    String missingUuid =
        "{\"operation-type\":\"rename-view\",\"source\":{\"namespace\":[],\"name\":\"s\"},\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(missingUuid));

    String missingSource =
        "{\"operation-type\":\"rename-view\",\"view-uuid\":\"uuid\",\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(missingSource));

    String missingDest =
        "{\"operation-type\":\"rename-view\",\"view-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"s\"}}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(missingDest));
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    String invalidUuid =
        "{\"operation-type\":\"rename-view\",\"view-uuid\":123,\"source\":{\"namespace\":[],\"name\":\"s\"},\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(invalidUuid));

    String invalidSource =
        "{\"operation-type\":\"rename-view\",\"view-uuid\":\"uuid\",\"source\":\"not-obj\",\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(invalidSource));

    String invalidDest =
        "{\"operation-type\":\"rename-view\",\"view-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"s\"},\"destination\":123}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogOperationParser.fromJson(invalidDest));
  }

  @Test
  void testRoundTrip() {
    String json =
        "{\"operation-type\":\"rename-view\",\"view-uuid\":\"uuid\",\"source\":{\"namespace\":[],\"name\":\"s\"},\"destination\":{\"namespace\":[\"a\"],\"name\":\"d\"}}";
    assertThat(CatalogOperationParser.toJson(CatalogOperationParser.fromJson(json)))
        .isEqualTo(json);
  }
}
