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
package org.apache.iceberg.rest.requests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewVersionParser;
import org.junit.jupiter.api.Test;

public class TestCreateViewRequestParser {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> CreateViewRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid create view request: null");

    assertThatThrownBy(() -> CreateViewRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse create view request from null object");

    assertThatThrownBy(() -> CreateViewRequestParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: name");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> CreateViewRequestParser.fromJson("{\"x\": \"val\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: name");

    assertThatThrownBy(() -> CreateViewRequestParser.fromJson("{\"name\": \"view-name\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: view-version");

    String viewVersion =
        ViewVersionParser.toJson(
            ImmutableViewVersion.builder()
                .schemaId(0)
                .versionId(1)
                .timestampMillis(23L)
                .defaultNamespace(Namespace.of("ns1"))
                .build());

    assertThatThrownBy(
            () ->
                CreateViewRequestParser.fromJson(
                    String.format(
                        "{\"name\": \"view-name\", \"location\": \"loc\", \"view-version\": %s}",
                        viewVersion)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: schema");
  }

  @Test
  public void roundTripSerde() {
    CreateViewRequest request =
        ImmutableCreateViewRequest.builder()
            .name("view-name")
            .viewVersion(
                ImmutableViewVersion.builder()
                    .schemaId(0)
                    .versionId(1)
                    .timestampMillis(23L)
                    .defaultNamespace(Namespace.of("ns1"))
                    .build())
            .location("location")
            .schema(new Schema(Types.NestedField.required(1, "x", Types.LongType.get())))
            .properties(ImmutableMap.of("key1", "val1"))
            .build();

    String expectedJson =
        "{\n"
            + "  \"name\" : \"view-name\",\n"
            + "  \"location\" : \"location\",\n"
            + "  \"view-version\" : {\n"
            + "    \"version-id\" : 1,\n"
            + "    \"timestamp-ms\" : 23,\n"
            + "    \"schema-id\" : 0,\n"
            + "    \"summary\" : { },\n"
            + "    \"default-namespace\" : [ \"ns1\" ],\n"
            + "    \"representations\" : [ ]\n"
            + "  },\n"
            + "  \"schema\" : {\n"
            + "    \"type\" : \"struct\",\n"
            + "    \"schema-id\" : 0,\n"
            + "    \"fields\" : [ {\n"
            + "      \"id\" : 1,\n"
            + "      \"name\" : \"x\",\n"
            + "      \"required\" : true,\n"
            + "      \"type\" : \"long\"\n"
            + "    } ]\n"
            + "  },\n"
            + "  \"properties\" : {\n"
            + "    \"key1\" : \"val1\"\n"
            + "  }\n"
            + "}";

    String json = CreateViewRequestParser.toJson(request, true);
    assertThat(json).isEqualTo(expectedJson);

    // can't do an equality comparison because Schema doesn't implement equals/hashCode
    assertThat(CreateViewRequestParser.toJson(CreateViewRequestParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }
}
