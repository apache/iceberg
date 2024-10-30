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
package org.apache.iceberg.rest.responses;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewMetadata;
import org.junit.jupiter.api.Test;

public class TestLoadViewResponseParser {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> LoadViewResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid load view response: null");

    assertThatThrownBy(() -> LoadViewResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse load view response from null object");

    assertThatThrownBy(() -> LoadViewResponseParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: metadata-location");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(() -> LoadViewResponseParser.fromJson("{\"x\": \"val\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: metadata-location");

    assertThatThrownBy(
            () -> LoadViewResponseParser.fromJson("{\"metadata-location\": \"custom-location\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: metadata");
  }

  @Test
  public void roundTripSerde() {
    String uuid = "386b9f01-002b-4d8c-b77f-42c3fd3b7c9b";
    ViewMetadata viewMetadata =
        ViewMetadata.builder()
            .assignUUID(uuid)
            .setLocation("location")
            .addSchema(new Schema(Types.NestedField.required(1, "x", Types.LongType.get())))
            .addVersion(
                ImmutableViewVersion.builder()
                    .schemaId(0)
                    .versionId(1)
                    .timestampMillis(23L)
                    .defaultNamespace(Namespace.of("ns1"))
                    .build())
            .addVersion(
                ImmutableViewVersion.builder()
                    .schemaId(0)
                    .versionId(2)
                    .timestampMillis(24L)
                    .defaultNamespace(Namespace.of("ns2"))
                    .build())
            .addVersion(
                ImmutableViewVersion.builder()
                    .schemaId(0)
                    .versionId(3)
                    .timestampMillis(25L)
                    .defaultNamespace(Namespace.of("ns3"))
                    .build())
            .setCurrentVersionId(3)
            .build();

    LoadViewResponse response =
        ImmutableLoadViewResponse.builder()
            .metadata(viewMetadata)
            .metadataLocation("custom-location")
            .build();
    String expectedJson =
        "{\n"
            + "  \"metadata-location\" : \"custom-location\",\n"
            + "  \"metadata\" : {\n"
            + "    \"view-uuid\" : \"386b9f01-002b-4d8c-b77f-42c3fd3b7c9b\",\n"
            + "    \"format-version\" : 1,\n"
            + "    \"location\" : \"location\",\n"
            + "    \"schemas\" : [ {\n"
            + "      \"type\" : \"struct\",\n"
            + "      \"schema-id\" : 0,\n"
            + "      \"fields\" : [ {\n"
            + "        \"id\" : 1,\n"
            + "        \"name\" : \"x\",\n"
            + "        \"required\" : true,\n"
            + "        \"type\" : \"long\"\n"
            + "      } ]\n"
            + "    } ],\n"
            + "    \"current-version-id\" : 3,\n"
            + "    \"versions\" : [ {\n"
            + "      \"version-id\" : 1,\n"
            + "      \"timestamp-ms\" : 23,\n"
            + "      \"schema-id\" : 0,\n"
            + "      \"summary\" : { },\n"
            + "      \"default-namespace\" : [ \"ns1\" ],\n"
            + "      \"representations\" : [ ]\n"
            + "    }, {\n"
            + "      \"version-id\" : 2,\n"
            + "      \"timestamp-ms\" : 24,\n"
            + "      \"schema-id\" : 0,\n"
            + "      \"summary\" : { },\n"
            + "      \"default-namespace\" : [ \"ns2\" ],\n"
            + "      \"representations\" : [ ]\n"
            + "    }, {\n"
            + "      \"version-id\" : 3,\n"
            + "      \"timestamp-ms\" : 25,\n"
            + "      \"schema-id\" : 0,\n"
            + "      \"summary\" : { },\n"
            + "      \"default-namespace\" : [ \"ns3\" ],\n"
            + "      \"representations\" : [ ]\n"
            + "    } ],\n"
            + "    \"version-log\" : [ {\n"
            + "      \"timestamp-ms\" : 25,\n"
            + "      \"version-id\" : 3\n"
            + "    } ]\n"
            + "  }\n"
            + "}";

    String json = LoadViewResponseParser.toJson(response, true);
    assertThat(json).isEqualTo(expectedJson);
    // can't do an equality comparison because Schema doesn't implement equals/hashCode
    assertThat(LoadViewResponseParser.toJson(LoadViewResponseParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }

  @Test
  public void roundTripSerdeWithConfig() {
    String uuid = "386b9f01-002b-4d8c-b77f-42c3fd3b7c9b";
    ViewMetadata viewMetadata =
        ViewMetadata.builder()
            .assignUUID(uuid)
            .setLocation("location")
            .addSchema(new Schema(Types.NestedField.required(1, "x", Types.LongType.get())))
            .addVersion(
                ImmutableViewVersion.builder()
                    .schemaId(0)
                    .versionId(1)
                    .timestampMillis(23L)
                    .defaultNamespace(Namespace.of("ns1"))
                    .build())
            .addVersion(
                ImmutableViewVersion.builder()
                    .schemaId(0)
                    .versionId(2)
                    .timestampMillis(24L)
                    .defaultNamespace(Namespace.of("ns2"))
                    .build())
            .addVersion(
                ImmutableViewVersion.builder()
                    .schemaId(0)
                    .versionId(3)
                    .timestampMillis(25L)
                    .defaultNamespace(Namespace.of("ns3"))
                    .build())
            .setCurrentVersionId(3)
            .build();

    LoadViewResponse response =
        ImmutableLoadViewResponse.builder()
            .metadata(viewMetadata)
            .metadataLocation("custom-location")
            .config(ImmutableMap.of("key1", "val1", "key2", "val2"))
            .build();
    String expectedJson =
        "{\n"
            + "  \"metadata-location\" : \"custom-location\",\n"
            + "  \"metadata\" : {\n"
            + "    \"view-uuid\" : \"386b9f01-002b-4d8c-b77f-42c3fd3b7c9b\",\n"
            + "    \"format-version\" : 1,\n"
            + "    \"location\" : \"location\",\n"
            + "    \"schemas\" : [ {\n"
            + "      \"type\" : \"struct\",\n"
            + "      \"schema-id\" : 0,\n"
            + "      \"fields\" : [ {\n"
            + "        \"id\" : 1,\n"
            + "        \"name\" : \"x\",\n"
            + "        \"required\" : true,\n"
            + "        \"type\" : \"long\"\n"
            + "      } ]\n"
            + "    } ],\n"
            + "    \"current-version-id\" : 3,\n"
            + "    \"versions\" : [ {\n"
            + "      \"version-id\" : 1,\n"
            + "      \"timestamp-ms\" : 23,\n"
            + "      \"schema-id\" : 0,\n"
            + "      \"summary\" : { },\n"
            + "      \"default-namespace\" : [ \"ns1\" ],\n"
            + "      \"representations\" : [ ]\n"
            + "    }, {\n"
            + "      \"version-id\" : 2,\n"
            + "      \"timestamp-ms\" : 24,\n"
            + "      \"schema-id\" : 0,\n"
            + "      \"summary\" : { },\n"
            + "      \"default-namespace\" : [ \"ns2\" ],\n"
            + "      \"representations\" : [ ]\n"
            + "    }, {\n"
            + "      \"version-id\" : 3,\n"
            + "      \"timestamp-ms\" : 25,\n"
            + "      \"schema-id\" : 0,\n"
            + "      \"summary\" : { },\n"
            + "      \"default-namespace\" : [ \"ns3\" ],\n"
            + "      \"representations\" : [ ]\n"
            + "    } ],\n"
            + "    \"version-log\" : [ {\n"
            + "      \"timestamp-ms\" : 25,\n"
            + "      \"version-id\" : 3\n"
            + "    } ]\n"
            + "  },\n"
            + "  \"config\" : {\n"
            + "    \"key1\" : \"val1\",\n"
            + "    \"key2\" : \"val2\"\n"
            + "  }\n"
            + "}";

    String json = LoadViewResponseParser.toJson(response, true);
    assertThat(json).isEqualTo(expectedJson);
    // can't do an equality comparison because Schema doesn't implement equals/hashCode
    assertThat(LoadViewResponseParser.toJson(LoadViewResponseParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }
}
