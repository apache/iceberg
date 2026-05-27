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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestUnregisterTableResponseParser {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> UnregisterTableResponseParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid unregister table response: null");

    assertThatThrownBy(() -> UnregisterTableResponseParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse unregister table response from null object");

    assertThatThrownBy(() -> UnregisterTableResponseParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: metadata-location");
  }

  @Test
  public void missingFields() {
    assertThatThrownBy(
            () ->
                UnregisterTableResponseParser.fromJson(
                    "{\"metadata-location\": \"custom-location\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: metadata");
  }

  @Test
  public void roundTripSerde() {
    String uuid = "386b9f01-002b-4d8c-b77f-42c3fd3b7c9b";
    TableMetadata metadata =
        TableMetadata.buildFromEmpty(1)
            .assignUUID(uuid)
            .setLocation("location")
            .setCurrentSchema(
                new Schema(Types.NestedField.required(1, "x", Types.LongType.get())), 1)
            .addPartitionSpec(PartitionSpec.unpartitioned())
            .addSortOrder(SortOrder.unsorted())
            .discardChanges()
            .withMetadataLocation("metadata-location")
            .build();

    UnregisterTableResponse response =
        ImmutableUnregisterTableResponse.builder()
            .metadataLocation("metadata-location")
            .metadata(metadata)
            .build();

    String expectedJson =
        String.format(
            "{\n"
                + "  \"metadata-location\" : \"metadata-location\",\n"
                + "  \"metadata\" : {\n"
                + "    \"format-version\" : 1,\n"
                + "    \"table-uuid\" : \"386b9f01-002b-4d8c-b77f-42c3fd3b7c9b\",\n"
                + "    \"location\" : \"location\",\n"
                + "    \"last-updated-ms\" : %s,\n"
                + "    \"last-column-id\" : 1,\n"
                + "    \"schema\" : {\n"
                + "      \"type\" : \"struct\",\n"
                + "      \"schema-id\" : 0,\n"
                + "      \"fields\" : [ {\n"
                + "        \"id\" : 1,\n"
                + "        \"name\" : \"x\",\n"
                + "        \"required\" : true,\n"
                + "        \"type\" : \"long\"\n"
                + "      } ]\n"
                + "    },\n"
                + "    \"current-schema-id\" : 0,\n"
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
                + "    \"partition-spec\" : [ ],\n"
                + "    \"default-spec-id\" : 0,\n"
                + "    \"partition-specs\" : [ {\n"
                + "      \"spec-id\" : 0,\n"
                + "      \"fields\" : [ ]\n"
                + "    } ],\n"
                + "    \"last-partition-id\" : 999,\n"
                + "    \"default-sort-order-id\" : 0,\n"
                + "    \"sort-orders\" : [ {\n"
                + "      \"order-id\" : 0,\n"
                + "      \"fields\" : [ ]\n"
                + "    } ],\n"
                + "    \"properties\" : { },\n"
                + "    \"current-snapshot-id\" : -1,\n"
                + "    \"refs\" : { },\n"
                + "    \"snapshots\" : [ ],\n"
                + "    \"statistics\" : [ ],\n"
                + "    \"partition-statistics\" : [ ],\n"
                + "    \"snapshot-log\" : [ ],\n"
                + "    \"metadata-log\" : [ ]\n"
                + "  }\n"
                + "}",
            metadata.lastUpdatedMillis());

    String json = UnregisterTableResponseParser.toJson(response, true);
    assertThat(json).isEqualTo(expectedJson);
    // can't do an equality comparison because Schema doesn't implement equals/hashCode
    assertThat(
            UnregisterTableResponseParser.toJson(
                UnregisterTableResponseParser.fromJson(json), true))
        .isEqualTo(expectedJson);
  }
}
