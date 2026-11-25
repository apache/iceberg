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
import org.apache.iceberg.TableMetadataParser;
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
            .withMetadataLocation("metadataTestLocation")
            .build();

    UnregisterTableResponse response =
        ImmutableUnregisterTableResponse.builder()
            .metadataLocation("metadataTestLocation")
            .metadata(metadata)
            .build();

    String expectedJson =
        String.format(
            "{\"metadata-location\":\"metadataTestLocation\",\"metadata\":%s}",
            TableMetadataParser.toJson(metadata));
    String actualJson = UnregisterTableResponseParser.toJson(response);
    assertThat(actualJson).isEqualTo(expectedJson);

    UnregisterTableResponse parsed = UnregisterTableResponseParser.fromJson(actualJson);
    assertThat(parsed.metadata().metadataFileLocation()).isEqualTo("metadataTestLocation");
    assertThat(UnregisterTableResponseParser.toJson(parsed)).isEqualTo(expectedJson);
  }
}
