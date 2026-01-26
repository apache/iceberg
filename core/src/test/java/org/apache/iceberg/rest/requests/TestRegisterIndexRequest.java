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
import org.junit.jupiter.api.Test;

public class TestRegisterIndexRequest {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> RegisterIndexRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid register index request: null");

    assertThatThrownBy(() -> RegisterIndexRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse register index request from null object");
  }

  @Test
  public void missingRequiredFields() {
    assertThatThrownBy(() -> RegisterIndexRequestParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: name");

    String missingMetadataLocation = "{\"name\":\"my_index\"}";
    assertThatThrownBy(() -> RegisterIndexRequestParser.fromJson(missingMetadataLocation))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: metadata-location");
  }

  @Test
  public void roundTripSerde() {
    RegisterIndexRequest request =
        ImmutableRegisterIndexRequest.builder()
            .name("customer_id_idx")
            .metadataLocation("s3://bucket/indexes/customer_id_idx/metadata/v1.metadata.json")
            .build();

    String json = RegisterIndexRequestParser.toJson(request);
    RegisterIndexRequest parsed = RegisterIndexRequestParser.fromJson(json);

    assertThat(parsed.name()).isEqualTo("customer_id_idx");
    assertThat(parsed.metadataLocation())
        .isEqualTo("s3://bucket/indexes/customer_id_idx/metadata/v1.metadata.json");
  }

  @Test
  public void testToJsonWithExpectedString() {
    RegisterIndexRequest request =
        ImmutableRegisterIndexRequest.builder()
            .name("my_btree_idx")
            .metadataLocation("s3://bucket/indexes/my_btree_idx/metadata/v1.metadata.json")
            .build();

    String expectedJson =
        """
        {
          "name": "my_btree_idx",
          "metadata-location": "s3://bucket/indexes/my_btree_idx/metadata/v1.metadata.json"
        }
        """
            .replaceAll("\\s+", "");

    String actualJson = RegisterIndexRequestParser.toJson(request);
    assertThat(actualJson).isEqualTo(expectedJson);

    // Verify round-trip
    RegisterIndexRequest parsed = RegisterIndexRequestParser.fromJson(actualJson);
    assertThat(parsed.name()).isEqualTo("my_btree_idx");
    assertThat(parsed.metadataLocation())
        .isEqualTo("s3://bucket/indexes/my_btree_idx/metadata/v1.metadata.json");
  }

  @Test
  public void testBuilderValidation() {
    assertThatThrownBy(() -> ImmutableRegisterIndexRequest.builder().build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid index name: null or empty");

    assertThatThrownBy(() -> ImmutableRegisterIndexRequest.builder().name("").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid index name: null or empty");

    assertThatThrownBy(() -> ImmutableRegisterIndexRequest.builder().name("test").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid metadata location: null or empty");

    assertThatThrownBy(
            () -> ImmutableRegisterIndexRequest.builder().name("test").metadataLocation("").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid metadata location: null or empty");
  }

  @Test
  public void testPrettyJsonOutput() {
    RegisterIndexRequest request =
        ImmutableRegisterIndexRequest.builder()
            .name("test_idx")
            .metadataLocation("s3://bucket/metadata.json")
            .build();

    String prettyJson = RegisterIndexRequestParser.toJson(request, true);
    String compactJson = RegisterIndexRequestParser.toJson(request, false);

    // Pretty JSON should contain newlines
    assertThat(prettyJson).contains("\n");
    // Compact JSON should not contain newlines
    assertThat(compactJson).doesNotContain("\n");

    // Both should parse to equivalent requests
    RegisterIndexRequest fromPretty = RegisterIndexRequestParser.fromJson(prettyJson);
    RegisterIndexRequest fromCompact = RegisterIndexRequestParser.fromJson(compactJson);

    assertThat(fromPretty.name()).isEqualTo(fromCompact.name());
    assertThat(fromPretty.metadataLocation()).isEqualTo(fromCompact.metadataLocation());
  }
}
