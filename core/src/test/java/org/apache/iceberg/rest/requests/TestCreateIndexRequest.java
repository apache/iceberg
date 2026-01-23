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
import org.apache.iceberg.index.IndexType;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestCreateIndexRequest {

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> CreateIndexRequestParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid create index request: null");

    assertThatThrownBy(() -> CreateIndexRequestParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse create index request from null object");
  }

  @Test
  public void missingRequiredFields() {
    assertThatThrownBy(() -> CreateIndexRequestParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: name");

    String missingType = "{\"name\":\"my_index\"}";
    assertThatThrownBy(() -> CreateIndexRequestParser.fromJson(missingType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: type");

    String missingColumnIds =
        """
        {
          "name": "my_index",
          "type": "btree"
        }"""
            .replaceAll("\\s+", "");
    assertThatThrownBy(() -> CreateIndexRequestParser.fromJson(missingColumnIds))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing list: index-column-ids");
  }

  @Test
  public void roundTripSerde() {
    CreateIndexRequest request =
        CreateIndexRequest.builder()
            .withName("customer_id_idx")
            .withType(IndexType.BTREE)
            .withIndexColumnIds(ImmutableList.of(1, 2))
            .withOptimizedColumnIds(ImmutableList.of(1))
            .withLocation("s3://bucket/indexes/customer_id_idx")
            .setProperty("key1", "value1")
            .build();

    String json = CreateIndexRequestParser.toJson(request);
    CreateIndexRequest parsed = CreateIndexRequestParser.fromJson(json);

    assertThat(parsed.name()).isEqualTo("customer_id_idx");
    assertThat(parsed.type()).isEqualTo(IndexType.BTREE);
    assertThat(parsed.indexColumnIds()).containsExactly(1, 2);
    assertThat(parsed.optimizedColumnIds()).containsExactly(1);
    assertThat(parsed.location()).isEqualTo("s3://bucket/indexes/customer_id_idx");
    assertThat(parsed.properties()).containsEntry("key1", "value1");
  }

  @Test
  public void roundTripSerdeMinimal() {
    CreateIndexRequest request =
        CreateIndexRequest.builder()
            .withName("simple_idx")
            .withType(IndexType.BLOOM)
            .withIndexColumnIds(ImmutableList.of(5))
            .build();

    String json = CreateIndexRequestParser.toJson(request);
    CreateIndexRequest parsed = CreateIndexRequestParser.fromJson(json);

    assertThat(parsed.name()).isEqualTo("simple_idx");
    assertThat(parsed.type()).isEqualTo(IndexType.BLOOM);
    assertThat(parsed.indexColumnIds()).containsExactly(5);
    assertThat(parsed.optimizedColumnIds()).isEmpty();
    assertThat(parsed.location()).isNull();
    assertThat(parsed.properties()).isEmpty();
  }

  @Test
  public void testToJsonWithExpectedString() {
    CreateIndexRequest request =
        CreateIndexRequest.builder()
            .withName("customer_id_btree_idx")
            .withType(IndexType.BTREE)
            .withIndexColumnIds(ImmutableList.of(1, 2))
            .withOptimizedColumnIds(ImmutableList.of(1))
            .withLocation("s3://bucket/indexes")
            .setProperty("prop1", "value1")
            .build();

    String expectedJson =
        """
        {
          "name": "customer_id_btree_idx",
          "type": "btree",
          "index-column-ids": [1, 2],
          "optimized-column-ids": [1],
          "location": "s3://bucket/indexes",
          "properties": {
            "prop1": "value1"
          }
        }
        """
            .replaceAll("\\s+", "");

    String actualJson = CreateIndexRequestParser.toJson(request);
    assertThat(actualJson).isEqualTo(expectedJson);

    // Verify round-trip
    CreateIndexRequest parsed = CreateIndexRequestParser.fromJson(actualJson);
    assertThat(parsed.name()).isEqualTo("customer_id_btree_idx");
    assertThat(parsed.type()).isEqualTo(IndexType.BTREE);
    assertThat(parsed.indexColumnIds()).containsExactly(1, 2);
    assertThat(parsed.optimizedColumnIds()).containsExactly(1);
    assertThat(parsed.location()).isEqualTo("s3://bucket/indexes");
    assertThat(parsed.properties()).containsEntry("prop1", "value1");
  }

  @Test
  public void testToJsonMinimalWithExpectedString() {
    CreateIndexRequest request =
        CreateIndexRequest.builder()
            .withName("term_idx")
            .withType(IndexType.TERM)
            .withIndexColumnIds(ImmutableList.of(3))
            .build();

    String expectedJson =
        """
        {
          "name": "term_idx",
          "type": "term",
          "index-column-ids": [3]
        }
        """
            .replaceAll("\\s+", "");

    String actualJson = CreateIndexRequestParser.toJson(request);
    assertThat(actualJson).isEqualTo(expectedJson);
  }

  @Test
  public void testBuilderValidation() {
    assertThatThrownBy(() -> CreateIndexRequest.builder().build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid index name: null or empty");

    assertThatThrownBy(() -> CreateIndexRequest.builder().withName("test").build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid index type: null");

    assertThatThrownBy(
            () -> CreateIndexRequest.builder().withName("test").withType(IndexType.BTREE).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid index column IDs: null or empty");
  }

  @Test
  public void testAllIndexTypes() {
    for (IndexType indexType : IndexType.values()) {
      CreateIndexRequest request =
          CreateIndexRequest.builder()
              .withName("idx_" + indexType.typeName())
              .withType(indexType)
              .withIndexColumnIds(ImmutableList.of(1))
              .build();

      String json = CreateIndexRequestParser.toJson(request);
      CreateIndexRequest parsed = CreateIndexRequestParser.fromJson(json);

      assertThat(parsed.type()).isEqualTo(indexType);
    }
  }

  @Test
  public void testAddMethods() {
    CreateIndexRequest request =
        CreateIndexRequest.builder()
            .withName("test_idx")
            .withType(IndexType.IVF)
            .addIndexColumnId(1)
            .addIndexColumnId(2)
            .addOptimizedColumnId(1)
            .setProperty("a", "1")
            .setProperty("b", "2")
            .build();

    assertThat(request.indexColumnIds()).containsExactly(1, 2);
    assertThat(request.optimizedColumnIds()).containsExactly(1);
    assertThat(request.properties())
        .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of("a", "1", "b", "2"));
  }

  @Test
  public void testSetProperties() {
    CreateIndexRequest request =
        CreateIndexRequest.builder()
            .withName("test_idx")
            .withType(IndexType.BTREE)
            .withIndexColumnIds(ImmutableList.of(1))
            .setProperties(ImmutableMap.of("key1", "val1", "key2", "val2"))
            .build();

    assertThat(request.properties())
        .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of("key1", "val1", "key2", "val2"));
  }

  @Test
  public void testInvalidIndexType() {
    String jsonWithInvalidType =
        """
        {
          "name": "test_idx",
          "type": "invalid_type",
          "index-column-ids": [1]
        }"""
            .replaceAll("\\s+", "");

    assertThatThrownBy(() -> CreateIndexRequestParser.fromJson(jsonWithInvalidType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown index type: invalid_type");
  }
}
