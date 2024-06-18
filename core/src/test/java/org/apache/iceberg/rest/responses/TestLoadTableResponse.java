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

import static org.apache.iceberg.TestHelpers.assertSameSchemaList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.RequestResponseTestBase;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestLoadTableResponse extends RequestResponseTestBase<LoadTableResponse> {

  private static final String TEST_METADATA_LOCATION =
      "s3://bucket/test/location/metadata/v1.metadata.json";

  private static final String TEST_TABLE_LOCATION = "s3://bucket/test/location";

  private static final Schema SCHEMA_7 =
      new Schema(
          7,
          Types.NestedField.required(1, "x", Types.LongType.get()),
          Types.NestedField.required(2, "y", Types.LongType.get(), "comment"),
          Types.NestedField.required(3, "z", Types.LongType.get()));

  private static final PartitionSpec SPEC_5 =
      PartitionSpec.builderFor(SCHEMA_7).withSpecId(5).build();

  private static final SortOrder SORT_ORDER_3 =
      SortOrder.builderFor(SCHEMA_7)
          .withOrderId(3)
          .asc("y", NullOrder.NULLS_FIRST)
          .desc(Expressions.bucket("z", 4), NullOrder.NULLS_LAST)
          .build();

  private static final Map<String, String> TABLE_PROPS =
      ImmutableMap.of(
          "format-version", "1",
          "owner", "hank");

  private static final Map<String, String> CONFIG = ImmutableMap.of("foo", "bar");

  @Override
  public String[] allFieldsFromSpec() {
    return new String[] {"metadata-location", "metadata", "config"};
  }

  @Override
  public LoadTableResponse createExampleInstance() {
    TableMetadata metadata =
        TableMetadata.buildFrom(
                TableMetadata.newTableMetadata(
                    SCHEMA_7, SPEC_5, SORT_ORDER_3, TEST_TABLE_LOCATION, TABLE_PROPS))
            .discardChanges()
            .withMetadataLocation(TEST_METADATA_LOCATION)
            .build();

    return LoadTableResponse.builder().withTableMetadata(metadata).addAllConfig(CONFIG).build();
  }

  @Override
  public LoadTableResponse deserialize(String json) throws JsonProcessingException {
    LoadTableResponse resp = mapper().readValue(json, LoadTableResponse.class);
    resp.validate();
    return resp;
  }

  @Test
  public void testFailures() {
    assertThatThrownBy(() -> LoadTableResponse.builder().build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid metadata: null");
  }

  @Test
  public void testRoundTripSerdeWithV1TableMetadata() throws Exception {
    String tableMetadataJson = readTableMetadataInputFile("TableMetadataV1Valid.json");
    TableMetadata v1Metadata =
        TableMetadataParser.fromJson(TEST_METADATA_LOCATION, tableMetadataJson);
    // Convert the TableMetadata JSON from the file to an object and then back to JSON so that
    // missing fields
    // are filled in with their default values.
    String json =
        String.format(
            "{\"metadata-location\":\"%s\",\"metadata\":%s,\"config\":{\"foo\":\"bar\"}}",
            TEST_METADATA_LOCATION, TableMetadataParser.toJson(v1Metadata));
    LoadTableResponse resp =
        LoadTableResponse.builder().withTableMetadata(v1Metadata).addAllConfig(CONFIG).build();
    assertRoundTripSerializesEquallyFrom(json, resp);
  }

  @Test
  public void testMissingSchemaType() throws Exception {
    // When the schema type (struct) is missing
    String tableMetadataJson = readTableMetadataInputFile("TableMetadataV1MissingSchemaType.json");
    assertThatThrownBy(
            () -> TableMetadataParser.fromJson(TEST_METADATA_LOCATION, tableMetadataJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse type from json:");
  }

  @Test
  public void testRoundTripSerdeWithV2TableMetadata() throws Exception {
    String tableMetadataJson = readTableMetadataInputFile("TableMetadataV2Valid.json");
    TableMetadata v2Metadata =
        TableMetadataParser.fromJson(TEST_METADATA_LOCATION, tableMetadataJson);
    // Convert the TableMetadata JSON from the file to an object and then back to JSON so that
    // missing fields
    // are filled in with their default values.
    String json =
        String.format(
            "{\"metadata-location\":\"%s\",\"metadata\":%s,\"config\":{\"foo\":\"bar\"}}",
            TEST_METADATA_LOCATION, TableMetadataParser.toJson(v2Metadata));
    LoadTableResponse resp =
        LoadTableResponse.builder().withTableMetadata(v2Metadata).addAllConfig(CONFIG).build();
    assertRoundTripSerializesEquallyFrom(json, resp);
  }

  @Test
  public void testCanDeserializeWithoutDefaultValues() throws Exception {
    String metadataJson = readTableMetadataInputFile("TableMetadataV1Valid.json");
    // `config` is missing in the JSON
    String json =
        String.format(
            "{\"metadata-location\":\"%s\",\"metadata\":%s}", TEST_METADATA_LOCATION, metadataJson);
    TableMetadata metadata = TableMetadataParser.fromJson(TEST_METADATA_LOCATION, metadataJson);
    LoadTableResponse actual = deserialize(json);
    LoadTableResponse expected = LoadTableResponse.builder().withTableMetadata(metadata).build();
    assertEquals(actual, expected);
    assertThat(actual.config())
        .as("Deserialized JSON with missing fields should have the default values")
        .isEqualTo(ImmutableMap.of());
  }

  @Override
  public void assertEquals(LoadTableResponse actual, LoadTableResponse expected) {
    assertThat(actual.config())
        .as("Should have the same configuration")
        .isEqualTo(expected.config());
    assertEqualTableMetadata(actual.tableMetadata(), expected.tableMetadata());
    assertThat(actual.metadataLocation())
        .as("Should have the same metadata location")
        .isEqualTo(expected.metadataLocation());
  }

  private void assertEqualTableMetadata(TableMetadata actual, TableMetadata expected) {
    assertThat(actual.formatVersion())
        .as("Format version should match")
        .isEqualTo(expected.formatVersion());
    assertThat(actual.uuid()).as("Table UUID should match").isEqualTo(expected.uuid());
    assertThat(actual.location()).as("Table location should match").isEqualTo(expected.location());
    assertThat(actual.lastColumnId()).as("Last column id").isEqualTo(expected.lastColumnId());
    assertThat(actual.schema().asStruct())
        .as("Schema should match")
        .isEqualTo(expected.schema().asStruct());
    assertSameSchemaList(expected.schemas(), actual.schemas());
    assertThat(actual.currentSchemaId())
        .as("Current schema id should match")
        .isEqualTo(expected.currentSchemaId());
    assertThat(actual.schema().asStruct())
        .as("Schema should match")
        .isEqualTo(expected.schema().asStruct());
    assertThat(actual.lastSequenceNumber())
        .as("Last sequence number should match")
        .isEqualTo(expected.lastSequenceNumber());
    assertThat(actual.spec().toString())
        .as("Partition spec should match")
        .isEqualTo(expected.spec().toString());
    assertThat(actual.defaultSpecId())
        .as("Default spec ID should match")
        .isEqualTo(expected.defaultSpecId());
    assertThat(actual.specs()).as("PartitionSpec map should match").isEqualTo(expected.specs());
    assertThat(actual.defaultSortOrderId())
        .as("Default Sort ID should match")
        .isEqualTo(expected.defaultSortOrderId());
    assertThat(actual.sortOrder()).as("Sort order should match").isEqualTo(expected.sortOrder());
    assertThat(actual.sortOrders())
        .as("Sort order map should match")
        .isEqualTo(expected.sortOrders());
    assertThat(actual.properties()).as("Properties should match").isEqualTo(expected.properties());
    assertThat(Lists.transform(actual.snapshots(), Snapshot::snapshotId))
        .as("Snapshots should match")
        .isEqualTo(Lists.transform(expected.snapshots(), Snapshot::snapshotId));
    assertThat(actual.snapshotLog()).as("History should match").isEqualTo(expected.snapshotLog());
    Snapshot expectedCurrentSnapshot = expected.currentSnapshot();
    Snapshot actualCurrentSnapshot = actual.currentSnapshot();
    assertThat(
            expectedCurrentSnapshot != null && actualCurrentSnapshot != null
                || expectedCurrentSnapshot == null && actualCurrentSnapshot == null)
        .as("Both expected and actual current snapshot should either be null or non-null")
        .isTrue();
    if (expectedCurrentSnapshot != null) {
      assertThat(actual.currentSnapshot().snapshotId())
          .as("Current snapshot ID should match")
          .isEqualTo(expected.currentSnapshot().snapshotId());
      assertThat(actual.currentSnapshot().parentId())
          .as("Parent snapshot ID should match")
          .isEqualTo(expected.currentSnapshot().parentId());
      assertThat(actual.currentSnapshot().schemaId())
          .as("Schema ID for current snapshot should match")
          .isEqualTo(expected.currentSnapshot().schemaId());
    }
    assertThat(actual.metadataFileLocation())
        .as("Metadata file location should match")
        .isEqualTo(expected.metadataFileLocation());
    assertThat(actual.lastColumnId())
        .as("Last column id should match")
        .isEqualTo(expected.lastColumnId());
    assertThat(actual.schema().asStruct())
        .as("Schema should match")
        .isEqualTo(expected.schema().asStruct());
    assertSameSchemaList(expected.schemas(), actual.schemas());
    assertThat(actual.currentSchemaId())
        .as("Current schema id should match")
        .isEqualTo(expected.currentSchemaId());
    assertThat(actual.refs()).as("Refs map should match").isEqualTo(expected.refs());
  }

  private String readTableMetadataInputFile(String fileName) throws Exception {
    Path path = Paths.get(getClass().getClassLoader().getResource(fileName).toURI());
    return String.join("", java.nio.file.Files.readAllLines(path));
  }
}
