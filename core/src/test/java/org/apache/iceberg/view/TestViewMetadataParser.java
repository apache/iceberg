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
package org.apache.iceberg.view;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestViewMetadataParser {

  private static final Schema TEST_SCHEMA =
      new Schema(
          1,
          Types.NestedField.required(1, "x", Types.LongType.get()),
          Types.NestedField.required(2, "y", Types.LongType.get(), "comment"),
          Types.NestedField.required(3, "z", Types.LongType.get()));

  @Test
  public void nullAndEmptyCheck() {
    assertThatThrownBy(() -> ViewMetadataParser.fromJson((String) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse view metadata from null string");

    assertThatThrownBy(() -> ViewMetadataParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse view metadata from null object");

    assertThatThrownBy(() -> ViewMetadataParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid view metadata: null");
  }

  @Test
  public void readAndWriteValidViewMetadata() throws Exception {
    ViewVersion version1 =
        ImmutableViewVersion.builder()
            .versionId(1)
            .timestampMillis(4353L)
            .summary(ImmutableMap.of("operation", "create"))
            .schemaId(1)
            .defaultCatalog("some-catalog")
            .defaultNamespace(Namespace.empty())
            .addRepresentations(
                ImmutableSQLViewRepresentation.builder()
                    .sql("select 'foo' foo")
                    .dialect("spark-sql")
                    .build())
            .build();

    ViewVersion version2 =
        ImmutableViewVersion.builder()
            .versionId(2)
            .schemaId(1)
            .timestampMillis(5555L)
            .summary(ImmutableMap.of("operation", "replace"))
            .defaultCatalog("some-catalog")
            .defaultNamespace(Namespace.empty())
            .addRepresentations(
                ImmutableSQLViewRepresentation.builder()
                    .sql("select 1 id, 'abc' data")
                    .dialect("spark-sql")
                    .build())
            .build();

    String json = readViewMetadataInputFile("org/apache/iceberg/view/ValidViewMetadata.json");
    ViewMetadata expectedViewMetadata =
        ViewMetadata.builder()
            .assignUUID("fa6506c3-7681-40c8-86dc-e36561f83385")
            .addSchema(TEST_SCHEMA)
            .addVersion(version1)
            .addVersion(version2)
            .setLocation("s3://bucket/test/location")
            .setProperties(ImmutableMap.of("some-key", "some-value"))
            .setCurrentVersionId(2)
            .upgradeFormatVersion(1)
            .build();

    ViewMetadata actual = ViewMetadataParser.fromJson(json);
    assertThat(actual)
        .usingRecursiveComparison()
        .ignoringFieldsOfTypes(Schema.class)
        .ignoringFields("changes")
        .isEqualTo(expectedViewMetadata);
    for (Schema schema : expectedViewMetadata.schemas()) {
      assertThat(schema.sameSchema(actual.schemasById().get(schema.schemaId()))).isTrue();
    }

    actual = ViewMetadataParser.fromJson(ViewMetadataParser.toJson(expectedViewMetadata));
    assertThat(actual)
        .usingRecursiveComparison()
        .ignoringFieldsOfTypes(Schema.class)
        .ignoringFields("changes")
        .isEqualTo(expectedViewMetadata);
    for (Schema schema : expectedViewMetadata.schemas()) {
      assertThat(schema.sameSchema(actual.schemasById().get(schema.schemaId()))).isTrue();
    }
  }

  @Test
  public void failReadingViewMetadataMissingLocation() throws Exception {
    String json =
        readViewMetadataInputFile("org/apache/iceberg/view/ViewMetadataMissingLocation.json");
    assertThatThrownBy(() -> ViewMetadataParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: location");
  }

  @Test
  public void failReadingViewMetadataInvalidSchemaId() throws Exception {
    String json =
        readViewMetadataInputFile("org/apache/iceberg/view/ViewMetadataInvalidCurrentSchema.json");
    ViewMetadata metadata = ViewMetadataParser.fromJson(json);
    assertThatThrownBy(metadata::currentSchemaId)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find current schema with id 1234 in schemas: [1]");
  }

  @Test
  public void failReadingViewMetadataMissingVersion() throws Exception {
    String json =
        readViewMetadataInputFile("org/apache/iceberg/view/ViewMetadataMissingCurrentVersion.json");
    assertThatThrownBy(() -> ViewMetadataParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing int: current-version-id");
  }

  @Test
  public void failReadingViewMetadataInvalidVersionId() throws Exception {
    String json =
        readViewMetadataInputFile("org/apache/iceberg/view/ViewMetadataInvalidCurrentVersion.json");
    ViewMetadata metadata = ViewMetadataParser.fromJson(json);
    assertThatThrownBy(metadata::currentVersion)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find current version 1234 in view versions: [1, 2]");
  }

  private String readViewMetadataInputFile(String fileName) throws Exception {
    Path path = Paths.get(getClass().getClassLoader().getResource(fileName).toURI());
    return String.join("", java.nio.file.Files.readAllLines(path));
  }
}
