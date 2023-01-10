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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestViewMetadata {

  private static ViewVersion version1 =
      ImmutableViewVersion.builder()
          .versionId(1)
          .timestampMillis(4353L)
          .summary(ImmutableMap.of("operation", "create"))
          .schemaId(1)
          .addRepresentations(
              ImmutableSQLViewRepresentation.builder()
                  .sql("select 'foo' foo")
                  .dialect("spark-sql")
                  .defaultCatalog("some-catalog")
                  .build())
          .build();

  private static ViewHistoryEntry historyEntry1 =
      ImmutableViewHistoryEntry.builder().timestampMillis(4353L).versionId(1).build();

  private static ViewVersion version2 =
      ImmutableViewVersion.builder()
          .versionId(2)
          .schemaId(1)
          .timestampMillis(5555L)
          .summary(ImmutableMap.of("operation", "replace"))
          .addRepresentations(
              ImmutableSQLViewRepresentation.builder()
                  .sql("select 1 id, 'abc' data")
                  .defaultCatalog("some-catalog")
                  .dialect("spark-sql")
                  .build())
          .build();

  private static ViewHistoryEntry historyEntry2 =
      ImmutableViewHistoryEntry.builder().timestampMillis(5555L).versionId(2).build();

  private static final Schema TEST_SCHEMA =
      new Schema(
          1,
          Types.NestedField.required(1, "x", Types.LongType.get()),
          Types.NestedField.required(2, "y", Types.LongType.get(), "comment"),
          Types.NestedField.required(3, "z", Types.LongType.get()));

  @Parameterized.Parameters
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][] {});
  }

  @Test
  public void testReadAndWriteValidViewMetadata() throws Exception {
    String json = readViewMetadataInputFile("view/ValidViewMetadata.json");
    ViewMetadata expectedViewMetadata =
        ImmutableViewMetadata.builder()
            .currentSchemaId(1)
            .schemas(ImmutableList.of(TEST_SCHEMA))
            .versions(ImmutableList.of(version1, version2))
            .history(ImmutableList.of(historyEntry1, historyEntry2))
            .location("s3://bucket/test/location")
            .properties(ImmutableMap.of("some-key", "some-value"))
            .currentVersionId(2)
            .formatVersion(1)
            .build();
    assertSameViewMetadata(expectedViewMetadata, ViewMetadataParser.fromJson(json));
    assertSameViewMetadata(
        expectedViewMetadata,
        ViewMetadataParser.fromJson(ViewMetadataParser.toJson(expectedViewMetadata)));
  }

  @Test
  public void testReadViewMetadataWithLimitedNumberVersionEntries() throws Exception {
    String json = readViewMetadataInputFile("view/ViewMetadataLimitedVersions.json");

    ViewMetadata viewMetadata = ViewMetadataParser.fromJson(json);

    Assert.assertEquals(1, viewMetadata.versions().size());
    Assert.assertEquals(1, viewMetadata.history().size());
  }

  @Test
  public void testFailReadingViewMetadataMissingLocation() throws Exception {
    String json = readViewMetadataInputFile("view/ViewMetadataMissingLocation.json");
    Assertions.assertThatThrownBy(() -> ViewMetadataParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse missing string: location");
  }

  @Test
  public void testFailReadingViewMetadataMissingCurrentSchema() throws Exception {
    String json = readViewMetadataInputFile("view/ViewMetadataMissingCurrentSchema.json");
    Assertions.assertThatThrownBy(() -> ViewMetadataParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse missing int: current-schema-id");
  }

  @Test
  public void testFailReadingViewMetadataInvalidSchema() throws Exception {
    String json = readViewMetadataInputFile("view/ViewMetadataInvalidCurrentSchema.json");
    Assertions.assertThatThrownBy(() -> ViewMetadataParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find schema with current-schema-id=1234 from schemas");
  }

  @Test
  public void testFailReadingViewMetadataMissingVersion() throws Exception {
    String json = readViewMetadataInputFile("view/ViewMetadataMissingCurrentVersion.json");
    Assertions.assertThatThrownBy(() -> ViewMetadataParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse missing int: current-version-id");
  }

  @Test
  public void testFailReadingViewMetadataInvalidVersion() throws Exception {
    String json = readViewMetadataInputFile("view/ViewMetadataMissingCurrentVersion.json");
    Assertions.assertThatThrownBy(() -> ViewMetadataParser.fromJson(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse missing int: current-version-id");
  }

  @Test
  public void testFailReadingNullViewMetadata() {
    Assertions.assertThatThrownBy(() -> ViewMetadataParser.fromJson((String) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse view metadata from null string");
  }

  private void assertSameViewMetadata(ViewMetadata expected, ViewMetadata actual) {
    Assert.assertEquals(expected.currentSchemaId(), actual.currentSchemaId());
    Assert.assertEquals(expected.versions(), actual.versions());
    Assert.assertEquals(expected.properties(), actual.properties());
    Assert.assertEquals(expected.location(), actual.location());
    Assert.assertEquals(expected.history(), actual.history());
    for (Schema schema : expected.schemas()) {
      Assert.assertTrue(schema.sameSchema(actual.schemasById().get(schema.schemaId())));
    }
  }

  private String readViewMetadataInputFile(String fileName) throws Exception {
    Path path = Paths.get(getClass().getClassLoader().getResource(fileName).toURI());
    return String.join("", java.nio.file.Files.readAllLines(path));
  }
}
