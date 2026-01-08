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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestViewVersionParser {

  @Test
  public void testParseViewVersion() {
    SQLViewRepresentation firstRepresentation =
        ImmutableSQLViewRepresentation.builder()
            .sql("select * from foo")
            .dialect("spark-sql")
            .build();
    SQLViewRepresentation secondRepresentation =
        ImmutableSQLViewRepresentation.builder()
            .sql("select a, b, c from foo")
            .dialect("some-sql")
            .build();

    ViewVersion expectedViewVersion =
        ImmutableViewVersion.builder()
            .versionId(1)
            .timestampMillis(12345)
            .defaultNamespace(Namespace.of("one", "two"))
            .addRepresentations(firstRepresentation, secondRepresentation)
            .summary(ImmutableMap.of("user", "some-user"))
            .schemaId(1)
            .build();

    String serializedRepresentations =
        "[{\"type\":\"sql\", \"sql\":\"select * from foo\", \"dialect\":\"spark-sql\"}, "
            + "{\"type\":\"sql\", \"sql\":\"select a, b, c from foo\", \"dialect\":\"some-sql\"}]";

    String serializedViewVersion =
        String.format(
            "{\"version-id\":1, \"timestamp-ms\":12345, \"schema-id\":1, \"summary\":{\"user\":\"some-user\"}, \"representations\":%s, \"default-namespace\":[\"one\",\"two\"]}",
            serializedRepresentations);

    assertThat(ViewVersionParser.fromJson(serializedViewVersion))
        .as("Should be able to parse valid view version")
        .isEqualTo(expectedViewVersion);
  }

  @Test
  public void testSerializeViewVersion() {
    SQLViewRepresentation firstRepresentation =
        ImmutableSQLViewRepresentation.builder()
            .sql("select * from foo")
            .dialect("spark-sql")
            .build();
    SQLViewRepresentation secondRepresentation =
        ImmutableSQLViewRepresentation.builder()
            .sql("select a, b, c from foo")
            .dialect("some-sql")
            .build();

    ViewVersion viewVersion =
        ImmutableViewVersion.builder()
            .versionId(1)
            .timestampMillis(12345)
            .addRepresentations(firstRepresentation, secondRepresentation)
            .summary(ImmutableMap.of("user", "some-user"))
            .defaultNamespace(Namespace.of("one", "two"))
            .defaultCatalog("catalog")
            .schemaId(1)
            .build();

    String expectedRepresentations =
        "[{\"type\":\"sql\",\"sql\":\"select * from foo\",\"dialect\":\"spark-sql\"},"
            + "{\"type\":\"sql\",\"sql\":\"select a, b, c from foo\",\"dialect\":\"some-sql\"}]";

    String expectedViewVersion =
        String.format(
            "{\"version-id\":1,\"timestamp-ms\":12345,\"schema-id\":1,\"summary\":{\"user\":\"some-user\"},"
                + "\"default-catalog\":\"catalog\",\"default-namespace\":[\"one\",\"two\"],\"representations\":%s}",
            expectedRepresentations);

    assertThat(ViewVersionParser.toJson(viewVersion))
        .as("Should be able to serialize valid view version")
        .isEqualTo(expectedViewVersion);
  }

  @Test
  public void testNullViewVersion() {
    assertThatThrownBy(() -> ViewVersionParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot serialize null view version");

    assertThatThrownBy(() -> ViewVersionParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse view version from null object");

    assertThatThrownBy(() -> ViewVersionParser.fromJson((String) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse view version from null string");
  }

  @Test
  public void missingDefaultCatalog() {
    assertThatThrownBy(
            () ->
                ViewVersionParser.fromJson(
                    "{\"version-id\":1,\"timestamp-ms\":12345,\"schema-id\":1,"
                        + "\"summary\":{\"operation\":\"create\"},\"representations\":[]}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: default-namespace");
  }

  @Test
  public void invalidRepresentations() {
    String invalidRepresentations =
        "{\"version-id\":1, \"timestamp-ms\":12345, \"schema-id\":1, \"summary\":{\"user\":\"some-user\"}, \"representations\": 23, \"default-namespace\":[\"one\",\"two\"]}";
    assertThatThrownBy(() -> ViewVersionParser.fromJson(invalidRepresentations))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse representations from non-array: 23");
  }

  @Test
  public void missingRepresentations() {
    String missingRepresentations =
        "{\"version-id\":1, \"timestamp-ms\":12345, \"schema-id\":1, \"summary\":{\"user\":\"some-user\"}, \"default-namespace\":[\"one\",\"two\"]}";
    assertThatThrownBy(() -> ViewVersionParser.fromJson(missingRepresentations))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: representations");
  }

  @Test
  public void testParseViewVersionWithStorageTable() {
    SQLViewRepresentation representation =
        ImmutableSQLViewRepresentation.builder()
            .sql("select * from events")
            .dialect("spark-sql")
            .build();

    TableIdentifier storageTable = TableIdentifier.of(Namespace.of("db", "schema"), "mv_storage");

    ViewVersion expectedViewVersion =
        ImmutableViewVersion.builder()
            .versionId(1)
            .timestampMillis(12345)
            .defaultNamespace(Namespace.of("default"))
            .addRepresentations(representation)
            .summary(ImmutableMap.of("user", "mv-user"))
            .schemaId(1)
            .storageTable(storageTable)
            .build();

    String serializedViewVersion =
        "{\"version-id\":1, \"timestamp-ms\":12345, \"schema-id\":1, "
            + "\"summary\":{\"user\":\"mv-user\"}, "
            + "\"default-namespace\":[\"default\"], "
            + "\"representations\":[{\"type\":\"sql\", \"sql\":\"select * from events\", \"dialect\":\"spark-sql\"}], "
            + "\"storage-table\":{\"namespace\":[\"db\",\"schema\"], \"name\":\"mv_storage\"}}";

    ViewVersion parsedVersion = ViewVersionParser.fromJson(serializedViewVersion);

    assertThat(parsedVersion)
        .as("Should be able to parse view version with storage table")
        .isEqualTo(expectedViewVersion);

    assertThat(parsedVersion.storageTable()).as("Storage table should not be null").isNotNull();
    assertThat(parsedVersion.storageTable().namespace())
        .as("Storage table namespace should match")
        .isEqualTo(Namespace.of("db", "schema"));
    assertThat(parsedVersion.storageTable().name())
        .as("Storage table name should match")
        .isEqualTo("mv_storage");
  }

  @Test
  public void testSerializeViewVersionWithStorageTable() {
    SQLViewRepresentation representation =
        ImmutableSQLViewRepresentation.builder()
            .sql("select count(*) from events")
            .dialect("spark-sql")
            .build();

    TableIdentifier storageTable =
        TableIdentifier.of(Namespace.of("warehouse", "mvs"), "event_counts_storage");

    ViewVersion viewVersion =
        ImmutableViewVersion.builder()
            .versionId(2)
            .timestampMillis(67890)
            .addRepresentations(representation)
            .summary(ImmutableMap.of("operation", "create_mv"))
            .defaultNamespace(Namespace.of("default"))
            .schemaId(3)
            .storageTable(storageTable)
            .build();

    String expectedJson =
        "{\"version-id\":2,\"timestamp-ms\":67890,\"schema-id\":3,"
            + "\"summary\":{\"operation\":\"create_mv\"},"
            + "\"default-namespace\":[\"default\"],"
            + "\"representations\":[{\"type\":\"sql\",\"sql\":\"select count(*) from events\",\"dialect\":\"spark-sql\"}],"
            + "\"storage-table\":{\"namespace\":[\"warehouse\",\"mvs\"],\"name\":\"event_counts_storage\"}}";

    assertThat(ViewVersionParser.toJson(viewVersion))
        .as("Should be able to serialize view version with storage table")
        .isEqualTo(expectedJson);
  }

  @Test
  public void testNullStorageTable() {
    SQLViewRepresentation representation =
        ImmutableSQLViewRepresentation.builder()
            .sql("select * from data")
            .dialect("spark-sql")
            .build();

    ViewVersion viewVersion =
        ImmutableViewVersion.builder()
            .versionId(1)
            .timestampMillis(12345)
            .defaultNamespace(Namespace.of("default"))
            .addRepresentations(representation)
            .summary(ImmutableMap.of("user", "test"))
            .schemaId(1)
            .storageTable(null)
            .build();

    assertThat(viewVersion.storageTable())
        .as("Storage table should be null for regular views")
        .isNull();

    String json = ViewVersionParser.toJson(viewVersion);
    assertThat(json)
        .as("JSON should not contain storage-table field when null")
        .doesNotContain("storage-table");

    ViewVersion parsedVersion = ViewVersionParser.fromJson(json);
    assertThat(parsedVersion.storageTable()).as("Parsed storage table should remain null").isNull();
  }

  @Test
  public void testStorageTableRoundtrip() {
    SQLViewRepresentation representation =
        ImmutableSQLViewRepresentation.builder()
            .sql("select sum(value) from transactions")
            .dialect("spark-sql")
            .build();

    TableIdentifier storageTable =
        TableIdentifier.of(Namespace.of("analytics", "materialized"), "transaction_sums");

    ViewVersion originalVersion =
        ImmutableViewVersion.builder()
            .versionId(5)
            .timestampMillis(99999)
            .defaultNamespace(Namespace.of("prod"))
            .defaultCatalog("iceberg_catalog")
            .addRepresentations(representation)
            .summary(ImmutableMap.of("user", "admin", "operation", "refresh"))
            .schemaId(7)
            .storageTable(storageTable)
            .build();

    String json = ViewVersionParser.toJson(originalVersion);
    ViewVersion parsedVersion = ViewVersionParser.fromJson(json);

    assertThat(parsedVersion).as("Roundtrip should preserve all fields").isEqualTo(originalVersion);

    assertThat(parsedVersion.storageTable())
        .as("Storage table should be preserved in roundtrip")
        .isNotNull()
        .isEqualTo(storageTable);

    assertThat(parsedVersion.storageTable().namespace())
        .as("Storage table namespace should match after roundtrip")
        .isEqualTo(Namespace.of("analytics", "materialized"));

    assertThat(parsedVersion.storageTable().name())
        .as("Storage table name should match after roundtrip")
        .isEqualTo("transaction_sums");
  }

  @Test
  public void testBackwardsCompatibilityWithoutStorageTable() {
    // Test that view versions without storage-table field parse correctly
    String legacyViewVersion =
        "{\"version-id\":1, \"timestamp-ms\":12345, \"schema-id\":1, "
            + "\"summary\":{\"user\":\"legacy-user\"}, "
            + "\"default-namespace\":[\"default\"], "
            + "\"representations\":[{\"type\":\"sql\", \"sql\":\"select * from old_view\", \"dialect\":\"spark-sql\"}]}";

    ViewVersion parsedVersion = ViewVersionParser.fromJson(legacyViewVersion);

    assertThat(parsedVersion)
        .as("Should parse legacy view version without storage-table")
        .isNotNull();

    assertThat(parsedVersion.storageTable())
        .as("Storage table should be null for legacy views")
        .isNull();

    assertThat(parsedVersion.versionId()).as("Version ID should be parsed").isEqualTo(1);
    assertThat(parsedVersion.schemaId()).as("Schema ID should be parsed").isEqualTo(1);
  }
}
