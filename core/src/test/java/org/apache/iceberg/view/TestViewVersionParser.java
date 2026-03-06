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
    String serializedViewVersion =
        "{\"version-id\":1,\"timestamp-ms\":12345,\"schema-id\":1,"
            + "\"summary\":{\"user\":\"some-user\"},\"default-namespace\":[\"one\",\"two\"],"
            + "\"representations\":[],"
            + "\"storage-table\":{\"namespace\":[\"db\",\"schema\"],\"name\":\"mv_storage\"}}";

    ViewVersion viewVersion = ViewVersionParser.fromJson(serializedViewVersion);

    assertThat(viewVersion.storageTable()).isNotNull();
    assertThat(viewVersion.storageTable().namespace())
        .isEqualTo(Namespace.of("db", "schema"));
    assertThat(viewVersion.storageTable().name()).isEqualTo("mv_storage");
    assertThat(viewVersion.storageTable().catalog()).isNull();
    assertThat(viewVersion.storageTable().hasCatalog()).isFalse();
  }

  @Test
  public void testParseViewVersionWithStorageTableAndCatalog() {
    String serializedViewVersion =
        "{\"version-id\":1,\"timestamp-ms\":12345,\"schema-id\":1,"
            + "\"summary\":{\"user\":\"some-user\"},\"default-namespace\":[\"one\",\"two\"],"
            + "\"representations\":[],"
            + "\"storage-table\":{\"namespace\":[\"db\"],\"name\":\"mv_storage\",\"catalog\":\"my_catalog\"}}";

    ViewVersion viewVersion = ViewVersionParser.fromJson(serializedViewVersion);

    assertThat(viewVersion.storageTable()).isNotNull();
    assertThat(viewVersion.storageTable().namespace()).isEqualTo(Namespace.of("db"));
    assertThat(viewVersion.storageTable().name()).isEqualTo("mv_storage");
    assertThat(viewVersion.storageTable().catalog()).isEqualTo("my_catalog");
    assertThat(viewVersion.storageTable().hasCatalog()).isTrue();
  }

  @Test
  public void testSerializeViewVersionWithStorageTable() {
    ViewVersion viewVersion =
        ImmutableViewVersion.builder()
            .versionId(1)
            .timestampMillis(12345)
            .schemaId(1)
            .summary(ImmutableMap.of("user", "some-user"))
            .defaultNamespace(Namespace.of("one", "two"))
            .storageTable(StorageTableIdentifier.of(Namespace.of("db", "schema"), "mv_storage"))
            .build();

    String json = ViewVersionParser.toJson(viewVersion);

    assertThat(json).contains("\"storage-table\"");
    assertThat(json).contains("\"name\":\"mv_storage\"");
    assertThat(json).contains("\"db\",\"schema\"");
    assertThat(json).doesNotContain("\"catalog\"");
  }

  @Test
  public void testSerializeViewVersionWithStorageTableAndCatalog() {
    ViewVersion viewVersion =
        ImmutableViewVersion.builder()
            .versionId(1)
            .timestampMillis(12345)
            .schemaId(1)
            .summary(ImmutableMap.of("user", "some-user"))
            .defaultNamespace(Namespace.of("one", "two"))
            .storageTable(
                StorageTableIdentifier.of("my_catalog", Namespace.of("db"), "mv_storage"))
            .build();

    String json = ViewVersionParser.toJson(viewVersion);

    assertThat(json).contains("\"storage-table\"");
    assertThat(json).contains("\"name\":\"mv_storage\"");
    assertThat(json).contains("\"catalog\":\"my_catalog\"");
  }

  @Test
  public void testRoundTripStorageTable() {
    StorageTableIdentifier storageTable =
        StorageTableIdentifier.of("my_catalog", Namespace.of("db", "schema"), "mv_storage");

    ViewVersion original =
        ImmutableViewVersion.builder()
            .versionId(2)
            .timestampMillis(99999)
            .schemaId(1)
            .summary(ImmutableMap.of("engine", "spark"))
            .defaultNamespace(Namespace.of("default"))
            .storageTable(storageTable)
            .build();

    ViewVersion roundTripped = ViewVersionParser.fromJson(ViewVersionParser.toJson(original));

    assertThat(roundTripped.storageTable()).isEqualTo(storageTable);
    assertThat(roundTripped.storageTable().catalog()).isEqualTo("my_catalog");
    assertThat(roundTripped.storageTable().namespace()).isEqualTo(Namespace.of("db", "schema"));
    assertThat(roundTripped.storageTable().name()).isEqualTo("mv_storage");
  }

  @Test
  public void testNoStorageTableForPlainView() {
    String serializedViewVersion =
        "{\"version-id\":1,\"timestamp-ms\":12345,\"schema-id\":1,"
            + "\"summary\":{\"user\":\"some-user\"},\"default-namespace\":[\"one\",\"two\"],"
            + "\"representations\":[]}";

    ViewVersion viewVersion = ViewVersionParser.fromJson(serializedViewVersion);

    assertThat(viewVersion.storageTable()).isNull();
  }
}
