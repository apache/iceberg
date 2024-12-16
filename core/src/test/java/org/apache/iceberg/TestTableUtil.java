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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestTableUtil {
  private static final Namespace NS = Namespace.of("ns");
  private static final TableIdentifier IDENTIFIER = TableIdentifier.of(NS, "test");

  @TempDir private File tmp;

  private InMemoryCatalog catalog;

  @BeforeEach
  public void initCatalog() {
    catalog = new InMemoryCatalog();
    catalog.initialize("catalog", ImmutableMap.of());
    catalog.createNamespace(NS);
  }

  @Test
  public void nullTable() {
    assertThatThrownBy(() -> TableUtil.formatVersion(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table: null");
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3})
  public void formatVersionForBaseTable(int formatVersion) {
    Table table =
        catalog.createTable(
            IDENTIFIER,
            new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())),
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)));

    assertThat(TableUtil.formatVersion(table)).isEqualTo(formatVersion);
    assertThat(TableUtil.formatVersion(SerializableTable.copyOf(table))).isEqualTo(formatVersion);
  }

  @Test
  public void formatVersionForMetadataTables() {
    Table table =
        catalog.createTable(
            IDENTIFIER, new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())));

    for (MetadataTableType type : MetadataTableType.values()) {
      Table metadataTable = MetadataTableUtils.createMetadataTableInstance(table, type);
      assertThatThrownBy(() -> TableUtil.formatVersion(metadataTable))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(
              "%s does not have a format version", metadataTable.getClass().getSimpleName());

      assertThatThrownBy(() -> TableUtil.formatVersion(SerializableTable.copyOf(metadataTable)))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessage(
              "%s does not have a format version",
              SerializableTable.SerializableMetadataTable.class.getName());
    }
  }
}
