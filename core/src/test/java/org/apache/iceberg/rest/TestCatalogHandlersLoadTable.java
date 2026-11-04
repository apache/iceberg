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
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.UnsupportedFormatVersionException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTCatalogProperties.SnapshotMode;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestCatalogHandlersLoadTable {
  private static final TableIdentifier TABLE = TableIdentifier.of(Namespace.of("db"), "t");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "x", Types.LongType.get()));

  @TempDir private Path temp;

  private InMemoryCatalog catalog;

  @BeforeEach
  void before() {
    this.catalog = new InMemoryCatalog();
    catalog.initialize(
        "in-memory",
        ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, temp.toUri().toString()));
    catalog.createNamespace(TABLE.namespace());
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3})
  void loadTableReturnsTablesBeforeFormatVersion4(int formatVersion) {
    createTable(formatVersion);

    assertThat(
            CatalogHandlers.loadTable(catalog, TABLE, SnapshotMode.ALL)
                .tableMetadata()
                .formatVersion())
        .isEqualTo(formatVersion);
  }

  @Test
  void loadTableRejectsFormatVersion4() {
    createTable(4);

    assertThatThrownBy(() -> CatalogHandlers.loadTable(catalog, TABLE, SnapshotMode.ALL))
        .isInstanceOf(UnsupportedFormatVersionException.class)
        .hasMessage(
            "Cannot load table db.t with format version 4 using the v1 endpoint; use "
                + Endpoint.V2_LOAD_TABLE);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4})
  void loadTableV2ReturnsAllFormatVersions(int formatVersion) {
    createTable(formatVersion);

    assertThat(
            CatalogHandlers.loadTableV2(catalog, TABLE, SnapshotMode.REFS)
                .tableMetadata()
                .formatVersion())
        .isEqualTo(formatVersion);
  }

  private void createTable(int formatVersion) {
    catalog
        .buildTable(TABLE, SCHEMA)
        .withProperty(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
        .create();
  }
}
