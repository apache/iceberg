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

import java.nio.file.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadTableResponseParser;
import org.apache.iceberg.rest.responses.UnregisterTableResponse;
import org.apache.iceberg.rest.responses.UnregisterTableResponseParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

class TestCatalogHandlersTableLocation {

  private static final Namespace NS = Namespace.of("db");

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
    catalog.createNamespace(NS);
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#ALL_VERSIONS")
  void loadTableResponseCarriesTableLocationOnlyForV4(int formatVersion) {
    TableIdentifier ident = createTable(formatVersion, "load");

    LoadTableResponse response =
        CatalogHandlers.loadTable(catalog, ident, RESTCatalogProperties.SnapshotMode.ALL);

    assertLoadTableLocation(response, formatVersion, catalog.loadTable(ident).location());
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#ALL_VERSIONS")
  void unregisterTableResponseCarriesTableLocationOnlyForV4(int formatVersion) {
    TableIdentifier ident = createTable(formatVersion, "unregister");
    String tableLocation = catalog.loadTable(ident).location();

    UnregisterTableResponse response = CatalogHandlers.unregisterTable(catalog, ident);

    if (formatVersion >= 4) {
      assertThat(response.tableLocation()).isEqualTo(tableLocation);
      assertThat(UnregisterTableResponseParser.toJson(response)).contains("table-location");
    } else {
      assertThat(response.tableLocation()).isNull();
      assertThat(UnregisterTableResponseParser.toJson(response)).doesNotContain("table-location");
    }
  }

  private TableIdentifier createTable(int formatVersion, String name) {
    TableIdentifier ident = TableIdentifier.of(NS, name + "_v" + formatVersion);
    catalog
        .buildTable(ident, SCHEMA)
        .withProperty(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
        .create();
    return ident;
  }

  private static void assertLoadTableLocation(
      LoadTableResponse response, int formatVersion, String tableLocation) {
    if (formatVersion >= 4) {
      assertThat(response.tableLocation()).isEqualTo(tableLocation);
      assertThat(LoadTableResponseParser.toJson(response)).contains("table-location");
    } else {
      assertThat(response.tableLocation()).isNull();
      assertThat(LoadTableResponseParser.toJson(response)).doesNotContain("table-location");
    }
  }
}
