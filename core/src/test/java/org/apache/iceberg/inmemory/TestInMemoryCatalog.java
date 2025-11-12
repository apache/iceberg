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
package org.apache.iceberg.inmemory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestInMemoryCatalog extends CatalogTests<InMemoryCatalog> {
  private InMemoryCatalog catalog;

  @BeforeEach
  public void before() {
    this.catalog =
        initCatalog(
            "in-memory-catalog",
            ImmutableMap.<String, String>builder()
                .put(
                    CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key1", "catalog-default-key1")
                .put(
                    CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key2", "catalog-default-key2")
                .put(
                    CatalogProperties.TABLE_DEFAULT_PREFIX + "override-key3",
                    "catalog-default-key3")
                .put(
                    CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key3",
                    "catalog-override-key3")
                .put(
                    CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key4",
                    "catalog-override-key4")
                .buildOrThrow());
  }

  @Override
  protected InMemoryCatalog catalog() {
    return catalog;
  }

  @Override
  protected InMemoryCatalog initCatalog(
      String catalogName, Map<String, String> additionalProperties) {
    InMemoryCatalog cat = new InMemoryCatalog();
    cat.initialize(catalogName, additionalProperties);
    return cat;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsEmptyNamespace() {
    return true;
  }

  @Test
  @Override
  public void testLoadTableWithMissingMetadataFile(@TempDir Path tempDir) throws IOException {

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(TBL.namespace());
    }

    catalog.buildTable(TBL, SCHEMA).create();
    assertThat(catalog.tableExists(TBL)).as("Table should exist").isTrue();
    Table table = catalog.loadTable(TBL);
    String metadataFileLocation =
        ((HasTableOperations) table).operations().current().metadataFileLocation();
    table.io().deleteFile(metadataFileLocation);
    assertThatThrownBy(() -> catalog.loadTable(TBL))
        .isInstanceOf(NotFoundException.class)
        .hasMessage("No in-memory file found for location: " + metadataFileLocation);
    table.io().newOutputFile(metadataFileLocation).create();
  }
}
