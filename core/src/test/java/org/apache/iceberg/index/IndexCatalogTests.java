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
package org.apache.iceberg.index;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.IndexCatalog;
import org.apache.iceberg.catalog.IndexIdentifier;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchIndexException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.LocationUtil;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public abstract class IndexCatalogTests<C extends IndexCatalog & SupportsNamespaces> {
  protected static final Schema SCHEMA =
      new Schema(
          5,
          required(3, "id", Types.IntegerType.get(), "unique ID"),
          required(4, "data", Types.StringType.get()));

  protected abstract C catalog();

  protected abstract Catalog tableCatalog();

  @TempDir private Path tempDir;

  protected String indexLocation(String... paths) {
    StringBuilder location =
        new StringBuilder(LocationUtil.stripTrailingSlash(tempDir.toFile().toURI().toString()));
    for (String path : paths) {
      location.append("/").append(path);
    }

    return location.toString();
  }

  protected boolean requiresNamespaceCreate() {
    return false;
  }

  protected boolean overridesRequestedLocation() {
    return false;
  }

  protected boolean supportsServerSideRetry() {
    return false;
  }

  @Test
  public void loadIndexWithNonExistingTable() {
    TableIdentifier tableIdentifier = TableIdentifier.of("non-existing", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "index");
    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();
    assertThatThrownBy(() -> catalog().loadIndex(indexIdentifier))
        .isInstanceOf(NoSuchIndexException.class)
        .hasMessageContaining("Index does not exist");
  }

  @Test
  public void loadNonExistingIndex() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "non_existing_index");
    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();
    assertThatThrownBy(() -> catalog().loadIndex(indexIdentifier))
        .isInstanceOf(NoSuchIndexException.class)
        .hasMessageContaining("Index does not exist");
  }

  @Test
  public void basicCreateIndex() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "test_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();

    Index index =
        catalog()
            .buildIndex(indexIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3)
            .withOptimizedColumnIds(3)
            .create();

    assertThat(index).isNotNull();
    assertThat(catalog().indexExists(indexIdentifier)).as("Index should exist").isTrue();

    // validate index settings
    assertThat(index.name()).isNotNull();
    assertThat(index.type()).isEqualTo(IndexType.BTREE);
    assertThat(index.indexColumnIds()).containsExactly(3);
    assertThat(index.optimizedColumnIds()).containsExactly(3);
    assertThat(index.history()).hasSize(1);
    assertThat(index.versions()).hasSize(1);

    assertThat(catalog().dropIndex(indexIdentifier)).isTrue();
    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();
  }

  @Test
  public void completeCreateIndex() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "complete_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();

    String location =
        indexLocation(
            tableIdentifier.namespace().toString(), tableIdentifier.name(), indexIdentifier.name());
    Index index =
        catalog()
            .buildIndex(indexIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3, 4)
            .withOptimizedColumnIds(3)
            .withProperty("prop1", "val1")
            .withProperty("prop2", "val2")
            .withLocation(location)
            .create();

    assertThat(index).isNotNull();
    assertThat(catalog().indexExists(indexIdentifier)).as("Index should exist").isTrue();

    if (!overridesRequestedLocation()) {
      assertThat(index.location()).isEqualTo(location);
    } else {
      assertThat(index.location()).isNotNull();
    }

    // validate index settings
    assertThat(index.uuid()).isNotNull();
    assertThat(index.name()).isNotNull();
    assertThat(index.type()).isEqualTo(IndexType.BTREE);
    assertThat(index.indexColumnIds()).containsExactly(3, 4);
    assertThat(index.optimizedColumnIds()).containsExactly(3);
    assertThat(index.history()).hasSize(1);
    assertThat(index.versions()).hasSize(1);

    assertThat(catalog().dropIndex(indexIdentifier)).isTrue();
    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();
  }

  @Test
  public void createIndexErrorCases() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "error_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();

    // type is required
    assertThatThrownBy(() -> catalog().buildIndex(indexIdentifier).withIndexColumnIds(3).create())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot create index without specifying a type");

    // index column ids are required
    assertThatThrownBy(
            () -> catalog().buildIndex(indexIdentifier).withType(IndexType.BTREE).create())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot create index without specifying index column ids");
  }

  @Test
  public void createIndexThatAlreadyExists() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "duplicate_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();

    Index index =
        catalog()
            .buildIndex(indexIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3)
            .withOptimizedColumnIds(3)
            .create();

    assertThat(index).isNotNull();
    assertThat(catalog().indexExists(indexIdentifier)).as("Index should exist").isTrue();

    assertThatThrownBy(
            () ->
                catalog()
                    .buildIndex(indexIdentifier)
                    .withType(IndexType.TERM)
                    .withIndexColumnIds(4)
                    .withOptimizedColumnIds(4)
                    .create())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Index already exists");
  }

  @Test
  public void createIndexOnNonExistingTable() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "non_existing_table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should not exist").isFalse();

    assertThatThrownBy(
            () ->
                catalog()
                    .buildIndex(indexIdentifier)
                    .withType(IndexType.BTREE)
                    .withIndexColumnIds(3)
                    .create())
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("Table does not exist");
  }

  @Test
  public void listIndexes() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().listIndexes(tableIdentifier)).isEmpty();

    IndexIdentifier index1Identifier = IndexIdentifier.of(tableIdentifier, "index1");
    catalog()
        .buildIndex(index1Identifier)
        .withType(IndexType.BTREE)
        .withIndexColumnIds(3)
        .withOptimizedColumnIds(3)
        .create();

    List<IndexSummary> indexes = catalog().listIndexes(tableIdentifier);
    assertThat(indexes).hasSize(1);
    assertThat(indexes.get(0).id()).isEqualTo(index1Identifier);
    assertThat(indexes.get(0).type()).isEqualTo(IndexType.BTREE);

    IndexIdentifier index2Identifier = IndexIdentifier.of(tableIdentifier, "index2");
    catalog()
        .buildIndex(index2Identifier)
        .withType(IndexType.TERM)
        .withIndexColumnIds(4)
        .withOptimizedColumnIds(4)
        .create();

    indexes = catalog().listIndexes(tableIdentifier);
    assertThat(indexes).hasSize(2);

    assertThat(catalog().dropIndex(index1Identifier)).isTrue();
    indexes = catalog().listIndexes(tableIdentifier);
    assertThat(indexes).hasSize(1);
    assertThat(indexes.get(0).id()).isEqualTo(index2Identifier);

    assertThat(catalog().dropIndex(index2Identifier)).isTrue();
    assertThat(catalog().listIndexes(tableIdentifier)).isEmpty();
  }

  @Test
  public void listIndexesByType() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    IndexIdentifier btreeIndexIdentifier = IndexIdentifier.of(tableIdentifier, "btree_index");
    catalog()
        .buildIndex(btreeIndexIdentifier)
        .withType(IndexType.BTREE)
        .withIndexColumnIds(3)
        .withOptimizedColumnIds(3)
        .create();

    IndexIdentifier termIndexIdentifier = IndexIdentifier.of(tableIdentifier, "term_index");
    catalog()
        .buildIndex(termIndexIdentifier)
        .withType(IndexType.TERM)
        .withIndexColumnIds(4)
        .withOptimizedColumnIds(4)
        .create();

    // list all indexes
    List<IndexSummary> allIndexes = catalog().listIndexes(tableIdentifier);
    assertThat(allIndexes).hasSize(2);

    // list only BTREE indexes
    List<IndexSummary> btreeIndexes = catalog().listIndexes(tableIdentifier, IndexType.BTREE);
    assertThat(btreeIndexes).hasSize(1);
    assertThat(btreeIndexes.get(0).id()).isEqualTo(btreeIndexIdentifier);
    assertThat(btreeIndexes.get(0).type()).isEqualTo(IndexType.BTREE);

    // list only TERM indexes
    List<IndexSummary> termIndexes = catalog().listIndexes(tableIdentifier, IndexType.TERM);
    assertThat(termIndexes).hasSize(1);
    assertThat(termIndexes.get(0).id()).isEqualTo(termIndexIdentifier);
    assertThat(termIndexes.get(0).type()).isEqualTo(IndexType.TERM);

    // list IVF indexes (should be empty)
    List<IndexSummary> ivfIndexes = catalog().listIndexes(tableIdentifier, IndexType.IVF);
    assertThat(ivfIndexes).isEmpty();

    // list BTREE and TERM indexes
    List<IndexSummary> multiTypeIndexes =
        catalog().listIndexes(tableIdentifier, IndexType.BTREE, IndexType.TERM);
    assertThat(multiTypeIndexes).hasSize(2);
  }

  @Test
  public void listIndexesOnNonExistingTable() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "non_existing_table");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should not exist").isFalse();

    assertThatThrownBy(() -> catalog().listIndexes(tableIdentifier))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("Table does not exist");
  }

  @ParameterizedTest(name = ".createOrReplace() = {arguments}")
  @ValueSource(booleans = {false, true})
  public void createOrReplaceIndex(boolean useCreateOrReplace) {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "replace_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();

    IndexBuilder indexBuilder =
        catalog()
            .buildIndex(indexIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3)
            .withOptimizedColumnIds(3)
            .withProperty("prop1", "val1");
    Index index = useCreateOrReplace ? indexBuilder.createOrReplace() : indexBuilder.create();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should exist").isTrue();
    assertThat(index.type()).isEqualTo(IndexType.BTREE);
    assertThat(index.indexColumnIds()).containsExactly(3);

    IndexVersion indexVersion = index.currentVersion();

    indexBuilder = catalog().buildIndex(indexIdentifier).withProperty("replacedProp1", "val1");
    Index replacedIndex =
        useCreateOrReplace ? indexBuilder.createOrReplace() : indexBuilder.replace();

    // validate replaced index settings
    assertThat(replacedIndex.name()).isNotNull();
    assertThat(replacedIndex.type()).isEqualTo(IndexType.BTREE);
    assertThat(replacedIndex.indexColumnIds()).containsExactly(3);
    assertThat(replacedIndex.optimizedColumnIds()).containsExactly(3);
    assertThat(replacedIndex.history()).hasSize(2);
    assertThat(replacedIndex.versions())
        .hasSize(2)
        .containsExactly(indexVersion, replacedIndex.currentVersion());

    assertThat(catalog().dropIndex(indexIdentifier)).isTrue();
    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();
  }

  @Test
  public void replaceIndexErrorCases() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "replace_error_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();

    catalog()
        .buildIndex(indexIdentifier)
        .withType(IndexType.BTREE)
        .withIndexColumnIds(3)
        .withOptimizedColumnIds(3)
        .create();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should exist").isTrue();

    assertThatThrownBy(
            () -> catalog().buildIndex(indexIdentifier).withType(IndexType.BTREE).replace())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot update index type");

    assertThatThrownBy(() -> catalog().buildIndex(indexIdentifier).withIndexColumnIds(3).replace())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot update index column ids");

    assertThatThrownBy(
            () -> catalog().buildIndex(indexIdentifier).withOptimizedColumnIds(3).replace())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot update optimized column ids");

    // cannot replace non-existing index
    IndexIdentifier nonExistingIdentifier = IndexIdentifier.of(tableIdentifier, "non_existing");
    assertThatThrownBy(
            () ->
                catalog()
                    .buildIndex(nonExistingIdentifier)
                    .withType(IndexType.BTREE)
                    .withIndexColumnIds(3)
                    .replace())
        .isInstanceOf(NoSuchIndexException.class)
        .hasMessageContaining("Index does not exist");
  }

  @Test
  public void updateIndexProperties() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "props_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();

    Index index =
        catalog()
            .buildIndex(indexIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3)
            .withOptimizedColumnIds(3)
            .create();

    IndexVersion indexVersion = index.currentVersion();

    index
        .updateProperties()
        .set("key1", "val1")
        .set("key2", "val2")
        .remove("non-existing")
        .commit();

    Index updatedIndex = catalog().loadIndex(indexIdentifier);

    // a new version should be added to the index history after updating index properties
    assertThat(updatedIndex.history()).hasSize(2).isEqualTo(index.history());
    assertThat(updatedIndex.versions())
        .hasSize(2)
        .containsExactlyInAnyOrderElementsOf(index.versions());

    assertThat(catalog().dropIndex(indexIdentifier)).isTrue();
    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();
  }

  @Test
  public void updateIndexPropertiesErrorCases() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "props_error_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();

    catalog()
        .buildIndex(indexIdentifier)
        .withType(IndexType.BTREE)
        .withIndexColumnIds(3)
        .withOptimizedColumnIds(3)
        .create();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should exist").isTrue();

    assertThatThrownBy(
            () ->
                catalog()
                    .loadIndex(indexIdentifier)
                    .updateProperties()
                    .set(null, "new-val1")
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid key: null");

    assertThatThrownBy(
            () ->
                catalog().loadIndex(indexIdentifier).updateProperties().set("key1", null).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid value: null");

    assertThatThrownBy(
            () -> catalog().loadIndex(indexIdentifier).updateProperties().remove(null).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid key: null");

    assertThatThrownBy(
            () ->
                catalog()
                    .loadIndex(indexIdentifier)
                    .updateProperties()
                    .set("key1", "x")
                    .set("key3", "y")
                    .remove("key2")
                    .set("key2", "z")
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot remove and update the same key: key2");
  }

  @Test
  public void updateIndexPropertiesConflict() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "props_conflict_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();

    Index index =
        catalog()
            .buildIndex(indexIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3)
            .withOptimizedColumnIds(3)
            .create();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should exist").isTrue();
    UpdateIndexProperties updateIndexProperties = index.updateProperties();

    // drop index and then try to use the updateProperties API
    catalog().dropIndex(indexIdentifier);
    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();

    assertThatThrownBy(() -> updateIndexProperties.set("key1", "val1").commit())
        .isInstanceOf(NoSuchIndexException.class)
        .hasMessageContaining("Index does not exist");
  }

  @Test
  public void createIndexConflict() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "conflict_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();
    IndexBuilder indexBuilder = catalog().buildIndex(indexIdentifier);

    catalog()
        .buildIndex(indexIdentifier)
        .withType(IndexType.BTREE)
        .withIndexColumnIds(3)
        .withOptimizedColumnIds(3)
        .create();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should exist").isTrue();

    // the index was already created concurrently
    assertThatThrownBy(
            () ->
                indexBuilder
                    .withType(IndexType.TERM)
                    .withIndexColumnIds(4)
                    .withOptimizedColumnIds(4)
                    .create())
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Index already exists");
  }

  @Test
  public void replaceIndexConflict() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "replace_conflict_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();

    catalog()
        .buildIndex(indexIdentifier)
        .withType(IndexType.BTREE)
        .withIndexColumnIds(3)
        .withOptimizedColumnIds(3)
        .create();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should exist").isTrue();
    IndexBuilder indexBuilder = catalog().buildIndex(indexIdentifier);

    catalog().dropIndex(indexIdentifier);
    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();

    // the index was already dropped concurrently
    assertThatThrownBy(
            () ->
                indexBuilder
                    .withType(IndexType.BTREE)
                    .withIndexColumnIds(3, 4)
                    .withOptimizedColumnIds(3, 4)
                    .replace())
        .isInstanceOf(NoSuchIndexException.class)
        .hasMessageContaining("Index does not exist");
  }

  @Test
  public void createAndReplaceIndexWithLocation() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "location_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();

    String location =
        indexLocation(
            tableIdentifier.namespace().toString(), tableIdentifier.name(), indexIdentifier.name());
    Index index =
        catalog()
            .buildIndex(indexIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3)
            .withOptimizedColumnIds(3)
            .withLocation(location)
            .create();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should exist").isTrue();

    if (!overridesRequestedLocation()) {
      assertThat(index.location()).isEqualTo(location);
    } else {
      assertThat(index.location()).isNotNull();
    }

    String updatedLocation = indexLocation("updated", "ns", "table", "location_index");
    index = catalog().buildIndex(indexIdentifier).withLocation(updatedLocation).replace();

    if (!overridesRequestedLocation()) {
      assertThat(index.location()).isEqualTo(updatedLocation);
    } else {
      assertThat(index.location()).isNotNull();
    }

    assertThat(catalog().dropIndex(indexIdentifier)).isTrue();
    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();
  }

  @Test
  public void updateIndexLocation() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "update_location_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();

    String location =
        indexLocation(
            tableIdentifier.namespace().toString(), tableIdentifier.name(), indexIdentifier.name());
    Index index =
        catalog()
            .buildIndex(indexIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3)
            .withOptimizedColumnIds(3)
            .withLocation(location)
            .create();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should exist").isTrue();
    if (!overridesRequestedLocation()) {
      assertThat(index.location()).isEqualTo(location);
    } else {
      assertThat(index.location()).isNotNull();
    }

    String updatedLocation = indexLocation("updated", "ns", "table", "update_location_index");
    index.updateLocation().setLocation(updatedLocation).commit();

    Index updatedIndex = catalog().loadIndex(indexIdentifier);

    if (!overridesRequestedLocation()) {
      assertThat(updatedIndex.location()).isEqualTo(updatedLocation);
    } else {
      assertThat(index.location()).isNotNull();
    }

    // history and index versions should stay the same after updating index location
    assertThat(updatedIndex.history()).hasSize(1).isEqualTo(index.history());
    assertThat(updatedIndex.versions()).hasSize(1).containsExactly(index.currentVersion());

    assertThat(catalog().dropIndex(indexIdentifier)).isTrue();
    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();
  }

  @Test
  public void updateIndexLocationConflict() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier =
        IndexIdentifier.of(tableIdentifier, "location_conflict_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();

    Index index =
        catalog()
            .buildIndex(indexIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3)
            .withOptimizedColumnIds(3)
            .create();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should exist").isTrue();

    // new location must be non-null
    assertThatThrownBy(() -> index.updateLocation().setLocation(null).commit())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Invalid index location: null");

    org.apache.iceberg.UpdateLocation updateIndexLocation = index.updateLocation();

    catalog().dropIndex(indexIdentifier);
    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();

    // the index was already dropped concurrently
    assertThatThrownBy(() -> updateIndexLocation.setLocation("new-location").commit())
        .isInstanceOf(NoSuchIndexException.class)
        .hasMessageContaining("Index does not exist");
  }

  @Test
  public void dropNonExistingIndex() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "non_existing");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();
    assertThat(catalog().dropIndex(indexIdentifier)).isFalse();
  }

  @Test
  public void invalidateIndex() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "invalidate_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();

    catalog()
        .buildIndex(indexIdentifier)
        .withType(IndexType.BTREE)
        .withIndexColumnIds(3)
        .withOptimizedColumnIds(3)
        .create();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should exist").isTrue();

    // invalidate should not throw for existing index
    catalog().invalidateIndex(indexIdentifier);
    assertThat(catalog().indexExists(indexIdentifier)).as("Index should still exist").isTrue();

    // invalidate should not throw for non-existing index
    IndexIdentifier nonExistingIdentifier = IndexIdentifier.of(tableIdentifier, "non_existing");
    catalog().invalidateIndex(nonExistingIdentifier);

    assertThat(catalog().dropIndex(indexIdentifier)).isTrue();
  }

  @Test
  public void multipleIndexTypesOnSameTable() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    // Create different types of indexes on the same table
    IndexIdentifier btreeIdentifier = IndexIdentifier.of(tableIdentifier, "btree_idx");
    Index btreeIndex =
        catalog()
            .buildIndex(btreeIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3)
            .withOptimizedColumnIds(3)
            .create();

    IndexIdentifier termIdentifier = IndexIdentifier.of(tableIdentifier, "term_idx");
    Index termIndex =
        catalog()
            .buildIndex(termIdentifier)
            .withType(IndexType.TERM)
            .withIndexColumnIds(4)
            .withOptimizedColumnIds(4)
            .create();

    IndexIdentifier ivfIdentifier = IndexIdentifier.of(tableIdentifier, "ivf_idx");
    Index ivfIndex =
        catalog()
            .buildIndex(ivfIdentifier)
            .withType(IndexType.IVF)
            .withIndexColumnIds(3, 4)
            .withOptimizedColumnIds(3)
            .create();

    assertThat(btreeIndex.type()).isEqualTo(IndexType.BTREE);
    assertThat(termIndex.type()).isEqualTo(IndexType.TERM);
    assertThat(ivfIndex.type()).isEqualTo(IndexType.IVF);

    List<IndexSummary> allIndexes = catalog().listIndexes(tableIdentifier);
    assertThat(allIndexes).hasSize(3);

    // Clean up
    assertThat(catalog().dropIndex(btreeIdentifier)).isTrue();
    assertThat(catalog().dropIndex(termIdentifier)).isTrue();
    assertThat(catalog().dropIndex(ivfIdentifier)).isTrue();
  }

  @Test
  public void createIndexWithSnapshot() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "snapshot_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();

    Index index =
        catalog()
            .buildIndex(indexIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3)
            .withOptimizedColumnIds(3)
            .withProperty("prop1", "val1")
            .withTableSnapshotId(100L)
            .withIndexSnapshotId(1L)
            .withSnapshotProperty("snap_prop1", "snap_val1")
            .create();

    assertThat(index).isNotNull();
    assertThat(catalog().indexExists(indexIdentifier)).as("Index should exist").isTrue();

    // Validate index snapshot
    assertThat(index.snapshots()).hasSize(1);
    IndexSnapshot snapshot = index.snapshots().get(0);
    assertThat(snapshot.tableSnapshotId()).isEqualTo(100L);
    assertThat(snapshot.indexSnapshotId()).isEqualTo(1L);
    assertThat(snapshot.versionId()).isEqualTo(1);
    assertThat(snapshot.properties()).containsEntry("snap_prop1", "snap_val1");

    assertThat(catalog().dropIndex(indexIdentifier)).isTrue();
    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();
  }

  @Test
  public void addSnapshotToExistingIndex() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "add_snapshot_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    // Create index without snapshot
    Index index =
        catalog()
            .buildIndex(indexIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3)
            .withOptimizedColumnIds(3)
            .withProperty("prop1", "val1")
            .create();

    assertThat(index.snapshots()).isEmpty();

    // Add snapshot via the index snapshot builder
    catalog()
        .loadIndex(indexIdentifier)
        .addIndexSnapshot()
        .withTableSnapshotId(100L)
        .withIndexSnapshotId(1L)
        .withSnapshotProperty("snap_prop", "snap_val")
        .commit();

    Index updatedIndex = catalog().loadIndex(indexIdentifier);
    assertThat(updatedIndex.snapshots()).hasSize(1);
    IndexSnapshot snapshot = updatedIndex.snapshots().get(0);
    assertThat(snapshot.tableSnapshotId()).isEqualTo(100L);
    assertThat(snapshot.indexSnapshotId()).isEqualTo(1L);

    assertThat(catalog().dropIndex(indexIdentifier)).isTrue();
  }

  @Test
  public void removeSnapshotsFromIndex() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "remove_snapshot_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    // Create index with a snapshot
    Index index =
        catalog()
            .buildIndex(indexIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3)
            .withOptimizedColumnIds(3)
            .withProperty("prop1", "val1")
            .withTableSnapshotId(100L)
            .withIndexSnapshotId(1L)
            .create();

    assertThat(index.snapshots()).hasSize(1);
    long snapshotIdToRemove = index.snapshots().get(0).indexSnapshotId();

    // Remove the snapshot using the builder
    catalog()
        .loadIndex(indexIdentifier)
        .removeIndexSnapshots()
        .removeSnapshotById(snapshotIdToRemove)
        .commit();

    Index updatedIndex = catalog().loadIndex(indexIdentifier);
    assertThat(updatedIndex.snapshots()).isEmpty();

    assertThat(catalog().dropIndex(indexIdentifier)).isTrue();
  }

  @Test
  public void removeMultipleSnapshotsFromIndex() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier =
        IndexIdentifier.of(tableIdentifier, "remove_multi_snapshot_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    // Create index with first snapshot
    Index index =
        catalog()
            .buildIndex(indexIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3)
            .withOptimizedColumnIds(3)
            .withProperty("prop1", "val1")
            .withTableSnapshotId(100L)
            .withIndexSnapshotId(1L)
            .create();

    assertThat(index.snapshots()).hasSize(1);

    // Add second snapshot via replace
    index =
        catalog()
            .buildIndex(indexIdentifier)
            .withTableSnapshotId(200L)
            .withIndexSnapshotId(2L)
            .replace();

    assertThat(index.snapshots()).hasSize(2);

    // Add third snapshot via replace
    index =
        catalog()
            .buildIndex(indexIdentifier)
            .withTableSnapshotId(300L)
            .withIndexSnapshotId(3L)
            .replace();

    assertThat(index.snapshots()).hasSize(3);

    // Remove multiple snapshots at once
    catalog()
        .loadIndex(indexIdentifier)
        .removeIndexSnapshots()
        .removeSnapshotsByIds(1L, 3L)
        .commit();

    Index updatedIndex = catalog().loadIndex(indexIdentifier);
    assertThat(updatedIndex.snapshots()).hasSize(1);
    assertThat(updatedIndex.snapshots().get(0).indexSnapshotId()).isEqualTo(2L);

    assertThat(catalog().dropIndex(indexIdentifier)).isTrue();
  }

  @Test
  public void removeNonExistentSnapshotFromIndex() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier =
        IndexIdentifier.of(tableIdentifier, "remove_nonexistent_snapshot_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    // Create index with a snapshot
    Index index =
        catalog()
            .buildIndex(indexIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3)
            .withOptimizedColumnIds(3)
            .withProperty("prop1", "val1")
            .withTableSnapshotId(100L)
            .withIndexSnapshotId(1L)
            .create();

    assertThat(index.snapshots()).hasSize(1);

    // Removing non-existent snapshot should not throw, just do nothing
    catalog().loadIndex(indexIdentifier).removeIndexSnapshots().removeSnapshotById(999L).commit();

    Index updatedIndex = catalog().loadIndex(indexIdentifier);
    // Original snapshot should still be there
    assertThat(updatedIndex.snapshots()).hasSize(1);

    assertThat(catalog().dropIndex(indexIdentifier)).isTrue();
  }

  @Test
  public void snapshotLookupMethods() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "lookup_snapshot_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    // Create index with a snapshot
    Index index =
        catalog()
            .buildIndex(indexIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3)
            .withOptimizedColumnIds(3)
            .withProperty("prop1", "val1")
            .withTableSnapshotId(100L)
            .withIndexSnapshotId(1L)
            .create();

    // Test snapshot lookup by index snapshot ID
    IndexSnapshot snapshotById = index.snapshot(1L);
    assertThat(snapshotById).isNotNull();
    assertThat(snapshotById.indexSnapshotId()).isEqualTo(1L);
    assertThat(snapshotById.tableSnapshotId()).isEqualTo(100L);

    // Test snapshot lookup by table snapshot ID
    IndexSnapshot snapshotByTableId = index.snapshotForTableSnapshot(100L);
    assertThat(snapshotByTableId).isNotNull();
    assertThat(snapshotByTableId.indexSnapshotId()).isEqualTo(1L);

    // Test non-existent lookups
    assertThat(index.snapshot(999L)).isNull();
    assertThat(index.snapshotForTableSnapshot(999L)).isNull();

    assertThat(catalog().dropIndex(indexIdentifier)).isTrue();
  }

  @Test
  public void concurrentAddIndexSnapshot() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier =
        IndexIdentifier.of(tableIdentifier, "concurrent_snapshot_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();

    Index index =
        catalog()
            .buildIndex(indexIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3)
            .withOptimizedColumnIds(3)
            .create();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should exist").isTrue();
    assertThat(index.snapshots()).isEmpty();

    AddIndexSnapshot addSnapshotOne =
        index
            .addIndexSnapshot()
            .withTableSnapshotId(100L)
            .withIndexSnapshotId(1L)
            .withSnapshotProperty("source", "snapshot-one");

    AddIndexSnapshot addSnapshotTwo =
        index
            .addIndexSnapshot()
            .withTableSnapshotId(200L)
            .withIndexSnapshotId(2L)
            .withSnapshotProperty("source", "snapshot-two");

    // simulate a concurrent add of the index snapshot
    IndexOperations indexOps = ((BaseIndex) index).operations();
    IndexMetadata current = indexOps.current();

    IndexMetadata firstUpdate = ((IndexSnapshotAdd) addSnapshotOne).internalApply();
    IndexMetadata secondUpdate = ((IndexSnapshotAdd) addSnapshotTwo).internalApply();

    indexOps.commit(current, firstUpdate);

    if (supportsServerSideRetry()) {
      // retry should succeed and the changes should be applied
      indexOps.commit(current, secondUpdate);

      Index updatedIndex = catalog().loadIndex(indexIdentifier);
      assertThat(updatedIndex.snapshots()).hasSize(2);

      IndexSnapshot snapshot1 = updatedIndex.snapshot(1L);
      assertThat(snapshot1).isNotNull();
      assertThat(snapshot1.tableSnapshotId()).isEqualTo(100L);
      assertThat(snapshot1.properties()).containsEntry("source", "snapshot-one");

      IndexSnapshot snapshot2 = updatedIndex.snapshot(2L);
      assertThat(snapshot2).isNotNull();
      assertThat(snapshot2.tableSnapshotId()).isEqualTo(200L);
      assertThat(snapshot2.properties()).containsEntry("source", "snapshot-two");
    } else {
      assertThatThrownBy(() -> indexOps.commit(current, secondUpdate))
          .isInstanceOf(CommitFailedException.class)
          .hasMessageContaining("Cannot commit");

      Index updatedIndex = catalog().loadIndex(indexIdentifier);
      assertThat(updatedIndex.snapshots()).hasSize(1);

      IndexSnapshot snapshot1 = updatedIndex.snapshot(1L);
      assertThat(snapshot1).isNotNull();
      assertThat(snapshot1.tableSnapshotId()).isEqualTo(100L);
      assertThat(snapshot1.properties()).containsEntry("source", "snapshot-one");

      assertThat(updatedIndex.snapshot(2L)).isNull();
    }

    assertThat(catalog().dropIndex(indexIdentifier)).isTrue();
    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();
  }

  @Disabled
  @Test
  public void concurrentUpdateProperties() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier =
        IndexIdentifier.of(tableIdentifier, "concurrent_update_props_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();

    Index index =
        catalog()
            .buildIndex(indexIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3)
            .withOptimizedColumnIds(3)
            .create();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should exist").isTrue();

    UpdateIndexProperties updatePropsOne =
        index.updateProperties().set("key1", "value1").set("source", "update-one");

    UpdateIndexProperties updatePropsTwo =
        index.updateProperties().set("key2", "value2").set("source", "update-two");

    // simulate a concurrent update of the index properties
    IndexOperations indexOps = ((BaseIndex) index).operations();
    IndexMetadata current = indexOps.current();

    IndexMetadata firstUpdate = ((IndexPropertiesUpdate) updatePropsOne).internalApply();
    IndexMetadata secondUpdate = ((IndexPropertiesUpdate) updatePropsTwo).internalApply();

    indexOps.commit(current, firstUpdate);

    if (supportsServerSideRetry()) {
      // retry should succeed and the changes should be applied
      indexOps.commit(current, secondUpdate);

      Index updatedIndex = catalog().loadIndex(indexIdentifier);
      Map<String, String> properties = updatedIndex.currentVersion().properties();
      assertThat(properties).containsEntry("key1", "value1");
      assertThat(properties).containsEntry("key2", "value2");
      // the second update should have overwritten the source property
      assertThat(properties).containsEntry("source", "update-two");
    } else {
      assertThatThrownBy(() -> indexOps.commit(current, secondUpdate))
          .isInstanceOf(CommitFailedException.class)
          .hasMessageContaining("Cannot commit");

      Index updatedIndex = catalog().loadIndex(indexIdentifier);
      Map<String, String> properties = updatedIndex.currentVersion().properties();
      assertThat(properties).containsEntry("key1", "value1");
      assertThat(properties).containsEntry("source", "update-one");
      assertThat(properties).doesNotContainKey("key2");
    }

    assertThat(catalog().dropIndex(indexIdentifier)).isTrue();
    assertThat(catalog().indexExists(indexIdentifier)).as("Index should not exist").isFalse();
  }

  @Test
  public void registerIndex() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier sourceIndexIdentifier = IndexIdentifier.of(tableIdentifier, "source_index");
    IndexIdentifier registeredIndexIdentifier =
        IndexIdentifier.of(tableIdentifier, "registered_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    // Create a source index to get its metadata file location
    Index sourceIndex =
        catalog()
            .buildIndex(sourceIndexIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3)
            .withOptimizedColumnIds(3)
            .withProperty("prop1", "val1")
            .create();

    assertThat(catalog().indexExists(sourceIndexIdentifier))
        .as("Source index should exist")
        .isTrue();

    // Get the metadata file location from the source index
    String metadataFileLocation =
        ((BaseIndex) sourceIndex).operations().current().metadataFileLocation();
    assertThat(metadataFileLocation).isNotNull();

    // Drop the source index before registering it with a new identifier
    assertThat(catalog().dropIndex(sourceIndexIdentifier)).isTrue();
    assertThat(catalog().indexExists(sourceIndexIdentifier))
        .as("Source index should not exist")
        .isFalse();

    // Register a new index using the metadata file location
    Index registeredIndex =
        catalog().registerIndex(registeredIndexIdentifier, metadataFileLocation);

    assertThat(registeredIndex).isNotNull();
    assertThat(catalog().indexExists(registeredIndexIdentifier))
        .as("Registered index should exist")
        .isTrue();

    // Validate the registered index has the same properties as the source
    assertThat(registeredIndex.type()).isEqualTo(sourceIndex.type());
    assertThat(registeredIndex.indexColumnIds()).isEqualTo(sourceIndex.indexColumnIds());
    assertThat(registeredIndex.optimizedColumnIds()).isEqualTo(sourceIndex.optimizedColumnIds());

    // Validate the registered index has the same UUID as the source (registerIndex preserves
    // metadata)
    assertThat(registeredIndex.uuid()).isEqualTo(sourceIndex.uuid());

    // Clean up
    assertThat(catalog().dropIndex(registeredIndexIdentifier)).isTrue();
  }

  @Test
  public void registerIndexThatAlreadyExists() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "existing_index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    // Create the index first
    Index existingIndex =
        catalog()
            .buildIndex(indexIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3)
            .withOptimizedColumnIds(3)
            .create();

    assertThat(catalog().indexExists(indexIdentifier)).as("Index should exist").isTrue();

    String metadataFileLocation =
        ((BaseIndex) existingIndex).operations().current().metadataFileLocation();

    // Trying to register an index with the same identifier should fail
    assertThatThrownBy(() -> catalog().registerIndex(indexIdentifier, metadataFileLocation))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("Index already exists");

    // Clean up
    assertThat(catalog().dropIndex(indexIdentifier)).isTrue();
  }

  @Test
  public void registerIndexOnNonExistingTable() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "non_existing_table");
    TableIdentifier existingTableIdentifier = TableIdentifier.of("ns", "existing_table");
    IndexIdentifier sourceIndexIdentifier =
        IndexIdentifier.of(existingTableIdentifier, "source_index");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    // Create a table and index to get a valid metadata file location
    tableCatalog().buildTable(existingTableIdentifier, SCHEMA).create();
    Index sourceIndex =
        catalog()
            .buildIndex(sourceIndexIdentifier)
            .withType(IndexType.BTREE)
            .withIndexColumnIds(3)
            .withOptimizedColumnIds(3)
            .create();

    String metadataFileLocation =
        ((BaseIndex) sourceIndex).operations().current().metadataFileLocation();

    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should not exist").isFalse();

    // Trying to register an index on a non-existing table should fail
    assertThatThrownBy(() -> catalog().registerIndex(indexIdentifier, metadataFileLocation))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("Table does not exist");

    // Clean up
    assertThat(catalog().dropIndex(sourceIndexIdentifier)).isTrue();
  }

  @Test
  public void registerIndexWithInvalidMetadataLocation() {
    TableIdentifier tableIdentifier = TableIdentifier.of("ns", "table");
    IndexIdentifier indexIdentifier = IndexIdentifier.of(tableIdentifier, "index");

    if (requiresNamespaceCreate()) {
      catalog().createNamespace(tableIdentifier.namespace());
    }

    tableCatalog().buildTable(tableIdentifier, SCHEMA).create();
    assertThat(tableCatalog().tableExists(tableIdentifier)).as("Table should exist").isTrue();

    // Trying to register with null metadata location should fail
    assertThatThrownBy(() -> catalog().registerIndex(indexIdentifier, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot register an empty metadata file location");

    // Trying to register with empty metadata location should fail
    assertThatThrownBy(() -> catalog().registerIndex(indexIdentifier, ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot register an empty metadata file location");
  }
}
