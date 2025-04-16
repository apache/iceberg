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
package org.apache.iceberg.gcp.bigquery;

import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE;
import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE_BIGQUERY;
import static org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog.PROPERTIES_KEY_TESTING_ENABLED;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.CatalogTests;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestBigQueryCatalog extends CatalogTests<BigQueryMetastoreCatalog> {

  private static final String GCP_PROJECT = "project-id";
  private static final String CATALOG_ID = "catalog-name";

  // another schema that is not the same
  protected static final Schema OTHER_SCHEMA_OPTIONAL =
      new Schema(optional(1, "some_id", Types.IntegerType.get()));

  // Schema passed to create tables
  protected static final Schema SCHEMA_OPTIONAL =
      new Schema(
          optional(3, "id", Types.IntegerType.get(), "unique ID 🤪"),
          optional(4, "data", Types.StringType.get()));

  // This is the actual schema for the table, with column IDs reassigned
  protected static final Schema REPLACE_SCHEMA_OPTIONAL =
      new Schema(
          optional(2, "id", Types.IntegerType.get(), "unique ID 🤪"),
          optional(3, "data", Types.StringType.get()));

  protected static final PartitionSpec REPLACE_SPEC_OPTIONAL =
      PartitionSpec.builderFor(REPLACE_SCHEMA_OPTIONAL).bucket("id", 16).withSpecId(1).build();

  protected static final SortOrder REPLACE_WRITE_ORDER_OPTIONAL =
      SortOrder.builderFor(REPLACE_SCHEMA_OPTIONAL)
          .asc(Expressions.bucket("id", 16))
          .asc("id")
          .build();

  @TempDir private File tempFolder;
  private BigQueryMetastoreCatalog catalog;
  private BigQueryMetaStoreClient fakeBigQueryMetaStoreClient;
  private String warehouseLocation;

  @BeforeEach
  public void before() throws Exception {
    catalog = initCatalog(CATALOG_ID, ImmutableMap.of());
  }

  @AfterEach
  public void after() throws Exception {
    // Drop all tables in all datasets
    List<Namespace> namespaces = catalog().listNamespaces(Namespace.empty());
    for (Namespace namespace : namespaces) {
      List<TableIdentifier> tables = catalog().listTables(namespace);
      for (TableIdentifier table : tables) {
        try {
          catalog().dropTable(table, true); // drop with purge
        } catch (NoSuchTableException e) {
          // Table might be dropped by a previous iteration, ignore
        }
      }

      // Drop all datasets (except any initial ones)
      try {
        catalog().dropNamespace(namespace);
      } catch (NoSuchNamespaceException e) {
        // Namespace might be dropped by a previous iteration, ignore
      }
    }
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Override
  protected boolean supportsNamesWithSlashes() {
    return false;
  }

  @Override
  protected boolean supportsNamesWithDot() {
    return false;
  }

  @Override
  public BigQueryMetastoreCatalog catalog() {
    return catalog;
  }

  @Override
  protected BigQueryMetastoreCatalog initCatalog(
      String catalogName, Map<String, String> additionalProperties) {

    warehouseLocation = tempFolder.toPath().resolve("hive-warehouse").toString();

    Map<String, String> properties =
        Map.of(
            ICEBERG_CATALOG_TYPE,
            ICEBERG_CATALOG_TYPE_BIGQUERY,
            GCPProperties.PROJECT_ID,
            GCP_PROJECT,
            CatalogProperties.WAREHOUSE_LOCATION,
            warehouseLocation,
            PROPERTIES_KEY_TESTING_ENABLED,
            "true",
            CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key1",
            "catalog-default-key1",
            CatalogProperties.TABLE_DEFAULT_PREFIX + "default-key2",
            "catalog-default-key2",
            CatalogProperties.TABLE_DEFAULT_PREFIX + "override-key3",
            "catalog-default-key3",
            CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key3",
            "catalog-override-key3",
            CatalogProperties.TABLE_OVERRIDE_PREFIX + "override-key4",
            "catalog-override-key4");

    BigQueryMetastoreCatalog tmpCatalog =
        (BigQueryMetastoreCatalog)
            CatalogUtil.buildIcebergCatalog(
                catalogName,
                ImmutableMap.<String, String>builder()
                    .putAll(properties)
                    .putAll(additionalProperties)
                    .build(),
                null);

    return tmpCatalog;
  }

  @Test
  public void testRenameTable() {
    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    assertThat(catalog.tableExists(TABLE))
        .as("Source table should not exist before create")
        .isFalse();

    catalog.buildTable(TABLE, SCHEMA).create();
    assertThat(catalog.tableExists(TABLE)).as("Table should exist after create").isTrue();

    assertThat(catalog.tableExists(RENAMED_TABLE))
        .as("Destination table should not exist before rename")
        .isFalse();

    assertThatThrownBy(() -> catalog.renameTable(TABLE, RENAMED_TABLE))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Table rename operation is unsupported");

    catalog.dropTable(TABLE);
  }

  @Disabled(
      "TODO(b/407563915): BigQuery does not allow adding a REQUIRED column to an existing table schema. "
          + "When this operation is attempted, an exception gets thrown.")
  @Test
  public void testOverrideTablePropertiesReplaceTransaction() {}

  @Test
  public void testConcurrentReplaceTransactionSchema() {

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Transaction transaction = catalog.buildTable(TABLE, OTHER_SCHEMA_OPTIONAL).createTransaction();
    transaction.newFastAppend().appendFile(FILE_A).commit();
    transaction.commitTransaction();

    Table original = catalog.loadTable(TABLE);
    assertFiles(original, FILE_A);

    Transaction secondReplace =
        catalog.buildTable(TABLE, OTHER_SCHEMA_OPTIONAL).replaceTransaction();
    secondReplace.newFastAppend().appendFile(FILE_C).commit();

    Transaction firstReplace = catalog.buildTable(TABLE, SCHEMA_OPTIONAL).replaceTransaction();
    firstReplace.newFastAppend().appendFile(FILE_B).commit();
    firstReplace.commitTransaction();

    Table afterFirstReplace = catalog.loadTable(TABLE);
    assertThat(afterFirstReplace.schema().asStruct())
        .as("Table schema should match the new schema")
        .isEqualTo(REPLACE_SCHEMA_OPTIONAL.asStruct());
    assertUUIDsMatch(original, afterFirstReplace);
    assertFiles(afterFirstReplace, FILE_B);

    secondReplace.commitTransaction();

    Table afterSecondReplace = catalog.loadTable(TABLE);
    assertThat(afterSecondReplace.schema().asStruct())
        .as("Table schema should match the original schema")
        .isEqualTo(original.schema().asStruct());
    assertUUIDsMatch(original, afterSecondReplace);
    assertFiles(afterSecondReplace, FILE_C);
  }

  // TODO: unit test use local path rather than abstraction. When we use gs for path it throws
  // exception
  @Test
  public void testCompleteCreateTransaction() {
    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    String tableLocation = warehouseLocation + "/tmp/ns/table";

    Map<String, String> properties =
        ImmutableMap.of("user", "someone", "created-at", "2022-02-25T00:38:19");
    Transaction create =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withLocation(tableLocation)
            .withPartitionSpec(SPEC)
            .withSortOrder(WRITE_ORDER)
            .withProperties(properties)
            .createTransaction();

    assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after createTransaction")
        .isFalse();

    create.newFastAppend().appendFile(FILE_A).commit();

    assertThat(catalog.tableExists(TABLE))
        .as("Table should not exist after append commit")
        .isFalse();

    create.commitTransaction();

    assertThat(catalog.tableExists(TABLE)).as("Table should exist after append commit").isTrue();

    Table table = catalog.loadTable(TABLE);
    assertThat(table.schema().asStruct())
        .as("Table schema should match the new schema")
        .isEqualTo(TABLE_SCHEMA.asStruct());
    assertThat(table.spec().fields())
        .as("Table should have create partition spec")
        .isEqualTo(TABLE_SPEC.fields());
    assertThat(table.sortOrder())
        .as("Table should have create sort order")
        .isEqualTo(TABLE_WRITE_ORDER);
    assertThat(table.properties().entrySet())
        .as("Table properties should be a superset of the requested properties")
        .containsAll(properties.entrySet());
    if (!overridesRequestedLocation()) {
      assertThat(table.location())
          .as("Table location should match requested")
          .isEqualTo(tableLocation);
    }
    assertFiles(table, FILE_A);
    assertFilesPartitionSpec(table);
    assertPreviousMetadataFileCount(table, 0);
  }

  @Disabled(
      "TODO(b/407563915): BigQuery does not allow adding a REQUIRED column to an existing table schema. "
          + "When this operation is attempted, an exception gets thrown.")
  @Test
  public void testCreateOrReplaceReplaceTransactionReplace() {}

  // TODO: Remove this test when BigQuery supports required filed change
  @Test
  public void testCreateOrReplaceReplaceTransactionReplaceWithOptionalFields() {

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table original = catalog.buildTable(TABLE, OTHER_SCHEMA_OPTIONAL).create();

    assertThat(catalog.tableExists(TABLE))
        .as("Table should exist before replaceTransaction")
        .isTrue();

    Transaction createOrReplace =
        catalog.buildTable(TABLE, SCHEMA_OPTIONAL).createOrReplaceTransaction();

    assertThat(catalog.tableExists(TABLE))
        .as("Table should still exist after replaceTransaction")
        .isTrue();

    createOrReplace.newFastAppend().appendFile(FILE_A).commit();

    // validate table has not changed
    Table table = catalog.loadTable(TABLE);

    assertThat(table.schema().asStruct())
        .as("Table schema should match concurrent create")
        .isEqualTo(OTHER_SCHEMA_OPTIONAL.asStruct());

    assertUUIDsMatch(original, table);
    assertNoFiles(table);

    createOrReplace.commitTransaction();

    // validate the table after replace
    assertThat(catalog.tableExists(TABLE)).as("Table should exist after append commit").isTrue();
    table.refresh(); // refresh should work with UUID validation

    Table loaded = catalog.loadTable(TABLE);

    assertThat(loaded.schema().asStruct())
        .as("Table schema should match the new schema")
        .isEqualTo(REPLACE_SCHEMA_OPTIONAL.asStruct());
    assertUUIDsMatch(original, loaded);
    assertFiles(loaded, FILE_A);
    assertPreviousMetadataFileCount(loaded, 1);
  }

  @Disabled(
      "TODO(b/407563915): BigQuery does not allow adding a REQUIRED column to an existing table schema. "
          + "When this operation is attempted, an exception gets thrown.")
  @Test
  public void testConcurrentReplaceTransactionSchema2() {}

  // TODO: Remove this test when BigQuery supports required field change
  @Test
  public void testConcurrentReplaceTransactionSchema2WithOptionalFields() {
    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Transaction transaction = catalog.buildTable(TABLE, OTHER_SCHEMA_OPTIONAL).createTransaction();
    transaction.newFastAppend().appendFile(FILE_A).commit();
    transaction.commitTransaction();

    Table original = catalog.loadTable(TABLE);
    assertFiles(original, FILE_A);

    Transaction secondReplace = catalog.buildTable(TABLE, SCHEMA_OPTIONAL).replaceTransaction();
    secondReplace.newFastAppend().appendFile(FILE_C).commit();

    Transaction firstReplace =
        catalog.buildTable(TABLE, OTHER_SCHEMA_OPTIONAL).replaceTransaction();
    firstReplace.newFastAppend().appendFile(FILE_B).commit();
    firstReplace.commitTransaction();

    Table afterFirstReplace = catalog.loadTable(TABLE);
    assertThat(afterFirstReplace.schema().asStruct())
        .as("Table schema should match the original schema")
        .isEqualTo(original.schema().asStruct());
    assertUUIDsMatch(original, afterFirstReplace);
    assertFiles(afterFirstReplace, FILE_B);

    secondReplace.commitTransaction();

    Table afterSecondReplace = catalog.loadTable(TABLE);
    assertThat(afterSecondReplace.schema().asStruct())
        .as("Table schema should match the new schema")
        .isEqualTo(REPLACE_SCHEMA_OPTIONAL.asStruct());
    assertUUIDsMatch(original, afterSecondReplace);
    assertFiles(afterSecondReplace, FILE_C);
  }

  @Disabled(
      "TODO(b/407563915): BigQuery does not allow adding a REQUIRED column to an existing table schema. "
          + "When this operation is attempted, an exception gets thrown.")
  @Test
  public void testDefaultTablePropertiesReplaceTransaction() {}

  // TODO: Remove this test when BigQuery supports required field change
  @Test
  public void testDefaultTablePropertiesReplaceTransactionWithOptionalFields() {
    TableIdentifier ident = TableIdentifier.of("ns", "nstable");

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(ident.namespace());
    }

    catalog.createTable(ident, SCHEMA_OPTIONAL);
    assertThat(catalog.tableExists(ident)).as("Table should exist").isTrue();

    catalog()
        .buildTable(ident, OTHER_SCHEMA_OPTIONAL)
        .withProperty("default-key2", "catalog-overridden-key2")
        .withProperty("prop1", "val1")
        .replaceTransaction()
        .commitTransaction();

    Table table = catalog.loadTable(ident);

    assertThat(table.properties())
        .containsEntry("default-key1", "catalog-default-key1")
        .containsEntry("default-key2", "catalog-overridden-key2")
        .containsEntry("prop1", "val1");

    assertThat(catalog.dropTable(ident)).as("Should successfully drop table").isTrue();
  }

  @Disabled(
      "TODO(b/407563915): BigQuery does not allow adding a REQUIRED column to an existing table schema. "
          + "When this operation is attempted, an exception gets thrown.")
  @Test
  public void testReplaceTransaction() {}

  @Test
  public void testReplaceTransactionWithOptionalFields() {
    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table original = catalog.buildTable(TABLE, OTHER_SCHEMA_OPTIONAL).create();

    assertThat(catalog.tableExists(TABLE))
        .as("Table should exist before replaceTransaction")
        .isTrue();

    Transaction replace = catalog.buildTable(TABLE, SCHEMA_OPTIONAL).replaceTransaction();

    assertThat(catalog.tableExists(TABLE))
        .as("Table should still exist after replaceTransaction")
        .isTrue();

    replace.newFastAppend().appendFile(FILE_A).commit();

    // validate table has not changed
    Table table = catalog.loadTable(TABLE);
    assertThat(table.schema().asStruct())
        .as("Table schema should match concurrent create")
        .isEqualTo(OTHER_SCHEMA_OPTIONAL.asStruct());
    assertUUIDsMatch(original, table);
    assertNoFiles(table);

    replace.commitTransaction();

    // validate the table after replace
    assertThat(catalog.tableExists(TABLE)).as("Table should exist after append commit").isTrue();
    table.refresh(); // refresh should work with UUID validation

    Table loaded = catalog.loadTable(TABLE);

    assertThat(loaded.schema().asStruct())
        .as("Table schema should match the new schema")
        .isEqualTo(REPLACE_SCHEMA_OPTIONAL.asStruct());

    assertUUIDsMatch(original, loaded);
    assertFiles(loaded, FILE_A);
    assertPreviousMetadataFileCount(loaded, 1);
  }

  // TODO: BigQuery Metastore does not support V3 Spec yet.
  @Override
  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void createTableTransaction(int formatVersion) {
    if (requiresNamespaceCreate()) {
      catalog().createNamespace(NS);
    }

    catalog()
        .newCreateTableTransaction(
            TABLE,
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of("format-version", String.valueOf(formatVersion)))
        .commitTransaction();

    assertThat(TableUtil.formatVersion(catalog().loadTable(TABLE))).isEqualTo(formatVersion);
  }

  @Disabled("BigQuery Metastore does not support V3 Spec yet.")
  @Test
  public void testCreateTableWithDefaultColumnValue() {}

  @Disabled(
      "TODO(b/407563915): BigQuery does not allow adding a REQUIRED column to an existing table schema. "
          + "When this operation is attempted, an exception gets thrown.")
  @Test
  public void testCompleteCreateOrReplaceTransactionReplace() {}

  @Test
  public void testCompleteCreateOrReplaceTransactionReplaceWithOptionalFields() {
    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table original = catalog.buildTable(TABLE, OTHER_SCHEMA_OPTIONAL).create();

    assertThat(catalog.tableExists(TABLE))
        .as("Table should exist before replaceTransaction")
        .isTrue();

    Map<String, String> properties =
        ImmutableMap.of("user", "someone", "created-at", "2022-02-25T00:38:19");
    Transaction createOrReplace =
        catalog
            .buildTable(TABLE, SCHEMA_OPTIONAL)
            .withLocation("file:/tmp/ns/table")
            .withPartitionSpec(SPEC)
            .withSortOrder(WRITE_ORDER)
            .withProperties(properties)
            .createOrReplaceTransaction();

    assertThat(catalog.tableExists(TABLE))
        .as("Table should still exist after replaceTransaction")
        .isTrue();

    createOrReplace.newFastAppend().appendFile(FILE_A).commit();

    // validate table has not changed
    Table table = catalog.loadTable(TABLE);
    assertThat(table.schema().asStruct())
        .as("Table schema should match concurrent create")
        .isEqualTo(OTHER_SCHEMA_OPTIONAL.asStruct());
    assertThat(table.spec().isUnpartitioned()).as("Table should be unpartitioned").isTrue();
    assertThat(table.sortOrder().isUnsorted()).as("Table should be unsorted").isTrue();
    assertThat(table.properties().get("created-at"))
        .as("Created at should not match")
        .isNotEqualTo("2022-02-25T00:38:19");
    assertUUIDsMatch(original, table);
    assertNoFiles(table);

    createOrReplace.commitTransaction();

    // validate the table after replace
    assertThat(catalog.tableExists(TABLE)).as("Table should exist after append commit").isTrue();
    table.refresh(); // refresh should work with UUID validation

    Table loaded = catalog.loadTable(TABLE);

    assertThat(loaded.schema().asStruct())
        .as("Table schema should match the new schema")
        .isEqualTo(REPLACE_SCHEMA_OPTIONAL.asStruct());
    assertThat(loaded.spec())
        .as("Table should have replace partition spec")
        .isEqualTo(REPLACE_SPEC_OPTIONAL);
    assertThat(loaded.sortOrder())
        .as("Table should have replace sort order")
        .isEqualTo(REPLACE_WRITE_ORDER);
    assertThat(loaded.properties().entrySet())
        .as("Table properties should be a superset of the requested properties")
        .containsAll(properties.entrySet());
    if (!overridesRequestedLocation()) {
      assertThat(table.location())
          .as("Table location should be replaced")
          .isEqualTo("file:/tmp/ns/table");
    }

    assertUUIDsMatch(original, loaded);
    assertFiles(loaded, FILE_A);
    assertPreviousMetadataFileCount(loaded, 1);
  }

  @Disabled(
      "TODO(b/407563915): BigQuery does not allow adding a REQUIRED column to an existing table schema. "
          + "When this operation is attempted, an exception gets thrown.")
  @Test
  public void testCompleteReplaceTransaction() {}

  @Test
  public void testCompleteReplaceTransactionWithOptionalFields() {
    if (requiresNamespaceCreate()) {
      catalog.createNamespace(NS);
    }

    Table original = catalog.buildTable(TABLE, OTHER_SCHEMA_OPTIONAL).create();

    assertThat(catalog.tableExists(TABLE))
        .as("Table should exist before replaceTransaction")
        .isTrue();

    Map<String, String> properties =
        ImmutableMap.of("user", "someone", "created-at", "2022-02-25T00:38:19");
    Transaction replace =
        catalog
            .buildTable(TABLE, SCHEMA_OPTIONAL)
            .withLocation("file:/tmp/ns/table")
            .withPartitionSpec(SPEC)
            .withSortOrder(WRITE_ORDER)
            .withProperties(properties)
            .replaceTransaction();

    assertThat(catalog.tableExists(TABLE))
        .as("Table should still exist after replaceTransaction")
        .isTrue();

    replace.newFastAppend().appendFile(FILE_A).commit();

    // validate table has not changed
    Table table = catalog.loadTable(TABLE);

    assertThat(table.schema().asStruct())
        .as("Table schema should match concurrent create")
        .isEqualTo(OTHER_SCHEMA_OPTIONAL.asStruct());
    assertThat(table.spec().isUnpartitioned()).as("Table should be unpartitioned").isTrue();
    assertThat(table.sortOrder().isUnsorted()).as("Table should be unsorted").isTrue();
    assertThat(table.properties().get("created-at"))
        .as("Created at should not match")
        .isNotEqualTo("2022-02-25T00:38:19");

    assertUUIDsMatch(original, table);
    assertNoFiles(table);

    replace.commitTransaction();

    // validate the table after replace
    assertThat(catalog.tableExists(TABLE)).as("Table should exist after append commit").isTrue();
    table.refresh(); // refresh should work with UUID validation

    Table loaded = catalog.loadTable(TABLE);

    assertThat(loaded.schema().asStruct())
        .as("Table schema should match the new schema")
        .isEqualTo(REPLACE_SCHEMA_OPTIONAL.asStruct());
    assertThat(loaded.spec())
        .as("Table should have replace partition spec")
        .isEqualTo(REPLACE_SPEC_OPTIONAL);
    assertThat(loaded.sortOrder())
        .as("Table should have replace sort order")
        .isEqualTo(REPLACE_WRITE_ORDER_OPTIONAL);
    assertThat(loaded.properties().entrySet())
        .as("Table properties should be a superset of the requested properties")
        .containsAll(properties.entrySet());
    if (!overridesRequestedLocation()) {
      assertThat(table.location())
          .as("Table location should be replaced")
          .isEqualTo("file:/tmp/ns/table");
    }

    assertUUIDsMatch(original, loaded);
    assertFiles(loaded, FILE_A);
    assertPreviousMetadataFileCount(loaded, 1);
  }
}
