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
package org.apache.iceberg.aws.glue;

import static org.apache.iceberg.expressions.Expressions.truncate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.LockManagers;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTableVersionsRequest;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Tag;

public class TestGlueCatalogTable extends GlueTestBase {

  @Test
  public void testCreateTable() {
    String namespace = createNamespace();
    String tableName = getRandomName();
    String tableDescription = "Test table";
    Map<String, String> tableProperties =
        ImmutableMap.<String, String>builder()
            .putAll(TABLE_LOCATION_PROPERTIES)
            .put(IcebergToGlueConverter.GLUE_DESCRIPTION_KEY, tableDescription)
            .build();
    glueCatalog.createTable(
        TableIdentifier.of(namespace, tableName), schema, partitionSpec, tableProperties);
    // verify table exists in Glue
    GetTableResponse response =
        GLUE.getTable(GetTableRequest.builder().databaseName(namespace).name(tableName).build());
    assertThat(response.table().databaseName()).isEqualTo(namespace);
    assertThat(response.table().name()).isEqualTo(tableName);
    assertThat(response.table().parameters())
        .containsEntry(
            BaseMetastoreTableOperations.TABLE_TYPE_PROP,
            BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH))
        .containsKey(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
    assertThat(response.table().storageDescriptor().columns()).hasSameSizeAs(schema.columns());
    assertThat(response.table().partitionKeys()).hasSameSizeAs(partitionSpec.fields());
    assertThat(response.table().storageDescriptor().additionalLocations())
        .containsExactlyInAnyOrderElementsOf(TABLE_LOCATION_PROPERTIES.values());
    // verify metadata file exists in S3
    String metaLocation =
        response.table().parameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
    String key = metaLocation.split(TEST_BUCKET_NAME, -1)[1].substring(1);
    S3.headObject(HeadObjectRequest.builder().bucket(TEST_BUCKET_NAME).key(key).build());
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    assertThat(table.spec()).isEqualTo(partitionSpec);
    assertThat(table.schema()).asString().isEqualTo(schema.toString());
    assertThat(table.properties())
        .containsEntry(IcebergToGlueConverter.GLUE_DESCRIPTION_KEY, tableDescription);
    assertThat(response.table().description()).isEqualTo(tableDescription);
  }

  @Test
  public void testCreateTableDuplicate() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    assertThatThrownBy(
            () ->
                glueCatalog.createTable(
                    TableIdentifier.of(namespace, tableName), schema, partitionSpec))
        .isInstanceOf(AlreadyExistsException.class)
        .as("should not create table with the same name")
        .hasMessageContaining("Table already exists");
  }

  @Test
  public void testCreateTableBadName() {
    String namespace = createNamespace();
    assertThatThrownBy(
            () ->
                glueCatalog.createTable(
                    TableIdentifier.of(namespace, "table-1"), schema, partitionSpec))
        .isInstanceOf(IllegalArgumentException.class)
        .as("should not create table with bad name")
        .hasMessageContaining("Invalid table identifier");
  }

  @Test
  public void testCreateAndLoadTableWithoutWarehouseLocation() {
    GlueCatalog glueCatalogWithoutWarehouse = new GlueCatalog();
    glueCatalogWithoutWarehouse.initialize(
        CATALOG_NAME,
        null,
        new AwsProperties(),
        new S3FileIOProperties(),
        GLUE,
        LockManagers.defaultLockManager(),
        ImmutableMap.of());
    String namespace = createNamespace();
    String tableName = getRandomName();
    TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
    try {
      glueCatalog.createTable(identifier, schema, partitionSpec, TABLE_LOCATION_PROPERTIES);
      glueCatalog.loadTable(identifier);
    } catch (RuntimeException e) {
      throw new RuntimeException(
          "Create and load table without warehouse location should succeed", e);
    }
  }

  @Test
  public void testListTables() {
    String namespace = createNamespace();
    assertThat(glueCatalog.listTables(Namespace.of(namespace))).isEmpty();
    String tableName = createTable(namespace);
    List<TableIdentifier> tables = glueCatalog.listTables(Namespace.of(namespace));
    assertThat(tables).hasSize(1).first().isEqualTo(TableIdentifier.of(namespace, tableName));
  }

  @Test
  public void testTableExists() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    assertThat(glueCatalog.tableExists(TableIdentifier.of(namespace, tableName))).isTrue();
  }

  @Test
  public void testUpdateTable() {
    String namespace = createNamespace();
    String tableName = getRandomName();
    // current should be null
    TableOperations ops = glueCatalog.newTableOps(TableIdentifier.of(namespace, tableName));
    TableMetadata current = ops.current();
    assertThat(current).isNull();
    // create table, refresh should update
    createTable(namespace, tableName);
    String description = "test description";
    updateTableDescription(namespace, tableName, description);
    current = ops.refresh();
    assertThat(current.schema()).asString().isEqualTo(schema.toString());
    assertThat(current.spec()).isEqualTo(partitionSpec);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    assertThat(table.history()).isEmpty();
    // commit new version, should create a new snapshot
    table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    DataFile dataFile =
        DataFiles.builder(partitionSpec)
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    table.newAppend().appendFile(dataFile).commit();
    table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    assertThat(table.history()).hasSize(1);
    // check table in Glue
    GetTableResponse response =
        GLUE.getTable(GetTableRequest.builder().databaseName(namespace).name(tableName).build());
    assertThat(response.table().tableType())
        .as("external table type is set after update")
        .isEqualTo("EXTERNAL_TABLE");
    assertThat(response.table().storageDescriptor().columns()).hasSameSizeAs(schema.columns());
    assertThat(response.table().partitionKeys()).hasSameSizeAs(partitionSpec.fields());
    assertThat(response.table().description()).isEqualTo(description);

    String updatedComment = "test updated comment";
    table
        .updateProperties()
        .set(IcebergToGlueConverter.GLUE_DESCRIPTION_KEY, updatedComment)
        .commit();
    // check table in Glue
    response =
        GLUE.getTable(GetTableRequest.builder().databaseName(namespace).name(tableName).build());
    assertThat(response.table().description()).isEqualTo(updatedComment);
  }

  @Test
  public void testDropColumn() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    table
        .updateSchema()
        .addColumn("c2", Types.StringType.get(), "updated from Iceberg API")
        .addColumn("c3", Types.StringType.get())
        .commit();

    updateTableColumns(
        namespace,
        tableName,
        column -> {
          if (column.name().equals("c3")) {
            return column.toBuilder().comment("updated from Glue API").build();
          } else {
            return column;
          }
        });

    table.updateSchema().deleteColumn("c2").deleteColumn("c3").commit();

    GetTableResponse response =
        GLUE.getTable(GetTableRequest.builder().databaseName(namespace).name(tableName).build());
    List<Column> actualColumns = response.table().storageDescriptor().columns();

    List<Column> expectedColumns =
        ImmutableList.of(
            Column.builder()
                .name("c1")
                .type("string")
                .comment("c1")
                .parameters(
                    ImmutableMap.of(
                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "1",
                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "false",
                        IcebergToGlueConverter.ICEBERG_FIELD_CURRENT, "true"))
                .build(),
            Column.builder()
                .name("c2")
                .type("string")
                .comment("updated from Iceberg API")
                .parameters(
                    ImmutableMap.of(
                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "2",
                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "true",
                        IcebergToGlueConverter.ICEBERG_FIELD_CURRENT, "false"))
                .build(),
            Column.builder()
                .name("c3")
                .type("string")
                .comment("updated from Glue API")
                .parameters(
                    ImmutableMap.of(
                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "3",
                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "true",
                        IcebergToGlueConverter.ICEBERG_FIELD_CURRENT, "false"))
                .build());
    assertThat(actualColumns).isEqualTo(expectedColumns);
  }

  @Test
  public void testRenameTable() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    // rename table
    String newTableName = tableName + "_2";
    glueCatalog.renameTable(
        TableIdentifier.of(namespace, tableName), TableIdentifier.of(namespace, newTableName));
    Table renamedTable = glueCatalog.loadTable(TableIdentifier.of(namespace, newTableName));
    assertThat(renamedTable.location()).isEqualTo(table.location());
    assertThat(renamedTable.schema()).asString().isEqualTo(table.schema().toString());
    assertThat(renamedTable.spec()).isEqualTo(table.spec());
    assertThat(renamedTable.currentSnapshot()).isEqualTo(table.currentSnapshot());
  }

  @Test
  public void testRenameTableFailsToCreateNewTable() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    TableIdentifier id = TableIdentifier.of(namespace, tableName);
    Table table = glueCatalog.loadTable(id);
    // create a new table in Glue, so that rename to that table will fail
    String newTableName = tableName + "_2";
    GLUE.createTable(
        CreateTableRequest.builder()
            .databaseName(namespace)
            .tableInput(TableInput.builder().name(newTableName).build())
            .build());
    assertThatThrownBy(
            () ->
                glueCatalog.renameTable(
                    TableIdentifier.of(namespace, tableName),
                    TableIdentifier.of(namespace, newTableName)))
        .isInstanceOf(software.amazon.awssdk.services.glue.model.AlreadyExistsException.class)
        .as("should fail to rename to an existing table")
        .hasMessageContaining("Table already exists");
    // old table can still be read with same metadata
    Table oldTable = glueCatalog.loadTable(id);
    assertThat(oldTable.location()).isEqualTo(table.location());
    assertThat(oldTable.schema()).asString().isEqualTo(table.schema().toString());
    assertThat(oldTable.spec()).isEqualTo(table.spec());
    assertThat(oldTable.currentSnapshot()).isEqualTo(table.currentSnapshot());
  }

  @Test
  public void testRenameTableFailsToDeleteOldTable() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    TableIdentifier id = TableIdentifier.of(namespace, tableName);
    Table table = glueCatalog.loadTable(id);
    // delete the old table metadata, so that drop old table will fail
    String newTableName = tableName + "_2";
    GLUE.updateTable(
        UpdateTableRequest.builder()
            .databaseName(namespace)
            .tableInput(TableInput.builder().name(tableName).parameters(Maps.newHashMap()).build())
            .build());
    assertThatThrownBy(
            () ->
                glueCatalog.renameTable(
                    TableIdentifier.of(namespace, tableName),
                    TableIdentifier.of(namespace, newTableName)))
        .isInstanceOf(ValidationException.class)
        .as("should fail to rename")
        .hasMessageContaining("Input Glue table is not an iceberg table");
    assertThatThrownBy(
            () ->
                GLUE.getTable(
                    GetTableRequest.builder().databaseName(namespace).name(newTableName).build()))
        .isInstanceOf(EntityNotFoundException.class)
        .as("renamed table should be deleted")
        .hasMessageContaining("not found");
  }

  @Test
  public void testDeleteTableWithoutPurge() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    glueCatalog.dropTable(TableIdentifier.of(namespace, tableName), false);
    assertThatThrownBy(() -> glueCatalog.loadTable(TableIdentifier.of(namespace, tableName)))
        .isInstanceOf(NoSuchTableException.class)
        .as("should not have table")
        .hasMessageContaining("Table does not exist");
    String warehouseLocation =
        glueCatalog.defaultWarehouseLocation(TableIdentifier.of(namespace, tableName));
    String prefix = warehouseLocation.split(TEST_BUCKET_NAME + "/", -1)[1];
    ListObjectsV2Response response =
        S3.listObjectsV2(
            ListObjectsV2Request.builder()
                .bucket(TEST_BUCKET_NAME)
                .prefix(prefix + "/metadata/")
                .build());
    assertThat(response.hasContents()).isTrue();
    boolean hasMetaFile = false;
    for (S3Object s3Object : response.contents()) {
      if (s3Object.key().contains(".json")) {
        hasMetaFile = true;
        break;
      }
    }
    assertThat(hasMetaFile).as("metadata json file exists after delete without purge").isTrue();
  }

  @Test
  public void testDeleteTableWithPurge() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));

    DataFile testFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/data-unpartitioned-a.parquet")
            .withFileSizeInBytes(1)
            .withRecordCount(1)
            .build();
    int numFilesToCreate = 31;
    int commitFrequency = 5;
    Transaction txn = table.newTransaction();
    for (int i = 1; i <= numFilesToCreate; i++) {
      AppendFiles appendFiles = txn.newFastAppend().appendFile(testFile);
      appendFiles.commit();
      // Every "commitFrequency" appends commit the transaction and start a new one so we can have
      // multiple manifests
      if (i % commitFrequency == 0) {
        txn.commitTransaction();
        txn = table.newTransaction();
      }
    }

    txn.commitTransaction();

    glueCatalog.dropTable(TableIdentifier.of(namespace, tableName));
    assertThatThrownBy(() -> glueCatalog.loadTable(TableIdentifier.of(namespace, tableName)))
        .isInstanceOf(NoSuchTableException.class)
        .as("should not have table")
        .hasMessageContaining("Table does not exist");
    String warehouseLocation =
        glueCatalog.defaultWarehouseLocation(TableIdentifier.of(namespace, tableName));
    String prefix = warehouseLocation.split(TEST_BUCKET_NAME + "/", -1)[1];
    ListObjectsV2Response response =
        S3.listObjectsV2(
            ListObjectsV2Request.builder().bucket(TEST_BUCKET_NAME).prefix(prefix).build());
    if (response.hasContents()) {
      // might have directory markers left
      for (S3Object s3Object : response.contents()) {
        Optional<Long> size = s3Object.getValueForField("Size", Long.class);
        assertThat(size.isPresent()).isTrue();
        assertThat(size.get()).isEqualTo(0);
      }
    }
  }

  @Test
  public void testCommitTableSkipArchive() {
    // create ns
    String namespace = getRandomName();
    NAMESPACES.add(namespace);
    glueCatalog.createNamespace(Namespace.of(namespace));
    // create table and commit without skip
    Schema schema = new Schema(Types.NestedField.required(1, "c1", Types.StringType.get(), "c1"));
    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).build();
    String tableName = getRandomName();
    AwsProperties properties = new AwsProperties();
    properties.setGlueCatalogSkipArchive(false);
    glueCatalog.initialize(
        CATALOG_NAME,
        TEST_BUCKET_PATH,
        properties,
        new S3FileIOProperties(),
        GLUE,
        LockManagers.defaultLockManager(),
        ImmutableMap.of());
    glueCatalog.createTable(TableIdentifier.of(namespace, tableName), schema, partitionSpec);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    DataFile dataFile =
        DataFiles.builder(partitionSpec)
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    table.newAppend().appendFile(dataFile).commit();
    assertThat(
            GLUE.getTableVersions(
                    GetTableVersionsRequest.builder()
                        .databaseName(namespace)
                        .tableName(tableName)
                        .build())
                .tableVersions())
        .hasSize(2);
    // create table and commit with skip
    tableName = getRandomName();
    glueCatalog.initialize(CATALOG_NAME, ImmutableMap.of());
    glueCatalog.createTable(TableIdentifier.of(namespace, tableName), schema, partitionSpec);
    table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    table.newAppend().appendFile(dataFile).commit();
    assertThat(
            GLUE.getTableVersions(
                    GetTableVersionsRequest.builder()
                        .databaseName(namespace)
                        .tableName(tableName)
                        .build())
                .tableVersions())
        .as("skipArchive should not create new version")
        .hasSize(1);
  }

  @Test
  public void testCommitTableSkipNameValidation() {
    String namespace = "dd-dd";
    NAMESPACES.add(namespace);
    glueCatalogWithSkipNameValidation.createNamespace(Namespace.of(namespace));
    String tableName = "cc-cc";
    glueCatalogWithSkipNameValidation.createTable(
        TableIdentifier.of(namespace, tableName), schema, partitionSpec, TABLE_LOCATION_PROPERTIES);
    GetTableResponse response =
        GLUE.getTable(GetTableRequest.builder().databaseName(namespace).name(tableName).build());
    assertThat(response.table().databaseName()).isEqualTo(namespace);
    assertThat(response.table().name()).isEqualTo(tableName);
  }

  @Test
  public void testColumnCommentsAndParameters() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    table
        .updateSchema()
        .addColumn(
            "c2",
            Types.StructType.of(Types.NestedField.required(3, "z", Types.IntegerType.get())),
            "c2")
        .addColumn("c3", Types.StringType.get())
        .addColumn("c4", Types.StringType.get())
        .commit();
    table.updateSpec().addField(truncate("c1", 8)).commit();
    table.updateSchema().deleteColumn("c3").renameColumn("c4", "c5").commit();
    GetTableResponse response =
        GLUE.getTable(GetTableRequest.builder().databaseName(namespace).name(tableName).build());
    List<Column> actualColumns = response.table().storageDescriptor().columns();

    List<Column> expectedColumns =
        ImmutableList.of(
            Column.builder()
                .name("c1")
                .type("string")
                .comment("c1")
                .parameters(
                    ImmutableMap.of(
                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "1",
                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "false",
                        IcebergToGlueConverter.ICEBERG_FIELD_CURRENT, "true"))
                .build(),
            Column.builder()
                .name("c2")
                .type("struct<z:int>")
                .comment("c2")
                .parameters(
                    ImmutableMap.of(
                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "2",
                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "true",
                        IcebergToGlueConverter.ICEBERG_FIELD_CURRENT, "true"))
                .build(),
            Column.builder()
                .name("c5")
                .type("string")
                .parameters(
                    ImmutableMap.of(
                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "5",
                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "true",
                        IcebergToGlueConverter.ICEBERG_FIELD_CURRENT, "true"))
                .build(),
            Column.builder()
                .name("c3")
                .type("string")
                .parameters(
                    ImmutableMap.of(
                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "4",
                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "true",
                        IcebergToGlueConverter.ICEBERG_FIELD_CURRENT, "false"))
                .build(),
            Column.builder()
                .name("c4")
                .type("string")
                .parameters(
                    ImmutableMap.of(
                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "5",
                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "true",
                        IcebergToGlueConverter.ICEBERG_FIELD_CURRENT, "false"))
                .build());
    assertThat(actualColumns).isEqualTo(expectedColumns);
  }

  @Test
  public void testGlueTableColumnCommentsPreserved() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    table
        .updateSchema()
        .addColumn("c2", Types.StringType.get())
        .addColumn("c3", Types.StringType.get())
        .commit();

    updateTableColumns(
        namespace,
        tableName,
        column -> {
          if (column.name().equals("c2") || column.name().equals("c3")) {
            return column.toBuilder().comment("updated from Glue API").build();
          } else {
            return column;
          }
        });

    table
        .updateSchema()
        .updateColumn("c2", Types.StringType.get(), "updated from Iceberg API")
        .commit();

    GetTableResponse response =
        GLUE.getTable(GetTableRequest.builder().databaseName(namespace).name(tableName).build());
    List<Column> actualColumns = response.table().storageDescriptor().columns();

    List<Column> expectedColumns =
        ImmutableList.of(
            Column.builder()
                .name("c1")
                .type("string")
                .comment("c1")
                .parameters(
                    ImmutableMap.of(
                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "1",
                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "false",
                        IcebergToGlueConverter.ICEBERG_FIELD_CURRENT, "true"))
                .build(),
            Column.builder()
                .name("c2")
                .type("string")
                .comment("updated from Iceberg API")
                .parameters(
                    ImmutableMap.of(
                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "2",
                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "true",
                        IcebergToGlueConverter.ICEBERG_FIELD_CURRENT, "true"))
                .build(),
            Column.builder()
                .name("c3")
                .type("string")
                .comment("updated from Glue API")
                .parameters(
                    ImmutableMap.of(
                        IcebergToGlueConverter.ICEBERG_FIELD_ID, "3",
                        IcebergToGlueConverter.ICEBERG_FIELD_OPTIONAL, "true",
                        IcebergToGlueConverter.ICEBERG_FIELD_CURRENT, "true"))
                .build());
    assertThat(actualColumns).isEqualTo(expectedColumns);
  }

  @Test
  public void testTablePropsDefinedAtCatalogLevel() {
    String namespace = createNamespace();
    String tableName = getRandomName();
    TableIdentifier tableIdent = TableIdentifier.of(namespace, tableName);
    ImmutableMap<String, String> catalogProps =
        ImmutableMap.of(
            "table-default.key1", "catalog-default-key1",
            "table-default.key2", "catalog-default-key2",
            "table-default.key3", "catalog-default-key3",
            "table-override.key3", "catalog-override-key3",
            "table-override.key4", "catalog-override-key4",
            "warehouse", "s3://" + TEST_BUCKET_NAME + "/" + TEST_PATH_PREFIX);

    glueCatalog.initialize("glue", catalogProps);

    Schema schema =
        new Schema(
            NestedField.required(3, "id", Types.IntegerType.get(), "unique ID"),
            NestedField.required(4, "data", Types.StringType.get()));

    Table table =
        glueCatalog
            .buildTable(tableIdent, schema)
            .withProperty("key2", "table-key2")
            .withProperty("key3", "table-key3")
            .withProperty("key5", "table-key5")
            .create();

    assertThat(table.properties())
        .as("Table defaults set for the catalog must be added to the table properties.")
        .containsEntry("key1", "catalog-default-key1")
        .as("Table property must override table default properties set at catalog level.")
        .containsEntry("key2", "table-key2")
        .as(
            "Table property override set at catalog level must override table default properties set at catalog level and table property specified.")
        .containsEntry("key3", "catalog-override-key3")
        .as("Table override not in table props or defaults should be added to table properties")
        .containsEntry("key4", "catalog-override-key4")
        .as(
            "Table properties without any catalog level default or override should be added to table properties")
        .containsEntry("key5", "table-key5");
  }

  @Test
  public void testRegisterTable() {
    String namespace = createNamespace();
    String tableName = getRandomName();
    createTable(namespace, tableName);
    TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
    Table table = glueCatalog.loadTable(identifier);
    String metadataLocation = ((BaseTable) table).operations().current().metadataFileLocation();
    assertThat(glueCatalog.dropTable(identifier, false)).isTrue();
    Table registeredTable = glueCatalog.registerTable(identifier, metadataLocation);
    assertThat(registeredTable).isNotNull();
    String expectedMetadataLocation =
        ((BaseTable) table).operations().current().metadataFileLocation();
    assertThat(metadataLocation).isEqualTo(expectedMetadataLocation);
    assertThat(glueCatalog.loadTable(identifier)).isNotNull();
    assertThat(glueCatalog.dropTable(identifier, true)).isTrue();
    assertThat(glueCatalog.dropNamespace(Namespace.of(namespace))).isTrue();
  }

  @Test
  public void testRegisterTableAlreadyExists() {
    String namespace = createNamespace();
    String tableName = getRandomName();
    createTable(namespace, tableName);
    TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
    Table table = glueCatalog.loadTable(identifier);
    String metadataLocation = ((BaseTable) table).operations().current().metadataFileLocation();
    assertThatThrownBy(() -> glueCatalog.registerTable(identifier, metadataLocation))
        .isInstanceOf(AlreadyExistsException.class);
    assertThat(glueCatalog.dropTable(identifier, true)).isTrue();
    assertThat(glueCatalog.dropNamespace(Namespace.of(namespace))).isTrue();
  }

  @Test
  public void testTableLevelS3Tags() {
    String testBucketPath = "s3://" + TEST_BUCKET_NAME + "/" + TEST_PATH_PREFIX;
    Map<String, String> properties =
        ImmutableMap.of(
            S3FileIOProperties.WRITE_TABLE_TAG_ENABLED,
            "true",
            S3FileIOProperties.WRITE_NAMESPACE_TAG_ENABLED,
            "true");
    glueCatalog.initialize(
        CATALOG_NAME,
        testBucketPath,
        new AwsProperties(properties),
        new S3FileIOProperties(properties),
        GLUE,
        null);
    String namespace = createNamespace();
    String tableName = getRandomName();
    createTable(namespace, tableName);

    // Get metadata object tag from S3
    GetTableResponse response =
        GLUE.getTable(GetTableRequest.builder().databaseName(namespace).name(tableName).build());
    String metaLocation =
        response.table().parameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
    String key = metaLocation.split(TEST_BUCKET_NAME, -1)[1].substring(1);
    List<Tag> tags =
        S3.getObjectTagging(
                GetObjectTaggingRequest.builder().bucket(TEST_BUCKET_NAME).key(key).build())
            .tagSet();
    Map<String, String> tagMap = tags.stream().collect(Collectors.toMap(Tag::key, Tag::value));

    assertThat(tagMap)
        .containsEntry(S3FileIOProperties.S3_TAG_ICEBERG_TABLE, tableName)
        .containsEntry(S3FileIOProperties.S3_TAG_ICEBERG_NAMESPACE, namespace);
  }
}
