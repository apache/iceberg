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
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
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
            .putAll(tableLocationProperties)
            .put(IcebergToGlueConverter.GLUE_DESCRIPTION_KEY, tableDescription)
            .build();
    glueCatalog.createTable(
        TableIdentifier.of(namespace, tableName), schema, partitionSpec, tableProperties);
    // verify table exists in Glue
    GetTableResponse response =
        glue.getTable(GetTableRequest.builder().databaseName(namespace).name(tableName).build());
    Assert.assertEquals(namespace, response.table().databaseName());
    Assert.assertEquals(tableName, response.table().name());
    Assert.assertEquals(
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH),
        response.table().parameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP));
    Assert.assertTrue(
        response
            .table()
            .parameters()
            .containsKey(BaseMetastoreTableOperations.METADATA_LOCATION_PROP));
    Assert.assertEquals(
        schema.columns().size(), response.table().storageDescriptor().columns().size());
    Assert.assertEquals(partitionSpec.fields().size(), response.table().partitionKeys().size());
    Assert.assertEquals(
        "additionalLocations should match",
        tableLocationProperties.values().stream().sorted().collect(Collectors.toList()),
        response.table().storageDescriptor().additionalLocations().stream()
            .sorted()
            .collect(Collectors.toList()));
    // verify metadata file exists in S3
    String metaLocation =
        response.table().parameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
    String key = metaLocation.split(testBucketName, -1)[1].substring(1);
    s3.headObject(HeadObjectRequest.builder().bucket(testBucketName).key(key).build());
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    Assert.assertEquals(partitionSpec, table.spec());
    Assert.assertEquals(schema.toString(), table.schema().toString());
    Assert.assertEquals(
        tableDescription, table.properties().get(IcebergToGlueConverter.GLUE_DESCRIPTION_KEY));
    Assert.assertEquals(tableDescription, response.table().description());
  }

  @Test
  public void testCreateTableDuplicate() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    Assertions.assertThatThrownBy(
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
    Assertions.assertThatThrownBy(
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
        catalogName,
        null,
        new AwsProperties(),
        new S3FileIOProperties(),
        glue,
        LockManagers.defaultLockManager(),
        ImmutableMap.of());
    String namespace = createNamespace();
    String tableName = getRandomName();
    TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
    try {
      glueCatalog.createTable(identifier, schema, partitionSpec, tableLocationProperties);
      glueCatalog.loadTable(identifier);
    } catch (RuntimeException e) {
      throw new RuntimeException(
          "Create and load table without warehouse location should succeed", e);
    }
  }

  @Test
  public void testListTables() {
    String namespace = createNamespace();
    Assert.assertTrue(
        "list namespace should have nothing before table creation",
        glueCatalog.listTables(Namespace.of(namespace)).isEmpty());
    String tableName = createTable(namespace);
    List<TableIdentifier> tables = glueCatalog.listTables(Namespace.of(namespace));
    Assert.assertEquals(1, tables.size());
    Assert.assertEquals(TableIdentifier.of(namespace, tableName), tables.get(0));
  }

  @Test
  public void testTableExists() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    Assert.assertTrue(glueCatalog.tableExists(TableIdentifier.of(namespace, tableName)));
  }

  @Test
  public void testUpdateTable() {
    String namespace = createNamespace();
    String tableName = getRandomName();
    // current should be null
    TableOperations ops = glueCatalog.newTableOps(TableIdentifier.of(namespace, tableName));
    TableMetadata current = ops.current();
    Assert.assertNull(current);
    // create table, refresh should update
    createTable(namespace, tableName);
    current = ops.refresh();
    Assert.assertEquals(schema.toString(), current.schema().toString());
    Assert.assertEquals(partitionSpec, current.spec());
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    Assert.assertTrue("initial table history should be empty", table.history().isEmpty());
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
    Assert.assertEquals("commit should create a new table version", 1, table.history().size());
    // check table in Glue
    GetTableResponse response =
        glue.getTable(GetTableRequest.builder().databaseName(namespace).name(tableName).build());
    Assert.assertEquals(
        "external table type is set after update", "EXTERNAL_TABLE", response.table().tableType());
    Assert.assertEquals(
        schema.columns().size(), response.table().storageDescriptor().columns().size());
    Assert.assertEquals(partitionSpec.fields().size(), response.table().partitionKeys().size());
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
    Assert.assertEquals(table.location(), renamedTable.location());
    Assert.assertEquals(table.schema().toString(), renamedTable.schema().toString());
    Assert.assertEquals(table.spec(), renamedTable.spec());
    Assert.assertEquals(table.currentSnapshot(), renamedTable.currentSnapshot());
  }

  @Test
  public void testRenameTableFailsToCreateNewTable() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    TableIdentifier id = TableIdentifier.of(namespace, tableName);
    Table table = glueCatalog.loadTable(id);
    // create a new table in Glue, so that rename to that table will fail
    String newTableName = tableName + "_2";
    glue.createTable(
        CreateTableRequest.builder()
            .databaseName(namespace)
            .tableInput(TableInput.builder().name(newTableName).build())
            .build());
    Assertions.assertThatThrownBy(
            () ->
                glueCatalog.renameTable(
                    TableIdentifier.of(namespace, tableName),
                    TableIdentifier.of(namespace, newTableName)))
        .isInstanceOf(software.amazon.awssdk.services.glue.model.AlreadyExistsException.class)
        .as("should fail to rename to an existing table")
        .hasMessageContaining("Table already exists");
    // old table can still be read with same metadata
    Table oldTable = glueCatalog.loadTable(id);
    Assert.assertEquals(table.location(), oldTable.location());
    Assert.assertEquals(table.schema().toString(), oldTable.schema().toString());
    Assert.assertEquals(table.spec(), oldTable.spec());
    Assert.assertEquals(table.currentSnapshot(), oldTable.currentSnapshot());
  }

  @Test
  public void testRenameTableFailsToDeleteOldTable() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    TableIdentifier id = TableIdentifier.of(namespace, tableName);
    Table table = glueCatalog.loadTable(id);
    // delete the old table metadata, so that drop old table will fail
    String newTableName = tableName + "_2";
    glue.updateTable(
        UpdateTableRequest.builder()
            .databaseName(namespace)
            .tableInput(TableInput.builder().name(tableName).parameters(Maps.newHashMap()).build())
            .build());
    Assertions.assertThatThrownBy(
            () ->
                glueCatalog.renameTable(
                    TableIdentifier.of(namespace, tableName),
                    TableIdentifier.of(namespace, newTableName)))
        .isInstanceOf(ValidationException.class)
        .as("should fail to rename")
        .hasMessageContaining("Input Glue table is not an iceberg table");
    Assertions.assertThatThrownBy(
            () ->
                glue.getTable(
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
    Assertions.assertThatThrownBy(
            () -> glueCatalog.loadTable(TableIdentifier.of(namespace, tableName)))
        .isInstanceOf(NoSuchTableException.class)
        .as("should not have table")
        .hasMessageContaining("Table does not exist");
    String warehouseLocation =
        glueCatalog.defaultWarehouseLocation(TableIdentifier.of(namespace, tableName));
    String prefix = warehouseLocation.split(testBucketName + "/", -1)[1];
    ListObjectsV2Response response =
        s3.listObjectsV2(
            ListObjectsV2Request.builder()
                .bucket(testBucketName)
                .prefix(prefix + "/metadata/")
                .build());
    Assert.assertTrue(response.hasContents());
    boolean hasMetaFile = false;
    for (S3Object s3Object : response.contents()) {
      if (s3Object.key().contains(".json")) {
        hasMetaFile = true;
        break;
      }
    }
    Assert.assertTrue("metadata json file exists after delete without purge", hasMetaFile);
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
    Assertions.assertThatThrownBy(
            () -> glueCatalog.loadTable(TableIdentifier.of(namespace, tableName)))
        .isInstanceOf(NoSuchTableException.class)
        .as("should not have table")
        .hasMessageContaining("Table does not exist");
    String warehouseLocation =
        glueCatalog.defaultWarehouseLocation(TableIdentifier.of(namespace, tableName));
    String prefix = warehouseLocation.split(testBucketName + "/", -1)[1];
    ListObjectsV2Response response =
        s3.listObjectsV2(
            ListObjectsV2Request.builder().bucket(testBucketName).prefix(prefix).build());
    if (response.hasContents()) {
      // might have directory markers left
      for (S3Object s3Object : response.contents()) {
        Optional<Long> size = s3Object.getValueForField("Size", Long.class);
        Assert.assertTrue(size.isPresent());
        Assert.assertEquals(0L, (long) size.get());
      }
    }
  }

  @Test
  public void testCommitTableSkipArchive() {
    // create ns
    String namespace = getRandomName();
    namespaces.add(namespace);
    glueCatalog.createNamespace(Namespace.of(namespace));
    // create table and commit without skip
    Schema schema = new Schema(Types.NestedField.required(1, "c1", Types.StringType.get(), "c1"));
    PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).build();
    String tableName = getRandomName();
    AwsProperties properties = new AwsProperties();
    properties.setGlueCatalogSkipArchive(false);
    glueCatalog.initialize(
        catalogName,
        testBucketPath,
        properties,
        new S3FileIOProperties(),
        glue,
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
    Assert.assertEquals(
        2,
        glue.getTableVersions(
                GetTableVersionsRequest.builder()
                    .databaseName(namespace)
                    .tableName(tableName)
                    .build())
            .tableVersions()
            .size());
    // create table and commit with skip
    tableName = getRandomName();
    glueCatalog.initialize(catalogName, ImmutableMap.of());
    glueCatalog.createTable(TableIdentifier.of(namespace, tableName), schema, partitionSpec);
    table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    table.newAppend().appendFile(dataFile).commit();
    Assert.assertEquals(
        "skipArchive should not create new version",
        1,
        glue.getTableVersions(
                GetTableVersionsRequest.builder()
                    .databaseName(namespace)
                    .tableName(tableName)
                    .build())
            .tableVersions()
            .size());
  }

  @Test
  public void testCommitTableSkipNameValidation() {
    String namespace = "dd-dd";
    namespaces.add(namespace);
    glueCatalogWithSkipNameValidation.createNamespace(Namespace.of(namespace));
    String tableName = "cc-cc";
    glueCatalogWithSkipNameValidation.createTable(
        TableIdentifier.of(namespace, tableName), schema, partitionSpec, tableLocationProperties);
    GetTableResponse response =
        glue.getTable(GetTableRequest.builder().databaseName(namespace).name(tableName).build());
    Assert.assertEquals(namespace, response.table().databaseName());
    Assert.assertEquals(tableName, response.table().name());
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
        glue.getTable(GetTableRequest.builder().databaseName(namespace).name(tableName).build());
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
    Assert.assertEquals("Columns do not match", expectedColumns, actualColumns);
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
            "warehouse", "s3://" + testBucketName + "/" + testPathPrefix);

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

    Assert.assertEquals(
        "Table defaults set for the catalog must be added to the table properties.",
        "catalog-default-key1",
        table.properties().get("key1"));
    Assert.assertEquals(
        "Table property must override table default properties set at catalog level.",
        "table-key2",
        table.properties().get("key2"));
    Assert.assertEquals(
        "Table property override set at catalog level must override table default"
            + " properties set at catalog level and table property specified.",
        "catalog-override-key3",
        table.properties().get("key3"));
    Assert.assertEquals(
        "Table override not in table props or defaults should be added to table properties",
        "catalog-override-key4",
        table.properties().get("key4"));
    Assert.assertEquals(
        "Table properties without any catalog level default or override should be added to table"
            + " properties.",
        "table-key5",
        table.properties().get("key5"));
  }

  @Test
  public void testRegisterTable() {
    String namespace = createNamespace();
    String tableName = getRandomName();
    createTable(namespace, tableName);
    TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
    Table table = glueCatalog.loadTable(identifier);
    String metadataLocation = ((BaseTable) table).operations().current().metadataFileLocation();
    Assertions.assertThat(glueCatalog.dropTable(identifier, false)).isTrue();
    Table registeredTable = glueCatalog.registerTable(identifier, metadataLocation);
    Assertions.assertThat(registeredTable).isNotNull();
    String expectedMetadataLocation =
        ((BaseTable) table).operations().current().metadataFileLocation();
    Assertions.assertThat(metadataLocation).isEqualTo(expectedMetadataLocation);
    Assertions.assertThat(glueCatalog.loadTable(identifier)).isNotNull();
    Assertions.assertThat(glueCatalog.dropTable(identifier, true)).isTrue();
    Assertions.assertThat(glueCatalog.dropNamespace(Namespace.of(namespace))).isTrue();
  }

  @Test
  public void testRegisterTableAlreadyExists() {
    String namespace = createNamespace();
    String tableName = getRandomName();
    createTable(namespace, tableName);
    TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
    Table table = glueCatalog.loadTable(identifier);
    String metadataLocation = ((BaseTable) table).operations().current().metadataFileLocation();
    Assertions.assertThatThrownBy(() -> glueCatalog.registerTable(identifier, metadataLocation))
        .isInstanceOf(AlreadyExistsException.class);
    Assertions.assertThat(glueCatalog.dropTable(identifier, true)).isTrue();
    Assertions.assertThat(glueCatalog.dropNamespace(Namespace.of(namespace))).isTrue();
  }

  @Test
  public void testTableLevelS3Tags() {
    String testBucketPath = "s3://" + testBucketName + "/" + testPathPrefix;
    Map<String, String> properties =
        ImmutableMap.of(
            S3FileIOProperties.WRITE_TABLE_TAG_ENABLED,
            "true",
            S3FileIOProperties.WRITE_NAMESPACE_TAG_ENABLED,
            "true");
    glueCatalog.initialize(
        catalogName,
        testBucketPath,
        new AwsProperties(properties),
        new S3FileIOProperties(properties),
        glue,
        null);
    String namespace = createNamespace();
    String tableName = getRandomName();
    createTable(namespace, tableName);

    // Get metadata object tag from S3
    GetTableResponse response =
        glue.getTable(GetTableRequest.builder().databaseName(namespace).name(tableName).build());
    String metaLocation =
        response.table().parameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
    String key = metaLocation.split(testBucketName, -1)[1].substring(1);
    List<Tag> tags =
        s3.getObjectTagging(
                GetObjectTaggingRequest.builder().bucket(testBucketName).key(key).build())
            .tagSet();
    Map<String, String> tagMap = tags.stream().collect(Collectors.toMap(Tag::key, Tag::value));

    Assert.assertTrue(tagMap.containsKey(S3FileIOProperties.S3_TAG_ICEBERG_TABLE));
    Assert.assertEquals(tableName, tagMap.get(S3FileIOProperties.S3_TAG_ICEBERG_TABLE));
    Assert.assertTrue(tagMap.containsKey(S3FileIOProperties.S3_TAG_ICEBERG_NAMESPACE));
    Assert.assertEquals(namespace, tagMap.get(S3FileIOProperties.S3_TAG_ICEBERG_NAMESPACE));
  }
}
