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

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTableVersionsRequest;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class GlueCatalogTableTest extends GlueTestBase {

  @Test
  public void testCreateTable() {
    String namespace = createNamespace();
    String tableName = getRandomName();
    glueCatalog.createTable(TableIdentifier.of(namespace, tableName), schema, partitionSpec);
    // verify table exists in Glue
    GetTableResponse response = glue.getTable(GetTableRequest.builder()
        .databaseName(namespace).name(tableName).build());
    Assert.assertEquals(namespace, response.table().databaseName());
    Assert.assertEquals(tableName, response.table().name());
    Assert.assertEquals(BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH),
        response.table().parameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP));
    Assert.assertTrue(response.table().parameters().containsKey(BaseMetastoreTableOperations.METADATA_LOCATION_PROP));
    // verify metadata file exists in S3
    String metaLocation = response.table().parameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
    String key = metaLocation.split(testBucketName, -1)[1].substring(1);
    s3.headObject(HeadObjectRequest.builder().bucket(testBucketName).key(key).build());
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    Assert.assertEquals(partitionSpec, table.spec());
    Assert.assertEquals(schema.toString(), table.schema().toString());
  }

  @Test
  public void testCreateTableDuplicate() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    AssertHelpers.assertThrows("should not create table with the same name",
        AlreadyExistsException.class,
        "Table already exists",
        () -> glueCatalog.createTable(TableIdentifier.of(namespace, tableName), schema, partitionSpec));
  }

  @Test
  public void testCreateTableBadName() {
    String namespace = createNamespace();
    AssertHelpers.assertThrows("should not create table with bad name",
        IllegalArgumentException.class,
        "Invalid table identifier",
        () -> glueCatalog.createTable(TableIdentifier.of(namespace, "table-1"), schema, partitionSpec));
  }

  @Test
  public void testListTables() {
    String namespace = createNamespace();
    Assert.assertTrue("list namespace should have nothing before table creation",
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
    DataFile dataFile = DataFiles.builder(partitionSpec)
        .withPath("/path/to/data-a.parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
    table.newAppend().appendFile(dataFile).commit();
    table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    Assert.assertEquals("commit should create a new table version", 1, table.history().size());
    // check table in Glue
    GetTableResponse response = glue.getTable(GetTableRequest.builder()
        .databaseName(namespace).name(tableName).build());
    Assert.assertEquals("external table type is set after update", "EXTERNAL_TABLE", response.table().tableType());
  }

  @Test
  public void testRenameTable() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    // rename table
    String newTableName = tableName + "_2";
    glueCatalog.renameTable(TableIdentifier.of(namespace, tableName), TableIdentifier.of(namespace, newTableName));
    Table renamedTable = glueCatalog.loadTable(TableIdentifier.of(namespace, newTableName));
    Assert.assertEquals(table.location(), renamedTable.location());
    Assert.assertEquals(table.schema().toString(), renamedTable.schema().toString());
    Assert.assertEquals(table.spec(), renamedTable.spec());
    Assert.assertEquals(table.currentSnapshot(), renamedTable.currentSnapshot());
  }

  @Test
  public void testRenameTable_failToCreateNewTable() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    TableIdentifier id = TableIdentifier.of(namespace, tableName);
    Table table = glueCatalog.loadTable(id);
    // create a new table in Glue, so that rename to that table will fail
    String newTableName = tableName + "_2";
    glue.createTable(CreateTableRequest.builder()
        .databaseName(namespace)
        .tableInput(TableInput.builder().name(newTableName).build())
        .build());
    AssertHelpers.assertThrows("should fail to rename to an existing table",
        software.amazon.awssdk.services.glue.model.AlreadyExistsException.class,
        "Table already exists",
        () -> glueCatalog.renameTable(
            TableIdentifier.of(namespace, tableName), TableIdentifier.of(namespace, newTableName)));
    // old table can still be read with same metadata
    Table oldTable = glueCatalog.loadTable(id);
    Assert.assertEquals(table.location(), oldTable.location());
    Assert.assertEquals(table.schema().toString(), oldTable.schema().toString());
    Assert.assertEquals(table.spec(), oldTable.spec());
    Assert.assertEquals(table.currentSnapshot(), oldTable.currentSnapshot());
  }

  @Test
  public void testRenameTable_failToDeleteOldTable() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    TableIdentifier id = TableIdentifier.of(namespace, tableName);
    Table table = glueCatalog.loadTable(id);
    // delete the old table metadata, so that drop old table will fail
    String newTableName = tableName + "_2";
    glue.updateTable(UpdateTableRequest.builder()
        .databaseName(namespace)
        .tableInput(TableInput.builder().name(tableName).parameters(Maps.newHashMap()).build())
        .build());
    AssertHelpers.assertThrows("should fail to rename",
        ValidationException.class,
        "Input Glue table is not an iceberg table",
        () -> glueCatalog.renameTable(
            TableIdentifier.of(namespace, tableName), TableIdentifier.of(namespace, newTableName)));
    AssertHelpers.assertThrows("renamed table should be deleted",
        EntityNotFoundException.class,
        "not found",
        () -> glue.getTable(GetTableRequest.builder().databaseName(namespace).name(newTableName).build()));
  }

  @Test
  public void testDeleteTableWithoutPurge() {
    String namespace = createNamespace();
    String tableName = createTable(namespace);
    glueCatalog.dropTable(TableIdentifier.of(namespace, tableName), false);
    AssertHelpers.assertThrows("should not have table",
        NoSuchTableException.class,
        "Table does not exist",
        () -> glueCatalog.loadTable(TableIdentifier.of(namespace, tableName)));
    String warehouseLocation = glueCatalog.defaultWarehouseLocation(TableIdentifier.of(namespace, tableName));
    String prefix = warehouseLocation.split(testBucketName + "/", -1)[1];
    ListObjectsV2Response response = s3.listObjectsV2(ListObjectsV2Request.builder()
        .bucket(testBucketName).prefix(prefix + "/metadata/").build());
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
    glueCatalog.dropTable(TableIdentifier.of(namespace, tableName));
    AssertHelpers.assertThrows("should not have table",
        NoSuchTableException.class,
        "Table does not exist",
        () -> glueCatalog.loadTable(TableIdentifier.of(namespace, tableName)));
    String warehouseLocation = glueCatalog.defaultWarehouseLocation(TableIdentifier.of(namespace, tableName));
    String prefix = warehouseLocation.split(testBucketName + "/", -1)[1];
    ListObjectsV2Response response = s3.listObjectsV2(ListObjectsV2Request.builder()
        .bucket(testBucketName).prefix(prefix).build());
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
    glueCatalog.createTable(TableIdentifier.of(namespace, tableName), schema, partitionSpec);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));
    DataFile dataFile = DataFiles.builder(partitionSpec)
        .withPath("/path/to/data-a.parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
    table.newAppend().appendFile(dataFile).commit();
    Assert.assertEquals(2, glue.getTableVersions(GetTableVersionsRequest.builder()
        .databaseName(namespace).tableName(tableName).build()).tableVersions().size());
    // create table and commit with skip
    tableName = getRandomName();
    glueCatalogWithSkip.createTable(TableIdentifier.of(namespace, tableName), schema, partitionSpec);
    table = glueCatalogWithSkip.loadTable(TableIdentifier.of(namespace, tableName));
    table.newAppend().appendFile(dataFile).commit();
    Assert.assertEquals("skipArchive should not create new version",
        1, glue.getTableVersions(GetTableVersionsRequest.builder()
            .databaseName(namespace).tableName(tableName).build()).tableVersions().size());
  }
}
