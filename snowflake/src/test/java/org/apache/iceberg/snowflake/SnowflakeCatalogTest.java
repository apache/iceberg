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
package org.apache.iceberg.snowflake;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.snowflake.entities.SnowflakeTableMetadata;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SnowflakeCatalogTest {

  static final String TEST_CATALOG_NAME = "slushLog";
  private SnowflakeCatalog catalog;

  @Before
  public void before() {
    catalog = new SnowflakeCatalog();

    FakeSnowflakeClient client = new FakeSnowflakeClient();
    client.addTable(
        "DB_1",
        "SCHEMA_1",
        "TAB_1",
        SnowflakeTableMetadata.parseJson(
            "{\"metadataLocation\":\"s3://tab1/metadata/v3.metadata.json\",\"status\":\"success\"}"));
    client.addTable(
        "DB_1",
        "SCHEMA_1",
        "TAB_2",
        SnowflakeTableMetadata.parseJson(
            "{\"metadataLocation\":\"s3://tab2/metadata/v1.metadata.json\",\"status\":\"success\"}"));
    client.addTable(
        "DB_2",
        "SCHEMA_2",
        "TAB_3",
        SnowflakeTableMetadata.parseJson(
            "{\"metadataLocation\":\"azure://myaccount.blob.core.windows.net/mycontainer/tab3/metadata/v334.metadata.json\",\"status\":\"success\"}"));
    client.addTable(
        "DB_2",
        "SCHEMA_2",
        "TAB_4",
        SnowflakeTableMetadata.parseJson(
            "{\"metadataLocation\":\"azure://myaccount.blob.core.windows.net/mycontainer/tab4/metadata/v323.metadata.json\",\"status\":\"success\"}"));
    client.addTable(
        "DB_3",
        "SCHEMA_3",
        "TAB_5",
        SnowflakeTableMetadata.parseJson(
            "{\"metadataLocation\":\"gcs://tab5/metadata/v793.metadata.json\",\"status\":\"success\"}"));
    client.addTable(
        "DB_3",
        "SCHEMA_4",
        "TAB_6",
        SnowflakeTableMetadata.parseJson(
            "{\"metadataLocation\":\"gcs://tab6/metadata/v123.metadata.json\",\"status\":\"success\"}"));

    catalog.setSnowflakeClient(client);

    InMemoryFileIO fakeFileIO = new InMemoryFileIO();

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "x", Types.StringType.get(), "comment1"),
            Types.NestedField.required(2, "y", Types.StringType.get(), "comment2"));
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(schema).identity("x").withSpecId(1000).build();
    fakeFileIO.addFile(
        "s3://tab1/metadata/v3.metadata.json",
        TableMetadataParser.toJson(
                TableMetadata.newTableMetadata(
                    schema, partitionSpec, "s3://tab1/", ImmutableMap.<String, String>of()))
            .getBytes());
    fakeFileIO.addFile(
        "wasbs://mycontainer@myaccount.blob.core.windows.net/tab3/metadata/v334.metadata.json",
        TableMetadataParser.toJson(
                TableMetadata.newTableMetadata(
                    schema,
                    partitionSpec,
                    "wasbs://mycontainer@myaccount.blob.core.windows.net/tab1/",
                    ImmutableMap.<String, String>of()))
            .getBytes());
    fakeFileIO.addFile(
        "gs://tab5/metadata/v793.metadata.json",
        TableMetadataParser.toJson(
                TableMetadata.newTableMetadata(
                    schema, partitionSpec, "gs://tab5/", ImmutableMap.<String, String>of()))
            .getBytes());

    catalog.setFileIO(fakeFileIO);

    Map<String, String> properties = Maps.newHashMap();
    catalog.initialize(TEST_CATALOG_NAME, properties);
  }

  @Test
  public void testListNamespace() {
    List<Namespace> namespaces = catalog.listNamespaces();
    Assert.assertEquals(
        Lists.newArrayList(
            Namespace.of("DB_1", "SCHEMA_1"),
            Namespace.of("DB_2", "SCHEMA_2"),
            Namespace.of("DB_3", "SCHEMA_3"),
            Namespace.of("DB_3", "SCHEMA_4")),
        namespaces);
  }

  @Test
  public void testListNamespaceWithinDB() {
    String dbName = "DB_1";
    List<Namespace> namespaces = catalog.listNamespaces(Namespace.of(dbName));
    Assert.assertEquals(Lists.newArrayList(Namespace.of(dbName, "SCHEMA_1")), namespaces);
  }

  @Test
  public void testListNamespaceWithinNonExistentDB() {
    // Existence check for nonexistent parent namespaces is optional in the SupportsNamespaces
    // interface.
    String dbName = "NONEXISTENT_DB";
    Assert.assertThrows(RuntimeException.class, () -> catalog.listNamespaces(Namespace.of(dbName)));
  }

  @Test
  public void testListNamespaceWithinSchema() {
    // No "sub-namespaces" beyond database.schema; invalid to try to list namespaces given
    // a database.schema.
    String dbName = "DB_3";
    String schemaName = "SCHEMA_4";
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> catalog.listNamespaces(Namespace.of(dbName, schemaName)));
  }

  @Test
  public void testListTables() {
    List<TableIdentifier> tables = catalog.listTables(Namespace.empty());
    Assert.assertEquals(
        Lists.newArrayList(
            TableIdentifier.of("DB_1", "SCHEMA_1", "TAB_1"),
            TableIdentifier.of("DB_1", "SCHEMA_1", "TAB_2"),
            TableIdentifier.of("DB_2", "SCHEMA_2", "TAB_3"),
            TableIdentifier.of("DB_2", "SCHEMA_2", "TAB_4"),
            TableIdentifier.of("DB_3", "SCHEMA_3", "TAB_5"),
            TableIdentifier.of("DB_3", "SCHEMA_4", "TAB_6")),
        tables);
  }

  @Test
  public void testListTablesWithinDB() {
    String dbName = "DB_1";
    List<TableIdentifier> tables = catalog.listTables(Namespace.of(dbName));
    Assert.assertEquals(
        Lists.newArrayList(
            TableIdentifier.of("DB_1", "SCHEMA_1", "TAB_1"),
            TableIdentifier.of("DB_1", "SCHEMA_1", "TAB_2")),
        tables);
  }

  @Test
  public void testListTablesWithinNonexistentDB() {
    String dbName = "NONEXISTENT_DB";
    Assert.assertThrows(RuntimeException.class, () -> catalog.listTables(Namespace.of(dbName)));
  }

  @Test
  public void testListTablesWithinSchema() {
    String dbName = "DB_2";
    String schemaName = "SCHEMA_2";
    List<TableIdentifier> tables = catalog.listTables(Namespace.of(dbName, schemaName));
    Assert.assertEquals(
        Lists.newArrayList(
            TableIdentifier.of("DB_2", "SCHEMA_2", "TAB_3"),
            TableIdentifier.of("DB_2", "SCHEMA_2", "TAB_4")),
        tables);
  }

  @Test
  public void testListTablesWithinNonexistentSchema() {
    String dbName = "DB_2";
    String schemaName = "NONEXISTENT_DB";
    Assert.assertThrows(
        RuntimeException.class, () -> catalog.listTables(Namespace.of(dbName, schemaName)));
  }

  @Test
  public void testLoadS3Table() {
    Table table = catalog.loadTable(TableIdentifier.of(Namespace.of("DB_1", "SCHEMA_1"), "TAB_1"));
    Assert.assertEquals(table.location(), "s3://tab1/");
  }

  @Test
  public void testLoadAzureTable() {
    Table table = catalog.loadTable(TableIdentifier.of(Namespace.of("DB_2", "SCHEMA_2"), "TAB_3"));
    Assert.assertEquals(
        table.location(), "wasbs://mycontainer@myaccount.blob.core.windows.net/tab1/");
  }

  @Test
  public void testLoadGcsTable() {
    Table table = catalog.loadTable(TableIdentifier.of(Namespace.of("DB_3", "SCHEMA_3"), "TAB_5"));
    Assert.assertEquals(table.location(), "gs://tab5/");
  }
}
