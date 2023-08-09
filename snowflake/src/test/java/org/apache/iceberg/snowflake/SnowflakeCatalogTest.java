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

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SnowflakeCatalogTest {

  private static final String TEST_CATALOG_NAME = "slushLog";
  private SnowflakeCatalog catalog;
  private FakeSnowflakeClient fakeClient;
  private InMemoryFileIO fakeFileIO;
  private SnowflakeCatalog.FileIOFactory fakeFileIOFactory;
  private Map<String, String> properties;

  @BeforeEach
  public void before() {
    catalog = new SnowflakeCatalog();

    fakeClient = new FakeSnowflakeClient();
    fakeClient.addTable(
        SnowflakeIdentifier.ofTable("DB_1", "SCHEMA_1", "TAB_1"),
        SnowflakeTableMetadata.parseJson(
            "{\"metadataLocation\":\"s3://tab1/metadata/v3.metadata.json\",\"status\":\"success\"}"));
    fakeClient.addTable(
        SnowflakeIdentifier.ofTable("DB_1", "SCHEMA_1", "TAB_2"),
        SnowflakeTableMetadata.parseJson(
            "{\"metadataLocation\":\"s3://tab2/metadata/v1.metadata.json\",\"status\":\"success\"}"));
    fakeClient.addTable(
        SnowflakeIdentifier.ofTable("DB_2", "SCHEMA_2", "TAB_3"),
        SnowflakeTableMetadata.parseJson(
            "{\"metadataLocation\":\"azure://myaccount.blob.core.windows.net/mycontainer/tab3/metadata/v334.metadata.json\",\"status\":\"success\"}"));
    fakeClient.addTable(
        SnowflakeIdentifier.ofTable("DB_2", "SCHEMA_2", "TAB_4"),
        SnowflakeTableMetadata.parseJson(
            "{\"metadataLocation\":\"azure://myaccount.blob.core.windows.net/mycontainer/tab4/metadata/v323.metadata.json\",\"status\":\"success\"}"));
    fakeClient.addTable(
        SnowflakeIdentifier.ofTable("DB_3", "SCHEMA_3", "TAB_5"),
        SnowflakeTableMetadata.parseJson(
            "{\"metadataLocation\":\"gcs://tab5/metadata/v793.metadata.json\",\"status\":\"success\"}"));
    fakeClient.addTable(
        SnowflakeIdentifier.ofTable("DB_3", "SCHEMA_4", "TAB_6"),
        SnowflakeTableMetadata.parseJson(
            "{\"metadataLocation\":\"gcs://tab6/metadata/v123.metadata.json\",\"status\":\"success\"}"));

    fakeFileIO = new InMemoryFileIO();

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
                    schema, partitionSpec, "s3://tab1", ImmutableMap.<String, String>of()))
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

    fakeFileIOFactory =
        new SnowflakeCatalog.FileIOFactory() {
          @Override
          public FileIO newFileIO(String impl, Map<String, String> prop, Object hadoopConf) {
            return fakeFileIO;
          }
        };

    properties = Maps.newHashMap();
    catalog.initialize(TEST_CATALOG_NAME, fakeClient, fakeFileIOFactory, properties);
  }

  @Test
  public void testInitializeNullClient() {
    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () -> catalog.initialize(TEST_CATALOG_NAME, null, fakeFileIOFactory, properties))
        .withMessageContaining("snowflakeClient must be non-null");
  }

  @Test
  public void testInitializeNullFileIO() {
    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> catalog.initialize(TEST_CATALOG_NAME, fakeClient, null, properties))
        .withMessageContaining("fileIOFactory must be non-null");
  }

  @Test
  public void testListNamespaceInRoot() {
    Assertions.assertThat(catalog.listNamespaces())
        .containsExactly(Namespace.of("DB_1"), Namespace.of("DB_2"), Namespace.of("DB_3"));
  }

  @Test
  public void testListNamespaceWithinDB() {
    String dbName = "DB_1";
    Assertions.assertThat(catalog.listNamespaces(Namespace.of(dbName)))
        .containsExactly(Namespace.of(dbName, "SCHEMA_1"));
  }

  @Test
  public void testListNamespaceWithinNonExistentDB() {
    // Existence check for nonexistent parent namespaces is optional in the SupportsNamespaces
    // interface.
    String dbName = "NONEXISTENT_DB";
    Assertions.assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> catalog.listNamespaces(Namespace.of(dbName)))
        .withMessageContaining("does not exist")
        .withMessageContaining(dbName);
  }

  @Test
  public void testListNamespaceWithinSchema() {
    // No "sub-namespaces" beyond database.schema; invalid to try to list namespaces given
    // a database.schema.
    String dbName = "DB_3";
    String schemaName = "SCHEMA_4";
    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> catalog.listNamespaces(Namespace.of(dbName, schemaName)))
        .withMessageContaining("level")
        .withMessageContaining("DB_3.SCHEMA_4");
  }

  @Test
  public void testListTables() {
    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> catalog.listTables(Namespace.empty()))
        .withMessageContaining("listTables must be at SCHEMA level");
  }

  @Test
  public void testListTablesWithinDB() {
    String dbName = "DB_1";
    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> catalog.listTables(Namespace.of(dbName)))
        .withMessageContaining("listTables must be at SCHEMA level");
  }

  @Test
  public void testListTablesWithinNonexistentDB() {
    String dbName = "NONEXISTENT_DB";
    String schemaName = "NONEXISTENT_SCHEMA";
    Assertions.assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> catalog.listTables(Namespace.of(dbName, schemaName)))
        .withMessageContaining("does not exist")
        .withMessageContaining(dbName);
  }

  @Test
  public void testListTablesWithinSchema() {
    String dbName = "DB_2";
    String schemaName = "SCHEMA_2";
    Assertions.assertThat(catalog.listTables(Namespace.of(dbName, schemaName)))
        .containsExactly(
            TableIdentifier.of("DB_2", "SCHEMA_2", "TAB_3"),
            TableIdentifier.of("DB_2", "SCHEMA_2", "TAB_4"));
  }

  @Test
  public void testListTablesWithinNonexistentSchema() {
    String dbName = "DB_2";
    String schemaName = "NONEXISTENT_SCHEMA";
    Assertions.assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> catalog.listTables(Namespace.of(dbName, schemaName)))
        .withMessageContaining("does not exist")
        .withMessageContaining("DB_2.NONEXISTENT_SCHEMA");
  }

  @Test
  public void testLoadS3Table() {
    Table table = catalog.loadTable(TableIdentifier.of(Namespace.of("DB_1", "SCHEMA_1"), "TAB_1"));
    Assertions.assertThat(table.location()).isEqualTo("s3://tab1");
  }

  @Test
  public void testLoadAzureTable() {
    Table table = catalog.loadTable(TableIdentifier.of(Namespace.of("DB_2", "SCHEMA_2"), "TAB_3"));
    Assertions.assertThat(table.location())
        .isEqualTo("wasbs://mycontainer@myaccount.blob.core.windows.net/tab1");
  }

  @Test
  public void testLoadGcsTable() {
    Table table = catalog.loadTable(TableIdentifier.of(Namespace.of("DB_3", "SCHEMA_3"), "TAB_5"));
    Assertions.assertThat(table.location()).isEqualTo("gs://tab5");
  }

  @Test
  public void testLoadTableWithMalformedTableIdentifier() {
    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                catalog.loadTable(
                    TableIdentifier.of(Namespace.of("DB_1", "SCHEMA_1", "BAD_NS_LEVEL"), "TAB_1")))
        .withMessageContaining("level")
        .withMessageContaining("DB_1.SCHEMA_1.BAD_NS_LEVEL");
    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () -> catalog.loadTable(TableIdentifier.of(Namespace.of("DB_WITHOUT_SCHEMA"), "TAB_1")))
        .withMessageContaining("level")
        .withMessageContaining("DB_WITHOUT_SCHEMA.TAB_1");
  }

  @Test
  public void testCloseBeforeInitializeDoesntThrow() throws IOException {
    catalog = new SnowflakeCatalog();

    // Make sure no exception is thrown if we call close() before initialize(), in case callers
    // add a catalog to auto-close() helpers but end up never using/initializing a catalog.
    catalog.close();

    Assertions.assertThat(fakeClient.isClosed())
        .overridingErrorMessage("expected not to have called close() on snowflakeClient")
        .isFalse();
  }

  @Test
  public void testClose() throws IOException {
    catalog.newTableOps(TableIdentifier.of("DB_1", "SCHEMA_1", "TAB_1"));
    catalog.close();
    Assertions.assertThat(fakeClient.isClosed())
        .overridingErrorMessage("expected close() to propagate to snowflakeClient")
        .isTrue();
    Assertions.assertThat(fakeFileIO.isClosed())
        .overridingErrorMessage("expected close() to propagate to fileIO")
        .isTrue();
  }

  @Test
  public void testTableNameFromTableOperations() {
    SnowflakeTableOperations castedTableOps =
        (SnowflakeTableOperations)
            catalog.newTableOps(TableIdentifier.of("DB_1", "SCHEMA_1", "TAB_1"));
    Assertions.assertThat(castedTableOps.fullTableName()).isEqualTo("slushLog.DB_1.SCHEMA_1.TAB_1");
  }

  @Test
  public void testDatabaseExists() {
    Assertions.assertThat(catalog.namespaceExists(Namespace.of("DB_1"))).isTrue();
    Assertions.assertThat(catalog.namespaceExists(Namespace.of("NONEXISTENT_DB"))).isFalse();
  }

  @Test
  public void testSchemaExists() {
    Assertions.assertThat(catalog.namespaceExists(Namespace.of("DB_1", "SCHEMA_1"))).isTrue();
    Assertions.assertThat(catalog.namespaceExists(Namespace.of("DB_1", "NONEXISTENT_SCHEMA")))
        .isFalse();
    Assertions.assertThat(catalog.namespaceExists(Namespace.of("NONEXISTENT_DB", "SCHEMA_1")))
        .isFalse();
  }
}
