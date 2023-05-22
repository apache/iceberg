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
package org.apache.iceberg.flink;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.types.Row;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class TestFlinkCatalogDatabase extends FlinkCatalogTestBase {

  public TestFlinkCatalogDatabase(String catalogName, Namespace baseNamespace) {
    super(catalogName, baseNamespace);
  }

  @After
  @Override
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.tl", flinkDatabase);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
  }

  @Test
  public void testCreateNamespace() {
    Assert.assertFalse(
        "Database should not already exist",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("CREATE DATABASE %s", flinkDatabase);

    Assert.assertTrue(
        "Database should exist", validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("CREATE DATABASE IF NOT EXISTS %s", flinkDatabase);
    Assert.assertTrue(
        "Database should still exist",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    Assert.assertFalse(
        "Database should be dropped", validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("CREATE DATABASE IF NOT EXISTS %s", flinkDatabase);
    Assert.assertTrue(
        "Database should be created", validationNamespaceCatalog.namespaceExists(icebergNamespace));
  }

  @Test
  public void testDefaultDatabase() {
    sql("USE CATALOG %s", catalogName);
    sql("SHOW TABLES");

    Assert.assertEquals(
        "Should use the current catalog", getTableEnv().getCurrentCatalog(), catalogName);
    Assert.assertEquals(
        "Should use the configured default namespace",
        getTableEnv().getCurrentDatabase(),
        "default");
  }

  @Test
  public void testDropEmptyDatabase() {
    Assert.assertFalse(
        "Namespace should not already exist",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("CREATE DATABASE %s", flinkDatabase);

    Assert.assertTrue(
        "Namespace should exist", validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("DROP DATABASE %s", flinkDatabase);

    Assert.assertFalse(
        "Namespace should have been dropped",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));
  }

  @Test
  public void testDropNonEmptyNamespace() {
    Assume.assumeFalse(
        "Hadoop catalog throws IOException: Directory is not empty.", isHadoopCatalog);

    Assert.assertFalse(
        "Namespace should not already exist",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("CREATE DATABASE %s", flinkDatabase);

    validationCatalog.createTable(
        TableIdentifier.of(icebergNamespace, "tl"),
        new Schema(Types.NestedField.optional(0, "id", Types.LongType.get())));

    Assert.assertTrue(
        "Namespace should exist", validationNamespaceCatalog.namespaceExists(icebergNamespace));
    Assert.assertTrue(
        "Table should exist",
        validationCatalog.tableExists(TableIdentifier.of(icebergNamespace, "tl")));

    AssertHelpers.assertThrowsCause(
        "Should fail if trying to delete a non-empty database",
        DatabaseNotEmptyException.class,
        String.format("Database %s in catalog %s is not empty.", DATABASE, catalogName),
        () -> sql("DROP DATABASE %s", flinkDatabase));

    sql("DROP TABLE %s.tl", flinkDatabase);
  }

  @Test
  public void testListTables() {
    Assert.assertFalse(
        "Namespace should not already exist",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);

    Assert.assertTrue(
        "Namespace should exist", validationNamespaceCatalog.namespaceExists(icebergNamespace));

    Assert.assertEquals("Should not list any tables", 0, sql("SHOW TABLES").size());

    validationCatalog.createTable(
        TableIdentifier.of(icebergNamespace, "tl"),
        new Schema(Types.NestedField.optional(0, "id", Types.LongType.get())));

    List<Row> tables = sql("SHOW TABLES");
    Assert.assertEquals("Only 1 table", 1, tables.size());
    Assert.assertEquals("Table name should match", "tl", tables.get(0).getField(0));
  }

  @Test
  public void testListNamespace() {
    Assert.assertFalse(
        "Namespace should not already exist",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);

    Assert.assertTrue(
        "Namespace should exist", validationNamespaceCatalog.namespaceExists(icebergNamespace));

    List<Row> databases = sql("SHOW DATABASES");

    if (isHadoopCatalog) {
      Assert.assertEquals("Should have 2 database", 2, databases.size());
      Assert.assertEquals(
          "Should have db and default database",
          Sets.newHashSet("default", "db"),
          Sets.newHashSet(databases.get(0).getField(0), databases.get(1).getField(0)));

      if (!baseNamespace.isEmpty()) {
        // test namespace not belongs to this catalog
        validationNamespaceCatalog.createNamespace(
            Namespace.of(baseNamespace.level(0), "UNKNOWN_NAMESPACE"));
        databases = sql("SHOW DATABASES");
        Assert.assertEquals("Should have 2 database", 2, databases.size());
        Assert.assertEquals(
            "Should have db and default database",
            Sets.newHashSet("default", "db"),
            Sets.newHashSet(databases.get(0).getField(0), databases.get(1).getField(0)));
      }
    } else {
      // If there are multiple classes extends FlinkTestBase, TestHiveMetastore may loose the
      // creation for default
      // database. See HiveMetaStore.HMSHandler.init.
      Assert.assertTrue(
          "Should have db database",
          databases.stream().anyMatch(d -> Objects.equals(d.getField(0), "db")));
    }
  }

  @Test
  public void testCreateNamespaceWithMetadata() {
    Assume.assumeFalse("HadoopCatalog does not support namespace metadata", isHadoopCatalog);

    Assert.assertFalse(
        "Namespace should not already exist",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("CREATE DATABASE %s WITH ('prop'='value')", flinkDatabase);

    Assert.assertTrue(
        "Namespace should exist", validationNamespaceCatalog.namespaceExists(icebergNamespace));

    Map<String, String> nsMetadata =
        validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);

    Assert.assertEquals(
        "Namespace should have expected prop value", "value", nsMetadata.get("prop"));
  }

  @Test
  public void testCreateNamespaceWithComment() {
    Assume.assumeFalse("HadoopCatalog does not support namespace metadata", isHadoopCatalog);

    Assert.assertFalse(
        "Namespace should not already exist",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("CREATE DATABASE %s COMMENT 'namespace doc'", flinkDatabase);

    Assert.assertTrue(
        "Namespace should exist", validationNamespaceCatalog.namespaceExists(icebergNamespace));

    Map<String, String> nsMetadata =
        validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);

    Assert.assertEquals(
        "Namespace should have expected comment", "namespace doc", nsMetadata.get("comment"));
  }

  @Test
  public void testCreateNamespaceWithLocation() throws Exception {
    Assume.assumeFalse("HadoopCatalog does not support namespace metadata", isHadoopCatalog);

    Assert.assertFalse(
        "Namespace should not already exist",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));

    File location = TEMPORARY_FOLDER.newFile();
    Assert.assertTrue(location.delete());

    sql("CREATE DATABASE %s WITH ('location'='%s')", flinkDatabase, location);

    Assert.assertTrue(
        "Namespace should exist", validationNamespaceCatalog.namespaceExists(icebergNamespace));

    Map<String, String> nsMetadata =
        validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);

    Assert.assertEquals(
        "Namespace should have expected location",
        "file:" + location.getPath(),
        nsMetadata.get("location"));
  }

  @Test
  public void testSetProperties() {
    Assume.assumeFalse("HadoopCatalog does not support namespace metadata", isHadoopCatalog);

    Assert.assertFalse(
        "Namespace should not already exist",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("CREATE DATABASE %s", flinkDatabase);

    Assert.assertTrue(
        "Namespace should exist", validationNamespaceCatalog.namespaceExists(icebergNamespace));

    Map<String, String> defaultMetadata =
        validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);
    Assert.assertFalse(
        "Default metadata should not have custom property", defaultMetadata.containsKey("prop"));

    sql("ALTER DATABASE %s SET ('prop'='value')", flinkDatabase);

    Map<String, String> nsMetadata =
        validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);

    Assert.assertEquals(
        "Namespace should have expected prop value", "value", nsMetadata.get("prop"));
  }

  @Test
  public void testHadoopNotSupportMeta() {
    Assume.assumeTrue("HadoopCatalog does not support namespace metadata", isHadoopCatalog);

    Assert.assertFalse(
        "Namespace should not already exist",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));

    AssertHelpers.assertThrowsCause(
        "Should fail if trying to create database with location in hadoop catalog.",
        UnsupportedOperationException.class,
        String.format("Cannot create namespace %s: metadata is not supported", icebergNamespace),
        () -> sql("CREATE DATABASE %s WITH ('prop'='value')", flinkDatabase));
  }
}
