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
package org.apache.iceberg.spark.sql;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class TestNamespaceSQL extends SparkCatalogTestBase {
  private static final Namespace NS = Namespace.of("db");

  private final String fullNamespace;
  private final boolean isHadoopCatalog;

  public TestNamespaceSQL(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
    this.fullNamespace = ("spark_catalog".equals(catalogName) ? "" : catalogName + ".") + NS;
    this.isHadoopCatalog = "testhadoop".equals(catalogName);
  }

  @After
  public void cleanNamespaces() {
    sql("DROP TABLE IF EXISTS %s.table", fullNamespace);
    sql("DROP NAMESPACE IF EXISTS %s", fullNamespace);
  }

  @Test
  public void testCreateNamespace() {
    Assert.assertFalse(
        "Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

    sql("CREATE NAMESPACE %s", fullNamespace);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(NS));
  }

  @Test
  public void testDefaultNamespace() {
    Assume.assumeFalse("Hadoop has no default namespace configured", isHadoopCatalog);

    sql("USE %s", catalogName);

    Object[] current = Iterables.getOnlyElement(sql("SHOW CURRENT NAMESPACE"));
    Assert.assertEquals("Should use the current catalog", current[0], catalogName);
    Assert.assertEquals("Should use the configured default namespace", current[1], "default");
  }

  @Test
  public void testDropEmptyNamespace() {
    Assert.assertFalse(
        "Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

    sql("CREATE NAMESPACE %s", fullNamespace);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(NS));

    sql("DROP NAMESPACE %s", fullNamespace);

    Assert.assertFalse(
        "Namespace should have been dropped", validationNamespaceCatalog.namespaceExists(NS));
  }

  @Test
  public void testDropNonEmptyNamespace() {
    Assume.assumeFalse("Session catalog has flaky behavior", "spark_catalog".equals(catalogName));

    Assert.assertFalse(
        "Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

    sql("CREATE NAMESPACE %s", fullNamespace);
    sql("CREATE TABLE %s.table (id bigint) USING iceberg", fullNamespace);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(NS));
    Assert.assertTrue(
        "Table should exist", validationCatalog.tableExists(TableIdentifier.of(NS, "table")));

    AssertHelpers.assertThrows(
        "Should fail if trying to delete a non-empty namespace",
        NamespaceNotEmptyException.class,
        "Namespace db is not empty.",
        () -> sql("DROP NAMESPACE %s", fullNamespace));

    sql("DROP TABLE %s.table", fullNamespace);
  }

  @Test
  public void testListTables() {
    Assert.assertFalse(
        "Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

    sql("CREATE NAMESPACE %s", fullNamespace);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(NS));

    List<Object[]> rows = sql("SHOW TABLES IN %s", fullNamespace);
    Assert.assertEquals("Should not list any tables", 0, rows.size());

    sql("CREATE TABLE %s.table (id bigint) USING iceberg", fullNamespace);

    Object[] row = Iterables.getOnlyElement(sql("SHOW TABLES IN %s", fullNamespace));
    Assert.assertEquals("Namespace should match", "db", row[0]);
    Assert.assertEquals("Table name should match", "table", row[1]);
  }

  @Test
  public void testListNamespace() {
    Assert.assertFalse(
        "Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

    sql("CREATE NAMESPACE %s", fullNamespace);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(NS));

    List<Object[]> namespaces = sql("SHOW NAMESPACES IN %s", catalogName);

    if (isHadoopCatalog) {
      Assert.assertEquals("Should have 1 namespace", 1, namespaces.size());
      Set<String> namespaceNames =
          namespaces.stream().map(arr -> arr[0].toString()).collect(Collectors.toSet());
      Assert.assertEquals("Should have only db namespace", ImmutableSet.of("db"), namespaceNames);
    } else {
      Assert.assertEquals("Should have 2 namespaces", 2, namespaces.size());
      Set<String> namespaceNames =
          namespaces.stream().map(arr -> arr[0].toString()).collect(Collectors.toSet());
      Assert.assertEquals(
          "Should have default and db namespaces",
          ImmutableSet.of("default", "db"),
          namespaceNames);
    }

    List<Object[]> nestedNamespaces = sql("SHOW NAMESPACES IN %s", fullNamespace);

    Set<String> nestedNames =
        nestedNamespaces.stream().map(arr -> arr[0].toString()).collect(Collectors.toSet());
    Assert.assertEquals("Should not have nested namespaces", ImmutableSet.of(), nestedNames);
  }

  @Test
  public void testCreateNamespaceWithMetadata() {
    Assume.assumeFalse("HadoopCatalog does not support namespace metadata", isHadoopCatalog);

    Assert.assertFalse(
        "Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

    sql("CREATE NAMESPACE %s WITH PROPERTIES ('prop'='value')", fullNamespace);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(NS));

    Map<String, String> nsMetadata = validationNamespaceCatalog.loadNamespaceMetadata(NS);

    Assert.assertEquals(
        "Namespace should have expected prop value", "value", nsMetadata.get("prop"));
  }

  @Test
  public void testCreateNamespaceWithComment() {
    Assume.assumeFalse("HadoopCatalog does not support namespace metadata", isHadoopCatalog);

    Assert.assertFalse(
        "Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

    sql("CREATE NAMESPACE %s COMMENT 'namespace doc'", fullNamespace);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(NS));

    Map<String, String> nsMetadata = validationNamespaceCatalog.loadNamespaceMetadata(NS);

    Assert.assertEquals(
        "Namespace should have expected comment", "namespace doc", nsMetadata.get("comment"));
  }

  @Test
  public void testCreateNamespaceWithLocation() throws Exception {
    Assume.assumeFalse("HadoopCatalog does not support namespace locations", isHadoopCatalog);

    Assert.assertFalse(
        "Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

    File location = temp.newFile();
    Assert.assertTrue(location.delete());

    sql("CREATE NAMESPACE %s LOCATION '%s'", fullNamespace, location);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(NS));

    Map<String, String> nsMetadata = validationNamespaceCatalog.loadNamespaceMetadata(NS);

    Assert.assertEquals(
        "Namespace should have expected location",
        "file:" + location.getPath(),
        nsMetadata.get("location"));
  }

  @Test
  public void testSetProperties() {
    Assume.assumeFalse("HadoopCatalog does not support namespace metadata", isHadoopCatalog);

    Assert.assertFalse(
        "Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

    sql("CREATE NAMESPACE %s", fullNamespace);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(NS));

    Map<String, String> defaultMetadata = validationNamespaceCatalog.loadNamespaceMetadata(NS);
    Assert.assertFalse(
        "Default metadata should not have custom property", defaultMetadata.containsKey("prop"));

    sql("ALTER NAMESPACE %s SET PROPERTIES ('prop'='value')", fullNamespace);

    Map<String, String> nsMetadata = validationNamespaceCatalog.loadNamespaceMetadata(NS);

    Assert.assertEquals(
        "Namespace should have expected prop value", "value", nsMetadata.get("prop"));
  }
}
