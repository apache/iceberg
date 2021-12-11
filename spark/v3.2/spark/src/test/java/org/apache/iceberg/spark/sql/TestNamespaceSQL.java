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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.spark.SparkException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class TestNamespaceSQL extends SparkCatalogTestBase {
  private static final Namespace NS = Namespace.of("db");
  private static final Namespace NESTED_NS1 = Namespace.of("db", "ns1");
  private static final Namespace NESTED_NS2 = Namespace.of("db", "ns2");
  private static final Namespace DOUBLE_NESTED_NS = Namespace.of("db", "ns2", "ns3");

  private final String fullNamespace;
  private final String fullNestedNamespace1;
  private final String fullNestedNamespace2;
  private final String fullDoubleNestedNamespace;
  private final boolean isHadoopCatalog;

  public TestNamespaceSQL(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
    this.fullNamespace = fullNamespaceFor(NS);
    this.fullNestedNamespace1 = fullNamespaceFor(NESTED_NS1);
    this.fullNestedNamespace2 = fullNamespaceFor(NESTED_NS2);
    this.fullDoubleNestedNamespace = fullNamespaceFor(DOUBLE_NESTED_NS);
    this.isHadoopCatalog = "testhadoop".equals(catalogName);
  }

  @After
  public void cleanNamespaces() {
    sql("DROP TABLE IF EXISTS %s.table", fullNamespace);
    sql("DROP NAMESPACE IF EXISTS %s", fullNamespace);
  }

  @Test
  public void testCreateNamespace() {
    Assert.assertFalse("Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

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
    Assert.assertFalse("Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

    sql("CREATE NAMESPACE %s", fullNamespace);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(NS));

    sql("DROP NAMESPACE %s", fullNamespace);

    Assert.assertFalse("Namespace should have been dropped", validationNamespaceCatalog.namespaceExists(NS));
  }

  @Test
  public void testDropNonEmptyNamespace() {
    Assert.assertFalse("Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

    sql("CREATE NAMESPACE %s", fullNamespace);
    sql("CREATE TABLE %s.table (id bigint) USING iceberg", fullNamespace);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(NS));
    Assert.assertTrue("Table should exist", validationCatalog.tableExists(TableIdentifier.of(NS, "table")));

    AssertHelpers.assertThrows("Should fail if trying to delete a non-empty namespace",
        SparkException.class, "non-empty namespace",
        () -> sql("DROP NAMESPACE %s", fullNamespace));

    sql("DROP TABLE %s.table", fullNamespace);
  }

  @Test
  public void testDropNonEmptyNamespaceWithSomeEmptySubNamespaces() {
    Assume.assumeTrue("Only hadoop catalog allows for multi-level namespaces", isHadoopCatalog);

    Assert.assertFalse("Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

    sql("CREATE NAMESPACE %s", fullNamespace);
    sql("CREATE NAMESPACE %s", fullNestedNamespace2);
    sql("CREATE NAMESPACE %s", fullDoubleNestedNamespace);
    sql("CREATE NAMESPACE %s", fullNestedNamespace1);
    sql("CREATE TABLE %s.table (id bigint) USING iceberg", fullDoubleNestedNamespace);

    // Sanity check
    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(NS)).isTrue();
    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(NESTED_NS1)).isTrue();
    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(NESTED_NS2)).isTrue();
    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(DOUBLE_NESTED_NS)).isTrue();
    Assertions.assertThat(validationCatalog.tableExists(TableIdentifier.of(DOUBLE_NESTED_NS, "table"))).isTrue();

    AssertHelpers.assertThrows("Should fail if trying to delete a non-empty namespace",
        SparkException.class, "non-empty namespace",
        () -> sql("DROP NAMESPACE %s", fullNamespace));

    // Ensure we had no partial drops
    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(NS)).isTrue();
    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(NESTED_NS1)).isTrue();
    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(NESTED_NS2)).isTrue();
    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(DOUBLE_NESTED_NS)).isTrue();
    Assertions.assertThat(validationCatalog.tableExists(TableIdentifier.of(DOUBLE_NESTED_NS, "table"))).isTrue();

    // Full quick clean-up
    Assertions.assertThatCode(() -> sql("DROP NAMESPACE %s CASCADE", fullNamespace))
        .doesNotThrowAnyException();
  }

  @Test
  public void testDropNonEmptyNamespaceOnCascade() {
    Assert.assertFalse("Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

    sql("CREATE NAMESPACE %s", fullNamespace);
    sql("CREATE TABLE %s.table (id bigint) USING iceberg", fullNamespace);
    sql("INSERT INTO %s.table VALUES (1), (2), (3), (4), (5)", fullNamespace);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(NS));
    Assert.assertTrue("Table should exist", validationCatalog.tableExists(TableIdentifier.of(NS, "table")));

    AssertHelpers.assertThrows(
        "Should fail if trying to delete a non-empty namespace",
        SparkException.class, "non-empty namespace",
        () -> sql("DROP NAMESPACE %s", fullNamespace));

    Assert.assertTrue("Namespace should still exist after a failed drop",
        validationNamespaceCatalog.namespaceExists(NS));
    Assert.assertTrue("Table should still exist after a failed drop",
        validationCatalog.tableExists(TableIdentifier.of(NS, "table")));

    sql("DROP NAMESPACE %s CASCADE", fullNamespace);

    Assert.assertFalse(
        "Namespace should not exist after deleting with CASCADE",
        validationNamespaceCatalog.namespaceExists(NS));
    Assert.assertFalse(
        "Table should not exist after deleting its parent namespace with CASCADE",
        validationCatalog.tableExists(TableIdentifier.of(NS, "table")));
  }

  @Test
  public void testDropNamespaceRespectsRestrictKeyword() {
    Assert.assertFalse("Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

    sql("CREATE NAMESPACE %s", fullNamespace);
    sql("CREATE TABLE %s.table (id bigint) USING iceberg", fullNamespace);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(NS));
    Assert.assertTrue("Table should exist", validationCatalog.tableExists(TableIdentifier.of(NS, "table")));

    AssertHelpers.assertThrows("Should fail if trying to delete a non-empty namespace with RESTRICT",
        SparkException.class, "non-empty namespace",
        () -> sql("DROP NAMESPACE %s RESTRICT", fullNamespace));

    Assert.assertTrue("Non-empty namespace should not drop with RESTRICT",
        validationNamespaceCatalog.namespaceExists(NS));
    Assert.assertTrue("Table should still exist after a failed drop",
        validationCatalog.tableExists(TableIdentifier.of(NS, "table")));

    // Drop the table and then try using RESTRICT again.
    sql("DROP TABLE %s.table", fullNamespace);
    Assert.assertFalse(validationCatalog.tableExists(TableIdentifier.of(NS, "table")));

    sql("DROP NAMESPACE %s RESTRICT", fullNamespace);
    Assert.assertFalse(
        "Namespace should get dropped when using RESTRICT if it's empty",
        validationNamespaceCatalog.namespaceExists(NS));
  }

  @Test
  public void testDropNamespaceIfExists() {
    Assert.assertFalse("Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

    Assertions
        .assertThatCode(() -> sql("DROP NAMESPACE IF EXISTS %s", NS))
        .doesNotThrowAnyException();

    Assertions
        .assertThat(sql("DROP NAMESPACE IF EXISTS %s", NS))
        .isEqualTo(ImmutableList.of());

    AssertHelpers.assertThrows(
        "Attempting to drop a non-existing namespace without IF EXISTS should throw",
        NoSuchNamespaceException.class, "not found",
        () -> sql("DROP NAMESPACE %s", NS));

    sql("CREATE NAMESPACE %s", fullNamespace);
    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(NS));

    sql("DROP NAMESPACE IF EXISTS %s", fullNamespace);
    Assert.assertFalse("Namespace should no longer exist", validationNamespaceCatalog.namespaceExists(NS));
  }

  @Test
  public void testListTables() {
    Assert.assertFalse("Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

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
    Assert.assertFalse("Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

    sql("CREATE NAMESPACE %s", fullNamespace);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(NS));

    List<Object[]> namespaces = sql("SHOW NAMESPACES IN %s", catalogName);

    if (isHadoopCatalog) {
      Assert.assertEquals("Should have 1 namespace", 1, namespaces.size());
      Set<String> namespaceNames = namespaces.stream().map(arr -> arr[0].toString()).collect(Collectors.toSet());
      Assert.assertEquals("Should have only db namespace", ImmutableSet.of("db"), namespaceNames);
    } else {
      Assert.assertEquals("Should have 2 namespaces", 2, namespaces.size());
      Set<String> namespaceNames = namespaces.stream().map(arr -> arr[0].toString()).collect(Collectors.toSet());
      Assert.assertEquals("Should have default and db namespaces", ImmutableSet.of("default", "db"), namespaceNames);
    }

    List<Object[]> nestedNamespaces = sql("SHOW NAMESPACES IN %s", fullNamespace);

    Set<String> nestedNames = nestedNamespaces.stream().map(arr -> arr[0].toString()).collect(Collectors.toSet());
    Assert.assertEquals("Should not have nested namespaces", ImmutableSet.of(), nestedNames);
  }

  @Test
  public void testCreateNamespaceWithMetadata() {
    Assume.assumeFalse("HadoopCatalog does not support namespace metadata", isHadoopCatalog);

    Assert.assertFalse("Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

    sql("CREATE NAMESPACE %s WITH PROPERTIES ('prop'='value')", fullNamespace);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(NS));

    Map<String, String> nsMetadata = validationNamespaceCatalog.loadNamespaceMetadata(NS);

    Assert.assertEquals("Namespace should have expected prop value", "value", nsMetadata.get("prop"));
  }

  @Test
  public void testCreateNamespaceWithComment() {
    Assume.assumeFalse("HadoopCatalog does not support namespace metadata", isHadoopCatalog);

    Assert.assertFalse("Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

    sql("CREATE NAMESPACE %s COMMENT 'namespace doc'", fullNamespace);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(NS));

    Map<String, String> nsMetadata = validationNamespaceCatalog.loadNamespaceMetadata(NS);

    Assert.assertEquals("Namespace should have expected comment", "namespace doc", nsMetadata.get("comment"));
  }

  @Test
  public void testCreateNamespaceWithLocation() throws Exception {
    Assume.assumeFalse("HadoopCatalog does not support namespace locations", isHadoopCatalog);

    Assert.assertFalse("Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

    File location = temp.newFile();
    Assert.assertTrue(location.delete());

    sql("CREATE NAMESPACE %s LOCATION '%s'", fullNamespace, location);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(NS));

    Map<String, String> nsMetadata = validationNamespaceCatalog.loadNamespaceMetadata(NS);

    Assert.assertEquals("Namespace should have expected location",
        "file:" + location.getPath(), nsMetadata.get("location"));
  }

  @Test
  public void testSetProperties() {
    Assume.assumeFalse("HadoopCatalog does not support namespace metadata", isHadoopCatalog);

    Assert.assertFalse("Namespace should not already exist", validationNamespaceCatalog.namespaceExists(NS));

    sql("CREATE NAMESPACE %s", fullNamespace);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(NS));

    Map<String, String> defaultMetadata = validationNamespaceCatalog.loadNamespaceMetadata(NS);
    Assert.assertFalse("Default metadata should not have custom property", defaultMetadata.containsKey("prop"));

    sql("ALTER NAMESPACE %s SET PROPERTIES ('prop'='value')", fullNamespace);

    Map<String, String> nsMetadata = validationNamespaceCatalog.loadNamespaceMetadata(NS);

    Assert.assertEquals("Namespace should have expected prop value", "value", nsMetadata.get("prop"));
  }

  // Returns the full string representation of a namespace, with catalog appended to the front.
  private String fullNamespaceFor(Namespace ns) {
    return ("spark_catalog".equals(catalogName) ? "" : catalogName + ".") + ns;
  }
}
