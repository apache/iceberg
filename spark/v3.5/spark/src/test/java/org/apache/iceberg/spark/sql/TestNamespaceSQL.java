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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

public class TestNamespaceSQL extends CatalogTestBase {
  private static final Namespace NS = Namespace.of("db");

  @Parameter(index = 3)
  private String fullNamespace;

  @Parameter(index = 4)
  private boolean isHadoopCatalog;

  @Parameters(
      name =
          "catalogName = {0}, implementation = {1}, config = {2}, fullNameSpace = {3}, isHadoopCatalog = {4}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties(),
        SparkCatalogConfig.HIVE.catalogName() + "." + NS.toString(),
        false
      },
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties(),
        SparkCatalogConfig.HADOOP.catalogName() + "." + NS,
        true
      },
      {
        SparkCatalogConfig.SPARK.catalogName(),
        SparkCatalogConfig.SPARK.implementation(),
        SparkCatalogConfig.SPARK.properties(),
        NS.toString(),
        false
      },
      {
        SparkCatalogConfig.SPARK_SESSION_WITH_VIEWS.catalogName(),
        SparkCatalogConfig.SPARK_SESSION_WITH_VIEWS.implementation(),
        SparkCatalogConfig.SPARK_SESSION_WITH_VIEWS.properties(),
        NS.toString(),
        false
      }
    };
  }

  @AfterEach
  public void cleanNamespaces() {
    sql("DROP TABLE IF EXISTS %s.table", fullNamespace);
    sql("DROP NAMESPACE IF EXISTS %s", fullNamespace);
  }

  @TestTemplate
  public void testCreateNamespace() {
    assertThat(validationNamespaceCatalog.namespaceExists(NS))
        .as("Namespace should not already exist")
        .isFalse();

    sql("CREATE NAMESPACE %s", fullNamespace);

    assertThat(validationNamespaceCatalog.namespaceExists(NS))
        .as("Namespace should exist")
        .isTrue();
  }

  @TestTemplate
  public void testDefaultNamespace() {
    assumeThat(isHadoopCatalog).as("Hadoop has no default namespace configured").isFalse();

    sql("USE %s", catalogName);

    Object[] current = Iterables.getOnlyElement(sql("SHOW CURRENT NAMESPACE"));
    assertThat(current[0]).as("Should use the current catalog").isEqualTo(catalogName);
    assertThat(current[1]).as("Should use the configured default namespace").isEqualTo("default");
  }

  @TestTemplate
  public void testDropEmptyNamespace() {
    assertThat(validationNamespaceCatalog.namespaceExists(NS))
        .as("Namespace should not already exist")
        .isFalse();

    sql("CREATE NAMESPACE %s", fullNamespace);

    assertThat(validationNamespaceCatalog.namespaceExists(NS))
        .as("Namespace should exist")
        .isTrue();

    sql("DROP NAMESPACE %s", fullNamespace);

    assertThat(validationNamespaceCatalog.namespaceExists(NS))
        .as("Namespace should have been dropped")
        .isFalse();
  }

  @TestTemplate
  public void testDropNonEmptyNamespace() {
    assertThat(validationNamespaceCatalog.namespaceExists(NS))
        .as("Namespace should not already exist")
        .isFalse();

    sql("CREATE NAMESPACE %s", fullNamespace);
    sql("CREATE TABLE %s.table (id bigint) USING iceberg", fullNamespace);

    assertThat(validationNamespaceCatalog.namespaceExists(NS))
        .as("Namespace should exist")
        .isTrue();
    assertThat(validationCatalog.tableExists(TableIdentifier.of(NS, "table")))
        .as("Table should exist")
        .isTrue();

    assertThatThrownBy(() -> sql("DROP NAMESPACE %s", fullNamespace))
        .isInstanceOfAny(NamespaceNotEmptyException.class, AnalysisException.class)
        .message()
        .satisfiesAnyOf(
            msg -> assertThat(msg).startsWith("Namespace db is not empty."),
            msg ->
                assertThat(msg)
                    .startsWith(
                        "[SCHEMA_NOT_EMPTY] Cannot drop a schema `db` because it contains objects."));

    sql("DROP TABLE %s.table", fullNamespace);
  }

  @TestTemplate
  public void testListTables() {
    assertThat(validationNamespaceCatalog.namespaceExists(NS))
        .as("Namespace should not already exist")
        .isFalse();

    sql("CREATE NAMESPACE %s", fullNamespace);

    assertThat(validationNamespaceCatalog.namespaceExists(NS))
        .as("Namespace should exist")
        .isTrue();

    List<Object[]> rows = sql("SHOW TABLES IN %s", fullNamespace);
    assertThat(rows).as("Should not list any tables").hasSize(0);

    sql("CREATE TABLE %s.table (id bigint) USING iceberg", fullNamespace);

    Object[] row = Iterables.getOnlyElement(sql("SHOW TABLES IN %s", fullNamespace));
    assertThat(row[0]).as("Namespace should match").isEqualTo("db");
    assertThat(row[1]).as("Table name should match").isEqualTo("table");
  }

  @TestTemplate
  public void testListNamespace() {
    assertThat(validationNamespaceCatalog.namespaceExists(NS))
        .as("Namespace should not already exist")
        .isFalse();

    sql("CREATE NAMESPACE %s", fullNamespace);

    assertThat(validationNamespaceCatalog.namespaceExists(NS))
        .as("Namespace should exist")
        .isTrue();

    List<Object[]> namespaces = sql("SHOW NAMESPACES IN %s", catalogName);

    if (isHadoopCatalog) {
      assertThat(namespaces).as("Should have 1 namespace").hasSize(1);
      Set<String> namespaceNames =
          namespaces.stream().map(arr -> arr[0].toString()).collect(Collectors.toSet());
      assertThat(namespaceNames)
          .as("Should have only db namespace")
          .isEqualTo(ImmutableSet.of("db"));
    } else {
      assertThat(namespaces).as("Should have 2 namespaces").hasSize(2);
      Set<String> namespaceNames =
          namespaces.stream().map(arr -> arr[0].toString()).collect(Collectors.toSet());
      assertThat(namespaceNames)
          .as("Should have default and db namespaces")
          .isEqualTo(ImmutableSet.of("default", "db"));
    }

    List<Object[]> nestedNamespaces = sql("SHOW NAMESPACES IN %s", fullNamespace);

    Set<String> nestedNames =
        nestedNamespaces.stream().map(arr -> arr[0].toString()).collect(Collectors.toSet());
    assertThat(nestedNames).as("Should not have nested namespaces").isEmpty();
  }

  @TestTemplate
  public void testCreateNamespaceWithMetadata() {
    assumeThat(isHadoopCatalog).as("HadoopCatalog does not support namespace metadata").isFalse();

    assertThat(validationNamespaceCatalog.namespaceExists(NS))
        .as("Namespace should not already exist")
        .isFalse();

    sql("CREATE NAMESPACE %s WITH PROPERTIES ('prop'='value')", fullNamespace);

    assertThat(validationNamespaceCatalog.namespaceExists(NS))
        .as("Namespace should exist")
        .isTrue();

    Map<String, String> nsMetadata = validationNamespaceCatalog.loadNamespaceMetadata(NS);

    assertThat(nsMetadata).containsEntry("prop", "value");
  }

  @TestTemplate
  public void testCreateNamespaceWithComment() {
    assumeThat(isHadoopCatalog).as("HadoopCatalog does not support namespace metadata").isFalse();

    assertThat(validationNamespaceCatalog.namespaceExists(NS))
        .as("Namespace should not already exist")
        .isFalse();

    sql("CREATE NAMESPACE %s COMMENT 'namespace doc'", fullNamespace);

    assertThat(validationNamespaceCatalog.namespaceExists(NS))
        .as("Namespace should exist")
        .isTrue();

    Map<String, String> nsMetadata = validationNamespaceCatalog.loadNamespaceMetadata(NS);

    assertThat(nsMetadata).containsEntry("comment", "namespace doc");
  }

  @TestTemplate
  public void testCreateNamespaceWithLocation() throws Exception {
    assumeThat(isHadoopCatalog).as("HadoopCatalog does not support namespace metadata").isFalse();

    assertThat(validationNamespaceCatalog.namespaceExists(NS))
        .as("Namespace should not already exist")
        .isFalse();

    File location = File.createTempFile("junit", null, temp.toFile());
    assertThat(location.delete()).isTrue();

    sql("CREATE NAMESPACE %s LOCATION '%s'", fullNamespace, location);

    assertThat(validationNamespaceCatalog.namespaceExists(NS))
        .as("Namespace should exist")
        .isTrue();

    Map<String, String> nsMetadata = validationNamespaceCatalog.loadNamespaceMetadata(NS);

    assertThat(nsMetadata).containsEntry("location", "file:" + location.getPath());
  }

  @TestTemplate
  public void testSetProperties() {
    assumeThat(isHadoopCatalog).as("HadoopCatalog does not support namespace metadata").isFalse();

    assertThat(validationNamespaceCatalog.namespaceExists(NS))
        .as("Namespace should not already exist")
        .isFalse();

    sql("CREATE NAMESPACE %s", fullNamespace);

    assertThat(validationNamespaceCatalog.namespaceExists(NS))
        .as("Namespace should exist")
        .isTrue();

    Map<String, String> defaultMetadata = validationNamespaceCatalog.loadNamespaceMetadata(NS);
    assertThat(defaultMetadata)
        .as("Default metadata should not have custom property")
        .doesNotContainKey("prop");

    sql("ALTER NAMESPACE %s SET PROPERTIES ('prop'='value')", fullNamespace);

    Map<String, String> nsMetadata = validationNamespaceCatalog.loadNamespaceMetadata(NS);

    assertThat(nsMetadata).containsEntry("prop", "value");
  }
}
