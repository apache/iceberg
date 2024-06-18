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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

public class TestFlinkCatalogDatabase extends CatalogTestBase {

  @AfterEach
  @Override
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.tl", flinkDatabase);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
  }

  @TestTemplate
  public void testCreateNamespace() {
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Database should not already exist")
        .isFalse();

    sql("CREATE DATABASE %s", flinkDatabase);
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Database should exist")
        .isTrue();

    sql("CREATE DATABASE IF NOT EXISTS %s", flinkDatabase);
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Database should still exist")
        .isTrue();

    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Database should be dropped")
        .isFalse();

    sql("CREATE DATABASE IF NOT EXISTS %s", flinkDatabase);
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Database should be created")
        .isTrue();
  }

  @TestTemplate
  public void testDropEmptyDatabase() {
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should not already exist")
        .isFalse();
    sql("CREATE DATABASE %s", flinkDatabase);
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should exist")
        .isTrue();
    sql("DROP DATABASE %s", flinkDatabase);
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should have been dropped")
        .isFalse();
  }

  @TestTemplate
  public void testDropNonEmptyNamespace() {
    assumeThat(isHadoopCatalog)
        .as("Hadoop catalog throws IOException: Directory is not empty.")
        .isFalse();
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should not already exist")
        .isFalse();
    sql("CREATE DATABASE %s", flinkDatabase);
    validationCatalog.createTable(
        TableIdentifier.of(icebergNamespace, "tl"),
        new Schema(Types.NestedField.optional(0, "id", Types.LongType.get())));
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should exist")
        .isTrue();
    assertThat(validationCatalog.tableExists(TableIdentifier.of(icebergNamespace, "tl")))
        .as("Table should exist")
        .isTrue();
    assertThatThrownBy(() -> sql("DROP DATABASE %s", flinkDatabase))
        .cause()
        .isInstanceOf(DatabaseNotEmptyException.class)
        .hasMessage(
            String.format("Database %s in catalog %s is not empty.", DATABASE, catalogName));
    sql("DROP TABLE %s.tl", flinkDatabase);
  }

  @TestTemplate
  public void testListTables() {
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should not already exist")
        .isFalse();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should exist")
        .isTrue();
    assertThat(sql("SHOW TABLES")).isEmpty();
    validationCatalog.createTable(
        TableIdentifier.of(icebergNamespace, "tl"),
        new Schema(Types.NestedField.optional(0, "id", Types.LongType.get())));

    List<Row> tables = sql("SHOW TABLES");
    assertThat(tables).hasSize(1);
    assertThat("tl").as("Table name should match").isEqualTo(tables.get(0).getField(0));
  }

  @TestTemplate
  public void testListNamespace() {
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should not already exist")
        .isFalse();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should exist")
        .isTrue();

    List<Row> databases = sql("SHOW DATABASES");

    if (isHadoopCatalog) {
      assertThat(databases).hasSize(1);
      assertThat(databases.get(0).getField(0)).as("Should have db database").isEqualTo("db");
      if (!baseNamespace.isEmpty()) {
        // test namespace not belongs to this catalog
        validationNamespaceCatalog.createNamespace(
            Namespace.of(baseNamespace.level(0), "UNKNOWN_NAMESPACE"));
        databases = sql("SHOW DATABASES");
        assertThat(databases).hasSize(1);
        assertThat(databases.get(0).getField(0)).as("Should have db database").isEqualTo("db");
      }
    } else {
      // If there are multiple classes extends FlinkTestBase, TestHiveMetastore may loose the
      // creation for default
      // database. See HiveMetaStore.HMSHandler.init.
      assertThat(databases)
          .as("Should have db database")
          .anyMatch(d -> Objects.equals(d.getField(0), "db"));
    }
  }

  @TestTemplate
  public void testCreateNamespaceWithMetadata() {
    assumeThat(isHadoopCatalog).as("HadoopCatalog does not support namespace metadata").isFalse();
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should not already exist")
        .isFalse();
    sql("CREATE DATABASE %s WITH ('prop'='value')", flinkDatabase);
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should exist")
        .isTrue();
    Map<String, String> nsMetadata =
        validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);
    assertThat(nsMetadata).containsEntry("prop", "value");
  }

  @TestTemplate
  public void testCreateNamespaceWithComment() {
    assumeThat(isHadoopCatalog).as("HadoopCatalog does not support namespace metadata").isFalse();
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should not already exist")
        .isFalse();

    sql("CREATE DATABASE %s COMMENT 'namespace doc'", flinkDatabase);
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should exist")
        .isTrue();
    Map<String, String> nsMetadata =
        validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);
    assertThat(nsMetadata).containsEntry("comment", "namespace doc");
  }

  @TestTemplate
  public void testCreateNamespaceWithLocation() throws Exception {
    assumeThat(isHadoopCatalog).as("HadoopCatalog does not support namespace metadata").isFalse();
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should not already exist")
        .isFalse();

    Path location = temporaryDirectory.getRoot();
    sql("CREATE DATABASE %s WITH ('location'='%s')", flinkDatabase, location);
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should exist")
        .isTrue();
    Map<String, String> nsMetadata =
        validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);
    assertThat(nsMetadata).containsEntry("location", "file:" + location.getRoot());
  }

  @TestTemplate
  public void testSetProperties() {
    assumeThat(isHadoopCatalog).as("HadoopCatalog does not support namespace metadata").isFalse();
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should not already exist")
        .isFalse();

    sql("CREATE DATABASE %s", flinkDatabase);
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should exist")
        .isTrue();

    Map<String, String> defaultMetadata =
        validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);
    assertThat(defaultMetadata).doesNotContainKey("prop");
    sql("ALTER DATABASE %s SET ('prop'='value')", flinkDatabase);
    Map<String, String> nsMetadata =
        validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);
    assertThat(nsMetadata).containsEntry("prop", "value");
  }

  @TestTemplate
  public void testHadoopNotSupportMeta() {
    assumeThat(isHadoopCatalog).as("HadoopCatalog does not support namespace metadata").isTrue();
    assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should not already exist")
        .isFalse();
    assertThatThrownBy(() -> sql("CREATE DATABASE %s WITH ('prop'='value')", flinkDatabase))
        .cause()
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(
            String.format(
                "Cannot create namespace %s: metadata is not supported", icebergNamespace));
  }
}
