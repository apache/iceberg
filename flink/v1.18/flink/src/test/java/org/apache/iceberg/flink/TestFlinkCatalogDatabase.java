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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
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
    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Database should not already exist")
        .isFalse();
    sql("CREATE DATABASE %s", flinkDatabase);

    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Database should exist")
        .isTrue();

    sql("CREATE DATABASE IF NOT EXISTS %s", flinkDatabase);

    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Database should still exist")
        .isTrue();

    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);

    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Database should be dropped")
        .isFalse();

    sql("CREATE DATABASE IF NOT EXISTS %s", flinkDatabase);

    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Database should be created")
        .isTrue();
  }

  @TestTemplate
  public void testDropEmptyDatabase() {

    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should not already exist")
        .isFalse();

    sql("CREATE DATABASE %s", flinkDatabase);

    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should exist")
        .isTrue();

    sql("DROP DATABASE %s", flinkDatabase);

    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should have been dropped")
        .isFalse();
  }

  @TestTemplate
  public void testDropNonEmptyNamespace() {

    Assumptions.assumeFalse(
        isHadoopCatalog, "Hadoop catalog throws IOException: Directory is not empty.");

    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should not already exist")
        .isFalse();

    sql("CREATE DATABASE %s", flinkDatabase);

    validationCatalog.createTable(
        TableIdentifier.of(icebergNamespace, "tl"),
        new Schema(Types.NestedField.optional(0, "id", Types.LongType.get())));

    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should exist")
        .isTrue();

    Assertions.assertThat(validationCatalog.tableExists(TableIdentifier.of(icebergNamespace, "tl")))
        .as("Table should exist")
        .isTrue();

    Assertions.assertThatThrownBy(() -> sql("DROP DATABASE %s", flinkDatabase))
        .cause()
        .isInstanceOf(DatabaseNotEmptyException.class)
        .hasMessage(
            String.format("Database %s in catalog %s is not empty.", DATABASE, catalogName));

    sql("DROP TABLE %s.tl", flinkDatabase);
  }

  @TestTemplate
  public void testListTables() {

    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should not already exist")
        .isFalse();

    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);

    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should exist")
        .isTrue();

    Assertions.assertThat(sql("SHOW TABLES").size()).as("Should not list any tables").isEqualTo(0);

    validationCatalog.createTable(
        TableIdentifier.of(icebergNamespace, "tl"),
        new Schema(Types.NestedField.optional(0, "id", Types.LongType.get())));

    List<Row> tables = sql("SHOW TABLES");
    Assertions.assertThat(tables.size()).as("Only 1 table").isEqualTo(1);
    Assertions.assertThat("tl").as("Table name should match").isEqualTo(tables.get(0).getField(0));
  }

  @TestTemplate
  public void testListNamespace() {
    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should not already exist")
        .isFalse();

    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);

    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should exist")
        .isTrue();

    List<Row> databases = sql("SHOW DATABASES");

    if (isHadoopCatalog) {
      Assertions.assertThat(databases.size()).as("Should have 1 database").isEqualTo(1);
      Assertions.assertThat(databases.get(0).getField(0))
          .as("Should have db database")
          .isEqualTo("db");
      if (!baseNamespace.isEmpty()) {
        // test namespace not belongs to this catalog
        validationNamespaceCatalog.createNamespace(
            Namespace.of(baseNamespace.level(0), "UNKNOWN_NAMESPACE"));
        databases = sql("SHOW DATABASES");
        Assertions.assertThat(databases.size()).as("Should have 1 database").isEqualTo(1);
        Assertions.assertThat(databases.get(0).getField(0))
            .as("Should have db database")
            .isEqualTo("db");
      }
    } else {
      // If there are multiple classes extends FlinkTestBase, TestHiveMetastore may loose the
      // creation for default
      // database. See HiveMetaStore.HMSHandler.init.
      Assertions.assertThat(databases.stream().anyMatch(d -> Objects.equals(d.getField(0), "db")))
          .as("Should have db database")
          .isTrue();
    }
  }

  @TestTemplate
  public void testCreateNamespaceWithMetadata() {
    Assumptions.assumeFalse(isHadoopCatalog, "HadoopCatalog does not support namespace metadata");
    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should not already exist")
        .isFalse();

    sql("CREATE DATABASE %s WITH ('prop'='value')", flinkDatabase);

    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should exist")
        .isTrue();

    Map<String, String> nsMetadata =
        validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);
    Assertions.assertThat(nsMetadata.get("prop"))
        .as("Namespace should have expected prop value")
        .isEqualTo("value");
  }

  @TestTemplate
  public void testCreateNamespaceWithComment() {
    Assumptions.assumeFalse(isHadoopCatalog, "HadoopCatalog does not support namespace metadata");
    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should not already exist")
        .isFalse();

    sql("CREATE DATABASE %s COMMENT 'namespace doc'", flinkDatabase);
    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should exist")
        .isTrue();

    Map<String, String> nsMetadata =
        validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);

    Assertions.assertThat(nsMetadata.get("comment"))
        .as("Namespace should have expected comment")
        .isEqualTo("namespace doc");
  }

  @TestTemplate
  public void testCreateNamespaceWithLocation() throws Exception {
    Assumptions.assumeFalse(isHadoopCatalog, "HadoopCatalog does not support namespace metadata");

    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should not already exist")
        .isFalse();

    Path location = temporaryDirectory.getRoot();

    sql("CREATE DATABASE %s WITH ('location'='%s')", flinkDatabase, location);

    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should exist")
        .isTrue();

    Map<String, String> nsMetadata =
        validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);

    Assertions.assertThat(nsMetadata.get("location"))
        .as("Namespace should have expected location")
        .isEqualTo("file:" + location.getRoot());
  }

  @TestTemplate
  public void testSetProperties() {
    // Assume.assumeFalse("HadoopCatalog does not support namespace metadata", isHadoopCatalog);
    Assumptions.assumeFalse(isHadoopCatalog, "HadoopCatalog does not support namespace metadata");

    //      Assert.assertFalse(
    //          "Namespace should not already exist",
    //          validationNamespaceCatalog.namespaceExists(icebergNamespace));

    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should not already exist")
        .isFalse();

    sql("CREATE DATABASE %s", flinkDatabase);

    //      Assert.assertTrue(
    //          "Namespace should exist",
    // validationNamespaceCatalog.namespaceExists(icebergNamespace));

    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should exist")
        .isTrue();

    Map<String, String> defaultMetadata =
        validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);

    //      Assert.assertFalse(
    //          "Default metadata should not have custom property",
    //   defaultMetadata.containsKey("prop"));

    Assertions.assertThat(defaultMetadata.containsKey("prop"))
        .as("Default metadata should not have custom property")
        .isFalse();

    sql("ALTER DATABASE %s SET ('prop'='value')", flinkDatabase);

    Map<String, String> nsMetadata =
        validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);

    //      Assert.assertEquals(
    //          "Namespace should have expected prop value", "value", nsMetadata.get("prop"));

    Assertions.assertThat(nsMetadata.get("prop"))
        .as("Namespace should have expected prop value")
        .isEqualTo("value");
  }

  @TestTemplate
  public void testHadoopNotSupportMeta() {

    Assumptions.assumeTrue(isHadoopCatalog, "HadoopCatalog does not support namespace metadata");
    Assertions.assertThat(isHadoopCatalog)
        .as("HadoopCatalog does not support namespace metadata")
        .isTrue();

    Assertions.assertThat(validationNamespaceCatalog.namespaceExists(icebergNamespace))
        .as("Namespace should not already exist")
        .isFalse();

    Assertions.assertThatThrownBy(
            () -> sql("CREATE DATABASE %s WITH ('prop'='value')", flinkDatabase))
        .cause()
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(
            String.format(
                "Cannot create namespace %s: metadata is not supported", icebergNamespace));
  }
}
