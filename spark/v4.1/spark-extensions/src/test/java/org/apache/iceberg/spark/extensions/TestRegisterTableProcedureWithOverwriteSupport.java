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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.TestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Currently, {@code registerTable} (with overwrite) is not overridden by {@code HiveCatalog} and
 * {@code HadoopCatalog}, so they fall back to the default implementation in the {@code Catalog}
 * interface which does not support overwrite. {@code RESTCatalog} delegates the call to a backend,
 * but the backend used in this test environment also does not support it. Since none of the
 * standard catalogs available in {@code ExtensionsTestBase} support overwrite for registration,
 * this new test class was created with a custom catalog ({@link OverwriteSupportedCatalog}) to
 * verify that the {@code overwrite} parameter is correctly passed through the Spark procedure.
 */
public class TestRegisterTableProcedureWithOverwriteSupport extends TestBase {

  @TempDir protected java.nio.file.Path temp;

  private String localTargetName = "register_table";
  private String sourceTableName = "source_table";
  private String customCatalogName;

  @AfterEach
  public void dropTables() {
    if (customCatalogName != null) {
      sql("DROP TABLE IF EXISTS %s.default.%s", customCatalogName, sourceTableName);
      sql("DROP TABLE IF EXISTS %s.default.%s", customCatalogName, localTargetName);
    }
  }

  @Test
  public void testRegisterTableWithOverwrite() throws Exception {
    customCatalogName = "custom_catalog";
    spark
        .conf()
        .set("spark.sql.catalog." + customCatalogName, "org.apache.iceberg.spark.SparkCatalog");
    spark
        .conf()
        .set(
            "spark.sql.catalog." + customCatalogName + ".catalog-impl",
            OverwriteSupportedCatalog.class.getName());
    spark
        .conf()
        .set(
            "spark.sql.catalog." + customCatalogName + ".warehouse",
            temp.resolve("warehouse").toString());

    sql("USE %s", customCatalogName);

    // Create namespace in custom catalog
    sql("CREATE NAMESPACE IF NOT EXISTS %s.default", customCatalogName);

    // Create source table in custom catalog
    sql(
        "CREATE TABLE %s.default.%s (id int, data string) using ICEBERG",
        customCatalogName, sourceTableName);

    Table table =
        Spark3Util.loadIcebergTable(spark, customCatalogName + ".default." + sourceTableName);
    String metadataJson = TableUtil.metadataFileLocation(table);

    // Create target table in custom catalog so it exists
    sql(
        "CREATE TABLE %s.default.%s (id int, data string) using ICEBERG",
        customCatalogName, localTargetName);

    // Call register_table with overwrite = true
    sql(
        "CALL %s.system.register_table(table => 'default.%s', metadata_file => '%s', overwrite => true)",
        customCatalogName, localTargetName, metadataJson);

    // Verify that the table in custom catalog now has the content of the source table
    List<Object[]> original =
        sql("SELECT * FROM %s.default.%s", customCatalogName, sourceTableName);
    List<Object[]> registered =
        sql("SELECT * FROM %s.default.%s", customCatalogName, localTargetName);
    assertThat(registered)
        .as("Registered table rows should match original table rows")
        .isEqualTo(original);
  }

  public static class OverwriteSupportedCatalog extends InMemoryCatalog {
    @Override
    public Table registerTable(
        TableIdentifier identifier, String metadataFileLocation, boolean overwrite) {
      if (overwrite && tableExists(identifier)) {
        dropTable(identifier, false);
      }
      return registerTable(identifier, metadataFileLocation);
    }
  }
}
