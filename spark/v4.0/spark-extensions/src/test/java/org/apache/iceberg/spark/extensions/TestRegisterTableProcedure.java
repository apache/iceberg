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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.atIndex;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.List;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRegisterTableProcedure extends ExtensionsTestBase {

  private String targetName;

  @BeforeEach
  public void setTargetName() {
    targetName = tableName("register_table");
  }

  @AfterEach
  public void dropTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s", targetName);
  }

  @TestTemplate
  public void testRegisterTable() throws NoSuchTableException, ParseException {
    long numRows = 1000;

    sql("CREATE TABLE %s (id int, data string) using ICEBERG", tableName);
    spark
        .range(0, numRows)
        .withColumn("data", functions.col("id").cast(DataTypes.StringType))
        .writeTo(tableName)
        .append();

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    long originalFileCount = (long) scalarSql("SELECT COUNT(*) from %s.files", tableName);
    long currentSnapshotId = table.currentSnapshot().snapshotId();
    String metadataJson = TableUtil.metadataFileLocation(table);

    List<Object[]> result =
        sql("CALL %s.system.register_table('%s', '%s')", catalogName, targetName, metadataJson);
    assertThat(result.get(0))
        .as("Current Snapshot is not correct")
        .contains(currentSnapshotId, atIndex(0));

    List<Object[]> original = sql("SELECT * FROM %s", tableName);
    List<Object[]> registered = sql("SELECT * FROM %s", targetName);
    assertEquals("Registered table rows should match original table rows", original, registered);
    assertThat(result.get(0))
        .as("Should have the right row count in the procedure result")
        .contains(numRows, atIndex(1))
        .as("Should have the right datafile count in the procedure result")
        .contains(originalFileCount, atIndex(2));
  }

  @TestTemplate
  void registerExistingTableWithoutOverwrite() throws NoSuchTableException, ParseException {
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);
    sql("CREATE TABLE %s (id int, data string) USING iceberg", targetName);

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    String metadataJson = TableUtil.metadataFileLocation(table);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.register_table(table => '%s', metadata_file => '%s', overwrite => false)",
                    catalogName, targetName, metadataJson))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageStartingWith("Table already exists");
  }

  @TestTemplate
  void registerTableWithOverwriteUnsupported() throws NoSuchTableException, ParseException {
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    String metadataJson = TableUtil.metadataFileLocation(table);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.register_table(table => '%s', metadata_file => '%s', overwrite => true)",
                    catalogName, targetName, metadataJson))
        .hasMessageContaining("Registering tables with overwrite is not supported");
  }

  @TestTemplate
  void registerTableWithOverwrite() throws NoSuchTableException, ParseException {
    assumeThat(catalogName).isEqualTo(SparkCatalogConfig.HADOOP.catalogName());

    String overwriteCatalog = "overwrite_cat";
    spark.conf().set("spark.sql.catalog." + overwriteCatalog, SparkCatalog.class.getName());
    spark
        .conf()
        .set(
            "spark.sql.catalog." + overwriteCatalog + "." + CatalogProperties.CATALOG_IMPL,
            OverwriteCatalog.class.getName());
    spark
        .conf()
        .set(
            "spark.sql.catalog." + overwriteCatalog + "." + CatalogProperties.FILE_IO_IMPL,
            HadoopFileIO.class.getName());
    spark
        .conf()
        .set(
            "spark.sql.catalog." + overwriteCatalog + "." + CatalogProperties.WAREHOUSE_LOCATION,
            temp.toFile().toURI().toString());

    String sourceTable = overwriteCatalog + ".default.source_table";
    String targetTable = overwriteCatalog + ".default.target_table";

    try {
      sql("CREATE NAMESPACE %s.default", overwriteCatalog);
      sql("CREATE TABLE %s (id int, data string) USING iceberg", sourceTable);
      sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", sourceTable);

      sql("CREATE TABLE %s (id int, data string) USING iceberg", targetTable);
      sql("INSERT INTO %s VALUES (3, 'c')", targetTable);
      assertThat(sql("SELECT * FROM %s", targetTable)).containsExactly(row(3, "c"));

      Table source = Spark3Util.loadIcebergTable(spark, sourceTable);
      long expectedSnapshotId = source.currentSnapshot().snapshotId();
      long expectedFileCount = (long) scalarSql("SELECT COUNT(*) FROM %s.files", sourceTable);
      List<Object[]> expectedRows = sql("SELECT * FROM %s ORDER BY id", sourceTable);
      String metadataJson = TableUtil.metadataFileLocation(source);

      List<Object[]> result =
          sql(
              "CALL %s.system.register_table(table => '%s', metadata_file => '%s', overwrite => true)",
              overwriteCatalog, targetTable, metadataJson);

      assertThat(result.get(0))
          .contains(expectedSnapshotId, atIndex(0))
          .contains((long) expectedRows.size(), atIndex(1))
          .contains(expectedFileCount, atIndex(2));
      assertThat(sql("SELECT * FROM %s ORDER BY id", targetTable))
          .containsExactlyElementsOf(expectedRows);
    } finally {
      sql("DROP TABLE IF EXISTS %s", sourceTable);
      sql("DROP TABLE IF EXISTS %s", targetTable);
      sql("DROP NAMESPACE IF EXISTS %s.default", overwriteCatalog);
    }
  }

  public static class OverwriteCatalog extends InMemoryCatalog {
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
