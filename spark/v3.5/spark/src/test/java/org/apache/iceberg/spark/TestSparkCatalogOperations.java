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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestSparkCatalogOperations extends CatalogTestBase {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default",
            "use-nullable-query-schema", "false")
      },
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        ImmutableMap.of(
            "type", "hadoop", "cache-enabled", "false", "use-nullable-query-schema", "false")
      },
      {
        SparkCatalogConfig.SPARK.catalogName(),
        SparkCatalogConfig.SPARK.implementation(),
        ImmutableMap.of(
            "type",
            "hive",
            "default-namespace",
            "default",
            "parquet-enabled",
            "true",
            "cache-enabled",
            "false", // Spark will delete tables using v1, leaving the cache out of sync
            "use-nullable-query-schema",
            "false"),
      }
    };
  }

  @BeforeEach
  public void createTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
  }

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testAlterTable() throws NoSuchTableException {
    BaseCatalog catalog = (BaseCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    Identifier identifier = Identifier.of(tableIdent.namespace().levels(), tableIdent.name());

    String fieldName = "location";
    String propsKey = "note";
    String propsValue = "jazz";
    Table table =
        catalog.alterTable(
            identifier,
            TableChange.addColumn(new String[] {fieldName}, DataTypes.StringType, true),
            TableChange.setProperty(propsKey, propsValue));

    assertThat(table).as("Should return updated table").isNotNull();

    StructField expectedField = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
    assertThat(table.schema().fields()[2])
        .as("Adding a column to a table should return the updated table with the new column")
        .isEqualTo(expectedField);

    assertThat(table.properties())
        .as(
            "Adding a property to a table should return the updated table with the new property with the new correct value")
        .containsEntry(propsKey, propsValue);
  }

  @TestTemplate
  public void testInvalidateTable() {
    // load table to CachingCatalog
    sql("SELECT count(1) FROM %s", tableName);

    // recreate table from another catalog or program
    Catalog anotherCatalog = validationCatalog;
    Schema schema = anotherCatalog.loadTable(tableIdent).schema();
    anotherCatalog.dropTable(tableIdent);
    anotherCatalog.createTable(tableIdent, schema);

    // invalidate and reload table
    sql("REFRESH TABLE %s", tableName);
    sql("SELECT count(1) FROM %s", tableName);
  }

  @TestTemplate
  public void testCTASUseNullableQuerySchema() {
    sql("INSERT INTO %s VALUES(1, 'abc'), (2, null)", tableName);

    String ctasTableName = tableName("ctas_table");

    sql("CREATE TABLE %s USING iceberg AS SELECT * FROM %s", ctasTableName, tableName);

    org.apache.iceberg.Table ctasTable =
        validationCatalog.loadTable(TableIdentifier.parse("default.ctas_table"));

    Schema expectedSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    assertThat(ctasTable.schema().asStruct())
        .as("Should have expected schema")
        .isEqualTo(expectedSchema.asStruct());

    sql("DROP TABLE IF EXISTS %s", ctasTableName);
  }

  @TestTemplate
  public void testRTASUseNullableQuerySchema() {
    sql("INSERT INTO %s VALUES(1, 'abc'), (2, null)", tableName);

    String rtasTableName = tableName("rtas_table");
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", rtasTableName);

    sql("REPLACE TABLE %s USING iceberg AS SELECT * FROM %s", rtasTableName, tableName);

    org.apache.iceberg.Table rtasTable =
        validationCatalog.loadTable(TableIdentifier.parse("default.rtas_table"));

    Schema expectedSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    assertThat(rtasTable.schema().asStruct())
        .as("Should have expected schema")
        .isEqualTo(expectedSchema.asStruct());

    assertEquals(
        "Should have rows matching the source table",
        sql("SELECT * FROM %s ORDER BY id", tableName),
        sql("SELECT * FROM %s ORDER BY id", rtasTableName));

    sql("DROP TABLE IF EXISTS %s", rtasTableName);
  }
}
