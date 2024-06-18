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
import java.nio.file.Files;
import java.util.UUID;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

public class TestCreateTable extends CatalogTestBase {

  @AfterEach
  public void dropTestTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testTransformIgnoreCase() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();
    sql(
        "CREATE TABLE IF NOT EXISTS %s (id BIGINT NOT NULL, ts timestamp) "
            + "USING iceberg partitioned by (HOURS(ts))",
        tableName);
    assertThat(validationCatalog.tableExists(tableIdent)).as("Table should already exist").isTrue();
    sql(
        "CREATE TABLE IF NOT EXISTS %s (id BIGINT NOT NULL, ts timestamp) "
            + "USING iceberg partitioned by (hours(ts))",
        tableName);
    assertThat(validationCatalog.tableExists(tableIdent)).as("Table should already exist").isTrue();
  }

  @TestTemplate
  public void testTransformSingularForm() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();
    sql(
        "CREATE TABLE IF NOT EXISTS %s (id BIGINT NOT NULL, ts timestamp) "
            + "USING iceberg partitioned by (hour(ts))",
        tableName);
    assertThat(validationCatalog.tableExists(tableIdent)).as("Table should exist").isTrue();
  }

  @TestTemplate
  public void testTransformPluralForm() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();
    sql(
        "CREATE TABLE IF NOT EXISTS %s (id BIGINT NOT NULL, ts timestamp) "
            + "USING iceberg partitioned by (hours(ts))",
        tableName);
    assertThat(validationCatalog.tableExists(tableIdent)).as("Table should exist").isTrue();
  }

  @TestTemplate
  public void testCreateTable() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    sql("CREATE TABLE %s (id BIGINT NOT NULL, data STRING) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table).as("Should load the new table").isNotNull();

    StructType expectedSchema =
        StructType.of(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(2, "data", Types.StringType.get()));
    assertThat(table.schema().asStruct())
        .as("Should have the expected schema")
        .isEqualTo(expectedSchema);
    assertThat(table.spec().fields()).as("Should not be partitioned").hasSize(0);
    assertThat(table.properties().get(TableProperties.DEFAULT_FILE_FORMAT))
        .as("Should not have the default format set")
        .isNull();
  }

  @TestTemplate
  public void testCreateTablePartitionedByUUID() {
    assertThat(validationCatalog.tableExists(tableIdent)).isFalse();
    Schema schema = new Schema(1, Types.NestedField.optional(1, "uuid", Types.UUIDType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("uuid", 16).build();
    validationCatalog.createTable(tableIdent, schema, spec);

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table).isNotNull();

    StructType expectedSchema =
        StructType.of(Types.NestedField.optional(1, "uuid", Types.UUIDType.get()));
    assertThat(table.schema().asStruct()).isEqualTo(expectedSchema);
    assertThat(table.spec().fields()).hasSize(1);

    String uuid = UUID.randomUUID().toString();

    sql("INSERT INTO %s VALUES('%s')", tableName, uuid);

    assertThat(sql("SELECT uuid FROM %s", tableName)).hasSize(1).element(0).isEqualTo(row(uuid));
  }

  @TestTemplate
  public void testCreateTableInRootNamespace() {
    assumeThat(catalogName)
        .as("Hadoop has no default namespace configured")
        .isEqualTo("testhadoop");

    try {
      sql("CREATE TABLE %s.table (id bigint) USING iceberg", catalogName);
    } finally {
      sql("DROP TABLE IF EXISTS %s.table", catalogName);
    }
  }

  @TestTemplate
  public void testCreateTableUsingParquet() {
    assumeThat(catalogName)
        .as("Not working with session catalog because Spark will not use v2 for a Parquet table")
        .isNotEqualTo("spark_catalog");

    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    sql("CREATE TABLE %s (id BIGINT NOT NULL, data STRING) USING parquet", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table).as("Should load the new table").isNotNull();

    StructType expectedSchema =
        StructType.of(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(2, "data", Types.StringType.get()));
    assertThat(table.schema().asStruct())
        .as("Should have the expected schema")
        .isEqualTo(expectedSchema);
    assertThat(table.spec().fields()).as("Should not be partitioned").hasSize(0);
    assertThat(table.properties().get(TableProperties.DEFAULT_FILE_FORMAT))
        .as("Should not have default format parquet")
        .isEqualTo("parquet");

    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s.default.fail (id BIGINT NOT NULL, data STRING) USING crocodile",
                    catalogName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unsupported format in USING: crocodile");
  }

  @TestTemplate
  public void testCreateTablePartitionedBy() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    sql(
        "CREATE TABLE %s "
            + "(id BIGINT NOT NULL, created_at TIMESTAMP, category STRING, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (category, bucket(8, id), days(created_at))",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table).as("Should load the new table").isNotNull();

    StructType expectedSchema =
        StructType.of(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(2, "created_at", Types.TimestampType.withZone()),
            NestedField.optional(3, "category", Types.StringType.get()),
            NestedField.optional(4, "data", Types.StringType.get()));
    assertThat(table.schema().asStruct())
        .as("Should have the expected schema")
        .isEqualTo(expectedSchema);

    PartitionSpec expectedSpec =
        PartitionSpec.builderFor(new Schema(expectedSchema.fields()))
            .identity("category")
            .bucket("id", 8)
            .day("created_at")
            .build();
    assertThat(table.spec()).as("Should be partitioned correctly").isEqualTo(expectedSpec);

    assertThat(table.properties().get(TableProperties.DEFAULT_FILE_FORMAT))
        .as("Should not have the default format set")
        .isNull();
  }

  @TestTemplate
  public void testCreateTableColumnComments() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    sql(
        "CREATE TABLE %s "
            + "(id BIGINT NOT NULL COMMENT 'Unique identifier', data STRING COMMENT 'Data value') "
            + "USING iceberg",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table).as("Should load the new table").isNotNull();

    StructType expectedSchema =
        StructType.of(
            NestedField.required(1, "id", Types.LongType.get(), "Unique identifier"),
            NestedField.optional(2, "data", Types.StringType.get(), "Data value"));
    assertThat(table.schema().asStruct())
        .as("Should have the expected schema")
        .isEqualTo(expectedSchema);
    assertThat(table.spec().fields()).as("Should not be partitioned").hasSize(0);
    assertThat(table.properties().get(TableProperties.DEFAULT_FILE_FORMAT))
        .as("Should not have the default format set")
        .isNull();
  }

  @TestTemplate
  public void testCreateTableComment() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    sql(
        "CREATE TABLE %s "
            + "(id BIGINT NOT NULL, data STRING) "
            + "USING iceberg "
            + "COMMENT 'Table doc'",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table).as("Should load the new table").isNotNull();

    StructType expectedSchema =
        StructType.of(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(2, "data", Types.StringType.get()));
    assertThat(table.schema().asStruct())
        .as("Should have the expected schema")
        .isEqualTo(expectedSchema);
    assertThat(table.spec().fields()).as("Should not be partitioned").hasSize(0);
    assertThat(table.properties().get(TableProperties.DEFAULT_FILE_FORMAT))
        .as("Should not have the default format set")
        .isNull();
    assertThat(table.properties().get(TableCatalog.PROP_COMMENT))
        .as("Should have the table comment set in properties")
        .isEqualTo("Table doc");
  }

  @TestTemplate
  public void testCreateTableLocation() throws Exception {
    assumeThat(validationCatalog)
        .as("Cannot set custom locations for Hadoop catalog tables")
        .isNotInstanceOf(HadoopCatalog.class);

    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    File tableLocation = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableLocation.delete()).isTrue();

    String location = "file:" + tableLocation.toString();

    sql(
        "CREATE TABLE %s "
            + "(id BIGINT NOT NULL, data STRING) "
            + "USING iceberg "
            + "LOCATION '%s'",
        tableName, location);

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table).as("Should load the new table").isNotNull();

    StructType expectedSchema =
        StructType.of(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(2, "data", Types.StringType.get()));
    assertThat(table.schema().asStruct())
        .as("Should have the expected schema")
        .isEqualTo(expectedSchema);
    assertThat(table.spec().fields()).as("Should not be partitioned").hasSize(0);
    assertThat(table.properties().get(TableProperties.DEFAULT_FILE_FORMAT))
        .as("Should not have the default format set")
        .isNull();
    assertThat(table.location()).as("Should have a custom table location").isEqualTo(location);
  }

  @TestTemplate
  public void testCreateTableProperties() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    sql(
        "CREATE TABLE %s "
            + "(id BIGINT NOT NULL, data STRING) "
            + "USING iceberg "
            + "TBLPROPERTIES (p1=2, p2='x')",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table).as("Should load the new table").isNotNull();

    StructType expectedSchema =
        StructType.of(
            NestedField.required(1, "id", Types.LongType.get()),
            NestedField.optional(2, "data", Types.StringType.get()));
    assertThat(table.schema().asStruct())
        .as("Should have the expected schema")
        .isEqualTo(expectedSchema);
    assertThat(table.spec().fields()).as("Should not be partitioned").hasSize(0);
    assertThat(table.properties()).containsEntry("p1", "2").containsEntry("p2", "x");
  }

  @TestTemplate
  public void testCreateTableWithFormatV2ThroughTableProperty() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    sql(
        "CREATE TABLE %s "
            + "(id BIGINT NOT NULL, data STRING) "
            + "USING iceberg "
            + "TBLPROPERTIES ('format-version'='2')",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(((BaseTable) table).operations().current().formatVersion())
        .as("should create table using format v2")
        .isEqualTo(2);
  }

  @TestTemplate
  public void testUpgradeTableWithFormatV2ThroughTableProperty() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    sql(
        "CREATE TABLE %s "
            + "(id BIGINT NOT NULL, data STRING) "
            + "USING iceberg "
            + "TBLPROPERTIES ('format-version'='1')",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    TableOperations ops = ((BaseTable) table).operations();
    assertThat(ops.refresh().formatVersion())
        .as("should create table using format v1")
        .isEqualTo(1);

    sql("ALTER TABLE %s SET TBLPROPERTIES ('format-version'='2')", tableName);
    assertThat(ops.refresh().formatVersion())
        .as("should update table to use format v2")
        .isEqualTo(2);
  }

  @TestTemplate
  public void testDowngradeTableToFormatV1ThroughTablePropertyFails() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    sql(
        "CREATE TABLE %s "
            + "(id BIGINT NOT NULL, data STRING) "
            + "USING iceberg "
            + "TBLPROPERTIES ('format-version'='2')",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    TableOperations ops = ((BaseTable) table).operations();
    assertThat(ops.refresh().formatVersion())
        .as("should create table using format v2")
        .isEqualTo(2);

    assertThatThrownBy(
            () -> sql("ALTER TABLE %s SET TBLPROPERTIES ('format-version'='1')", tableName))
        .cause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot downgrade v2 table to v1");
  }
}
