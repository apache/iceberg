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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Parameter;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestCreateTableAsSelect extends CatalogTestBase {

  @Parameter(index = 3)
  private String sourceName;

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}, sourceName = {3}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties(),
        SparkCatalogConfig.HIVE.catalogName() + ".default.source"
      },
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties(),
        SparkCatalogConfig.HADOOP.catalogName() + ".default.source"
      },
      {
        SparkCatalogConfig.SPARK.catalogName(),
        SparkCatalogConfig.SPARK.implementation(),
        SparkCatalogConfig.SPARK.properties(),
        "default.source"
      }
    };
  }

  @BeforeEach
  public void createTableIfNotExists() {
    sql(
        "CREATE TABLE IF NOT EXISTS %s (id bigint NOT NULL, data string) "
            + "USING iceberg PARTITIONED BY (truncate(id, 3))",
        sourceName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')", sourceName);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testUnpartitionedCTAS() {
    sql("CREATE TABLE %s USING iceberg AS SELECT * FROM %s", tableName, sourceName);

    Schema expectedSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    Table ctasTable = validationCatalog.loadTable(tableIdent);

    assertThat(ctasTable.schema().asStruct())
        .as("Should have expected nullable schema")
        .isEqualTo(expectedSchema.asStruct());

    assertThat(ctasTable.spec().fields()).as("Should be an unpartitioned table").hasSize(0);
    assertEquals(
        "Should have rows matching the source table",
        sql("SELECT * FROM %s ORDER BY id", sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testPartitionedCTAS() {
    sql(
        "CREATE TABLE %s USING iceberg PARTITIONED BY (id) AS SELECT * FROM %s ORDER BY id",
        tableName, sourceName);

    Schema expectedSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    PartitionSpec expectedSpec = PartitionSpec.builderFor(expectedSchema).identity("id").build();

    Table ctasTable = validationCatalog.loadTable(tableIdent);

    assertThat(ctasTable.schema().asStruct())
        .as("Should have expected nullable schema")
        .isEqualTo(expectedSchema.asStruct());
    assertThat(ctasTable.spec()).as("Should be partitioned by id").isEqualTo(expectedSpec);
    assertEquals(
        "Should have rows matching the source table",
        sql("SELECT * FROM %s ORDER BY id", sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testCTASWriteDistributionModeRespected() {
    sql(
        "CREATE TABLE %s USING iceberg PARTITIONED BY (bucket(2, id)) AS SELECT * FROM %s",
        tableName, sourceName);

    Schema expectedSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    PartitionSpec expectedSpec = PartitionSpec.builderFor(expectedSchema).bucket("id", 2).build();

    Table ctasTable = validationCatalog.loadTable(tableIdent);

    assertThat(ctasTable.schema().asStruct())
        .as("Should have expected nullable schema")
        .isEqualTo(expectedSchema.asStruct());
    assertThat(ctasTable.spec()).as("Should be partitioned by id").isEqualTo(expectedSpec);
    assertEquals(
        "Should have rows matching the source table",
        sql("SELECT * FROM %s ORDER BY id", sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testRTAS() {
    sql(
        "CREATE TABLE %s USING iceberg TBLPROPERTIES ('prop1'='val1', 'prop2'='val2')"
            + "AS SELECT * FROM %s",
        tableName, sourceName);

    assertEquals(
        "Should have rows matching the source table",
        sql("SELECT * FROM %s ORDER BY id", sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql(
        "REPLACE TABLE %s USING iceberg PARTITIONED BY (part) TBLPROPERTIES ('prop1'='newval1', 'prop3'='val3') AS "
            + "SELECT id, data, CASE WHEN (id %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
            + "FROM %s ORDER BY 3, 1",
        tableName, sourceName);

    Schema expectedSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "part", Types.StringType.get()));

    PartitionSpec expectedSpec =
        PartitionSpec.builderFor(expectedSchema).identity("part").withSpecId(1).build();

    Table rtasTable = validationCatalog.loadTable(tableIdent);

    // the replacement table has a different schema and partition spec than the original
    assertThat(rtasTable.schema().asStruct())
        .as("Should have expected nullable schema")
        .isEqualTo(expectedSchema.asStruct());
    assertThat(rtasTable.spec()).as("Should be partitioned by part").isEqualTo(expectedSpec);

    assertEquals(
        "Should have rows matching the source table",
        sql(
            "SELECT id, data, CASE WHEN (id %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
                + "FROM %s ORDER BY id",
            sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    assertThat(rtasTable.snapshots()).as("Table should have expected snapshots").hasSize(2);
    assertThat(rtasTable.properties().get("prop1"))
        .as("Should have updated table property")
        .isEqualTo("newval1");
    assertThat(rtasTable.properties().get("prop2"))
        .as("Should have preserved table property")
        .isEqualTo("val2");
    assertThat(rtasTable.properties().get("prop3"))
        .as("Should have new table property")
        .isEqualTo("val3");
  }

  @TestTemplate
  public void testCreateRTAS() {
    sql(
        "CREATE OR REPLACE TABLE %s USING iceberg PARTITIONED BY (part) AS "
            + "SELECT id, data, CASE WHEN (id %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
            + "FROM %s ORDER BY 3, 1",
        tableName, sourceName);

    assertEquals(
        "Should have rows matching the source table",
        sql(
            "SELECT id, data, CASE WHEN (id %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
                + "FROM %s ORDER BY id",
            sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql(
        "CREATE OR REPLACE TABLE %s USING iceberg PARTITIONED BY (part) AS "
            + "SELECT 2 * id as id, data, CASE WHEN ((2 * id) %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
            + "FROM %s ORDER BY 3, 1",
        tableName, sourceName);

    Schema expectedSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "part", Types.StringType.get()));

    PartitionSpec expectedSpec =
        PartitionSpec.builderFor(expectedSchema)
            .identity("part")
            .withSpecId(0) // the spec is identical and should be reused
            .build();

    Table rtasTable = validationCatalog.loadTable(tableIdent);

    // the replacement table has a different schema and partition spec than the original
    assertThat(rtasTable.schema().asStruct())
        .as("Should have expected nullable schema")
        .isEqualTo(expectedSchema.asStruct());
    assertThat(rtasTable.spec()).as("Should be partitioned by part").isEqualTo(expectedSpec);

    assertEquals(
        "Should have rows matching the source table",
        sql(
            "SELECT 2 * id, data, CASE WHEN ((2 * id) %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
                + "FROM %s ORDER BY id",
            sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    assertThat(rtasTable.snapshots()).as("Table should have expected snapshots").hasSize(2);
  }

  @TestTemplate
  public void testDataFrameV2Create() throws Exception {
    spark.table(sourceName).writeTo(tableName).using("iceberg").create();

    Schema expectedSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    Table ctasTable = validationCatalog.loadTable(tableIdent);

    assertThat(ctasTable.schema().asStruct())
        .as("Should have expected nullable schema")
        .isEqualTo(expectedSchema.asStruct());
    assertThat(ctasTable.spec().fields()).as("Should be an unpartitioned table").hasSize(0);
    assertEquals(
        "Should have rows matching the source table",
        sql("SELECT * FROM %s ORDER BY id", sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testDataFrameV2Replace() throws Exception {
    spark.table(sourceName).writeTo(tableName).using("iceberg").create();

    assertEquals(
        "Should have rows matching the source table",
        sql("SELECT * FROM %s ORDER BY id", sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    spark
        .table(sourceName)
        .select(
            col("id"),
            col("data"),
            when(col("id").mod(lit(2)).equalTo(lit(0)), lit("even")).otherwise("odd").as("part"))
        .orderBy("part", "id")
        .writeTo(tableName)
        .partitionedBy(col("part"))
        .using("iceberg")
        .replace();

    Schema expectedSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "part", Types.StringType.get()));

    PartitionSpec expectedSpec =
        PartitionSpec.builderFor(expectedSchema).identity("part").withSpecId(1).build();

    Table rtasTable = validationCatalog.loadTable(tableIdent);

    // the replacement table has a different schema and partition spec than the original
    assertThat(rtasTable.schema().asStruct())
        .as("Should have expected nullable schema")
        .isEqualTo(expectedSchema.asStruct());
    assertThat(rtasTable.spec()).as("Should be partitioned by part").isEqualTo(expectedSpec);

    assertEquals(
        "Should have rows matching the source table",
        sql(
            "SELECT id, data, CASE WHEN (id %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
                + "FROM %s ORDER BY id",
            sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    assertThat(rtasTable.snapshots()).as("Table should have expected snapshots").hasSize(2);
  }

  @TestTemplate
  public void testDataFrameV2CreateOrReplace() {
    spark
        .table(sourceName)
        .select(
            col("id"),
            col("data"),
            when(col("id").mod(lit(2)).equalTo(lit(0)), lit("even")).otherwise("odd").as("part"))
        .orderBy("part", "id")
        .writeTo(tableName)
        .partitionedBy(col("part"))
        .using("iceberg")
        .createOrReplace();

    assertEquals(
        "Should have rows matching the source table",
        sql(
            "SELECT id, data, CASE WHEN (id %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
                + "FROM %s ORDER BY id",
            sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    spark
        .table(sourceName)
        .select(col("id").multiply(lit(2)).as("id"), col("data"))
        .select(
            col("id"),
            col("data"),
            when(col("id").mod(lit(2)).equalTo(lit(0)), lit("even")).otherwise("odd").as("part"))
        .orderBy("part", "id")
        .writeTo(tableName)
        .partitionedBy(col("part"))
        .using("iceberg")
        .createOrReplace();

    Schema expectedSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "part", Types.StringType.get()));

    PartitionSpec expectedSpec =
        PartitionSpec.builderFor(expectedSchema)
            .identity("part")
            .withSpecId(0) // the spec is identical and should be reused
            .build();

    Table rtasTable = validationCatalog.loadTable(tableIdent);

    // the replacement table has a different schema and partition spec than the original
    assertThat(rtasTable.schema().asStruct())
        .as("Should have expected nullable schema")
        .isEqualTo(expectedSchema.asStruct());
    assertThat(rtasTable.spec()).as("Should be partitioned by part").isEqualTo(expectedSpec);

    assertEquals(
        "Should have rows matching the source table",
        sql(
            "SELECT 2 * id, data, CASE WHEN ((2 * id) %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
                + "FROM %s ORDER BY id",
            sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    assertThat(rtasTable.snapshots()).as("Table should have expected snapshots").hasSize(2);
  }

  @TestTemplate
  public void testCreateRTASWithPartitionSpecChanging() {
    sql(
        "CREATE OR REPLACE TABLE %s USING iceberg PARTITIONED BY (part) AS "
            + "SELECT id, data, CASE WHEN (id %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
            + "FROM %s ORDER BY 3, 1",
        tableName, sourceName);

    Table rtasTable = validationCatalog.loadTable(tableIdent);

    assertEquals(
        "Should have rows matching the source table",
        sql(
            "SELECT id, data, CASE WHEN (id %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
                + "FROM %s ORDER BY id",
            sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    // Change the partitioning of the table
    rtasTable.updateSpec().removeField("part").commit(); // Spec 1

    sql(
        "CREATE OR REPLACE TABLE %s USING iceberg PARTITIONED BY (part, id) AS "
            + "SELECT 2 * id as id, data, CASE WHEN ((2 * id) %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
            + "FROM %s ORDER BY 3, 1",
        tableName, sourceName);

    Schema expectedSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "part", Types.StringType.get()));

    PartitionSpec expectedSpec =
        PartitionSpec.builderFor(expectedSchema)
            .alwaysNull("part", "part_10000")
            .identity("part")
            .identity("id")
            .withSpecId(2) // The Spec is new
            .build();

    assertThat(rtasTable.spec()).as("Should be partitioned by part and id").isEqualTo(expectedSpec);

    // the replacement table has a different schema and partition spec than the original
    assertThat(rtasTable.schema().asStruct())
        .as("Should have expected nullable schema")
        .isEqualTo(expectedSchema.asStruct());

    assertEquals(
        "Should have rows matching the source table",
        sql(
            "SELECT 2 * id, data, CASE WHEN ((2 * id) %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
                + "FROM %s ORDER BY id",
            sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    assertThat(rtasTable.snapshots()).as("Table should have expected snapshots").hasSize(2);
  }
}
