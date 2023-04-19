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

import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestCreateTableAsSelect extends SparkCatalogTestBase {

  private final String sourceName;

  public TestCreateTableAsSelect(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
    this.sourceName = tableName("source");

    sql(
        "CREATE TABLE IF NOT EXISTS %s (id bigint NOT NULL, data string) "
            + "USING iceberg PARTITIONED BY (truncate(id, 3))",
        sourceName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", sourceName);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testUnpartitionedCTAS() {
    sql("CREATE TABLE %s USING iceberg AS SELECT * FROM %s", tableName, sourceName);

    Schema expectedSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    Table ctasTable = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals(
        "Should have expected nullable schema",
        expectedSchema.asStruct(),
        ctasTable.schema().asStruct());
    Assert.assertEquals("Should be an unpartitioned table", 0, ctasTable.spec().fields().size());
    assertEquals(
        "Should have rows matching the source table",
        sql("SELECT * FROM %s ORDER BY id", sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
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

    Assert.assertEquals(
        "Should have expected nullable schema",
        expectedSchema.asStruct(),
        ctasTable.schema().asStruct());
    Assert.assertEquals("Should be partitioned by id", expectedSpec, ctasTable.spec());
    assertEquals(
        "Should have rows matching the source table",
        sql("SELECT * FROM %s ORDER BY id", sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
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
    Assert.assertEquals(
        "Should have expected nullable schema",
        expectedSchema.asStruct(),
        rtasTable.schema().asStruct());
    Assert.assertEquals("Should be partitioned by part", expectedSpec, rtasTable.spec());

    assertEquals(
        "Should have rows matching the source table",
        sql(
            "SELECT id, data, CASE WHEN (id %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
                + "FROM %s ORDER BY id",
            sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    Assert.assertEquals(
        "Table should have expected snapshots", 2, Iterables.size(rtasTable.snapshots()));

    Assert.assertEquals(
        "Should have updated table property", "newval1", rtasTable.properties().get("prop1"));
    Assert.assertEquals(
        "Should have preserved table property", "val2", rtasTable.properties().get("prop2"));
    Assert.assertEquals(
        "Should have new table property", "val3", rtasTable.properties().get("prop3"));
  }

  @Test
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
    Assert.assertEquals(
        "Should have expected nullable schema",
        expectedSchema.asStruct(),
        rtasTable.schema().asStruct());
    Assert.assertEquals("Should be partitioned by part", expectedSpec, rtasTable.spec());

    assertEquals(
        "Should have rows matching the source table",
        sql(
            "SELECT 2 * id, data, CASE WHEN ((2 * id) %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
                + "FROM %s ORDER BY id",
            sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    Assert.assertEquals(
        "Table should have expected snapshots", 2, Iterables.size(rtasTable.snapshots()));
  }

  @Test
  public void testDataFrameV2Create() throws Exception {
    spark.table(sourceName).writeTo(tableName).using("iceberg").create();

    Schema expectedSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    Table ctasTable = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals(
        "Should have expected nullable schema",
        expectedSchema.asStruct(),
        ctasTable.schema().asStruct());
    Assert.assertEquals("Should be an unpartitioned table", 0, ctasTable.spec().fields().size());
    assertEquals(
        "Should have rows matching the source table",
        sql("SELECT * FROM %s ORDER BY id", sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
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
    Assert.assertEquals(
        "Should have expected nullable schema",
        expectedSchema.asStruct(),
        rtasTable.schema().asStruct());
    Assert.assertEquals("Should be partitioned by part", expectedSpec, rtasTable.spec());

    assertEquals(
        "Should have rows matching the source table",
        sql(
            "SELECT id, data, CASE WHEN (id %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
                + "FROM %s ORDER BY id",
            sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    Assert.assertEquals(
        "Table should have expected snapshots", 2, Iterables.size(rtasTable.snapshots()));
  }

  @Test
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
    Assert.assertEquals(
        "Should have expected nullable schema",
        expectedSchema.asStruct(),
        rtasTable.schema().asStruct());
    Assert.assertEquals("Should be partitioned by part", expectedSpec, rtasTable.spec());

    assertEquals(
        "Should have rows matching the source table",
        sql(
            "SELECT 2 * id, data, CASE WHEN ((2 * id) %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
                + "FROM %s ORDER BY id",
            sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    Assert.assertEquals(
        "Table should have expected snapshots", 2, Iterables.size(rtasTable.snapshots()));
  }

  @Test
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
            .alwaysNull("part", "part_1000")
            .identity("part")
            .identity("id")
            .withSpecId(2) // The Spec is new
            .build();

    Assert.assertEquals("Should be partitioned by part and id", expectedSpec, rtasTable.spec());

    // the replacement table has a different schema and partition spec than the original
    Assert.assertEquals(
        "Should have expected nullable schema",
        expectedSchema.asStruct(),
        rtasTable.schema().asStruct());

    assertEquals(
        "Should have rows matching the source table",
        sql(
            "SELECT 2 * id, data, CASE WHEN ((2 * id) %% 2) = 0 THEN 'even' ELSE 'odd' END AS part "
                + "FROM %s ORDER BY id",
            sourceName),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    Assert.assertEquals(
        "Table should have expected snapshots", 2, Iterables.size(rtasTable.snapshots()));
  }
}
