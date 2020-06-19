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

package org.apache.iceberg.examples;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.types.Types.NestedField.optional;

/**
 * This class tests how you can evolve your table schema with Iceberg.
 * This includes things like adding, deleting, renaming columns and type promotions.
 */
public class SchemaEvolutionTest {

  private static final Logger log = LoggerFactory.getLogger(SchemaEvolutionTest.class);

  private SparkSession spark;
  private Table table;
  private File tableLocation;
  private String dataLocation = "src/test/resources/data/";

  @Before
  public void before() throws IOException {
    spark = SparkSession.builder().master("local[2]").getOrCreate();
    tableLocation = Files.createTempDirectory("temp").toFile();
    Schema schema = new Schema(
        optional(1, "title", Types.StringType.get()),
        optional(2, "price", Types.IntegerType.get()),
        optional(3, "author", Types.StringType.get()),
        optional(4, "published", Types.TimestampType.withZone()),
        optional(5, "genre", Types.StringType.get())
    );
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .year("published")
        .build();

    HadoopTables tables = new HadoopTables(spark.sparkContext().hadoopConfiguration());
    table = tables.create(schema, spec, tableLocation.toString());

    Dataset<Row> df = spark.read().json(dataLocation + "/books.json");

    df.select(df.col("title"), df.col("price").cast(DataTypes.IntegerType),
        df.col("author"), df.col("published").cast(DataTypes.TimestampType),
        df.col("genre")).write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation.toString());

    table.refresh();
  }

  @Test
  public void addColumnToSchema() {
    table.updateSchema().addColumn("publisher", Types.StringType.get()).commit();

    Dataset<Row> df2 = spark.read().json(dataLocation + "new-books.json");

    df2.select(df2.col("title"), df2.col("price").cast(DataTypes.IntegerType),
        df2.col("author"), df2.col("published").cast(DataTypes.TimestampType),
        df2.col("genre"), df2.col("publisher")).write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation.toString());
  }

  @Test
  public void deleteColumnFromSchema() {
    table.updateSchema().deleteColumn("genre").commit();

    table.refresh();
    Dataset<Row> results = spark.read()
        .format("iceberg")
        .load(tableLocation.toString());

    results.createOrReplaceTempView("table");
    spark.sql("select * from table").show();
  }

  @Test
  public void renameColumn() {
    table.updateSchema().renameColumn("author", "writer").commit();

    table.refresh();
    Dataset<Row> results = spark.read()
        .format("iceberg")
        .load(tableLocation.toString());

    results.createOrReplaceTempView("table");
    spark.sql("select * from table").show();
  }

  @Test
  public void updateColumnTypeIntToLong() {
    table.updateSchema().updateColumn("price", Types.LongType.get()).commit();

    log.info("Promote int type to long type:\n" + table.schema().toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void updateColumnTypeIntToString() {
    table.updateSchema().updateColumn("price", Types.StringType.get()).commit();
  }

  @Test(expected = IllegalArgumentException.class)
  public void updateColumnTypeStringToInt() {
    table.updateSchema().updateColumn("author", Types.IntegerType.get()).commit();
  }

  @Test
  public void floatToDouble() throws IOException {
    // Set up a new table to test this conversion
    Schema schema = new Schema(optional(1, "float", Types.FloatType.get()));
    File location = Files.createTempDirectory("temp").toFile();
    HadoopTables tables = new HadoopTables(spark.sparkContext().hadoopConfiguration());
    Table floatTable = tables.create(schema, location.toString());

    floatTable.updateSchema().updateColumn("float", Types.DoubleType.get()).commit();

    log.info("Promote float type to double type:\n" + floatTable.schema().toString());
  }

  @Test
  public void widenDecimalPrecision() throws IOException {
    // Set up a new table to test this conversion
    Schema schema = new Schema(optional(1, "decimal", Types.DecimalType.of(2, 2)));
    File location = Files.createTempDirectory("temp").toFile();
    HadoopTables tables = new HadoopTables(spark.sparkContext().hadoopConfiguration());
    Table decimalTable = tables.create(schema, location.toString());

    decimalTable.updateSchema().updateColumn("decimal", Types.DecimalType.of(4, 2)).commit();

    log.info("Widen decimal type:\n" + decimalTable.schema().toString());
  }

  @Test
  public void after() throws IOException {
    spark.stop();
    FileUtils.deleteDirectory(tableLocation);
  }
}
