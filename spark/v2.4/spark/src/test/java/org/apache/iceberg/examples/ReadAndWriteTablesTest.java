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

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** This test class uses Spark to create partitioned and unpartitioned tables locally. */
public class ReadAndWriteTablesTest {

  private SparkSession spark;
  private Table table;
  private HadoopTables tables;
  private File pathToTable;
  private Schema schema;

  @Before
  public void before() throws IOException {
    spark = SparkSession.builder().master("local[2]").getOrCreate();

    pathToTable = Files.createTempDirectory("temp").toFile();
    tables = new HadoopTables(spark.sessionState().newHadoopConf());

    schema =
        new Schema(
            optional(1, "id", Types.IntegerType.get()),
            optional(2, "data", Types.StringType.get()));
  }

  @Test
  public void createUnpartitionedTable() {
    table = tables.create(schema, pathToTable.toString());

    List<SimpleRecord> expected =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    df.select("id", "data").write().format("iceberg").mode("append").save(pathToTable.toString());

    table.refresh();
  }

  @Test
  public void createPartitionedTable() {
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").build();

    table = tables.create(schema, spec, pathToTable.toString());

    List<SimpleRecord> expected =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    df.select("id", "data").write().format("iceberg").mode("append").save(pathToTable.toString());

    table.refresh();
  }

  @Test
  public void writeDataFromJsonFile() {
    Schema bookSchema =
        new Schema(
            optional(1, "title", Types.StringType.get()),
            optional(2, "price", Types.LongType.get()),
            optional(3, "author", Types.StringType.get()),
            optional(4, "published", Types.TimestampType.withZone()),
            optional(5, "genre", Types.StringType.get()));

    table = tables.create(bookSchema, pathToTable.toString());

    Dataset<Row> df = spark.read().json("src/test/resources/data/books.json");

    df.select(
            df.col("title"),
            df.col("price"),
            df.col("author"),
            df.col("published").cast(DataTypes.TimestampType),
            df.col("genre"))
        .write()
        .format("iceberg")
        .mode("append")
        .save(pathToTable.toString());

    table.refresh();
  }

  @Test
  public void readFromIcebergTableWithSpark() {
    table = tables.create(schema, pathToTable.toString());

    Dataset<Row> results = spark.read().format("iceberg").load(pathToTable.toString());

    results.createOrReplaceTempView("table");
    spark.sql("select * from table").show();
  }

  @Test
  public void readFromPartitionedTableWithFilter() {
    table = tables.create(schema, pathToTable.toString());

    Dataset<Row> results =
        spark.read().format("iceberg").load(pathToTable.toString()).filter("data != \"b\"");

    results.createOrReplaceTempView("table");
    spark.sql("SELECT * FROM table").show();
  }

  @After
  public void after() throws IOException {
    FileUtils.deleteDirectory(pathToTable);
    spark.stop();
  }
}
