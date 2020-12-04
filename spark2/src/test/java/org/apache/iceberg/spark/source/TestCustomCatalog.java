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

package org.apache.iceberg.spark.source;

import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestCustomCatalog {
  private static final String CATALOG_IMPL = CustomCatalogs.ICEBERG_CATALOG_PREFIX + CatalogProperties.CATALOG_IMPL;
  private static final String WAREHOUSE = CustomCatalogs.ICEBERG_CATALOG_PREFIX + CatalogProperties.WAREHOUSE_LOCATION;
  private static final String URI_KEY = CustomCatalogs.ICEBERG_CATALOG_PREFIX + CatalogProperties.HIVE_URI;
  private static final String URI_VAL = "thrift://localhost:12345"; // dummy uri
  private static final String CATALOG_VAL = "org.apache.iceberg.spark.source.TestCatalog";
  private static final TableIdentifier TABLE = TableIdentifier.of("default", "table");
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  File tableDir = null;
  String tableLocation = null;
  HadoopTables tables;

  protected static SparkSession spark = null;

  @BeforeClass
  public static void startMetastoreAndSpark() {
    spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopMetastoreAndSpark() {
    spark.stop();
    spark = null;
  }

  @Before
  public void setupTable() throws Exception {
    this.tables = new HadoopTables(spark.sessionState().newHadoopConf());
    this.tableDir = temp.newFolder();
    tableDir.delete(); // created by table create
    this.tableLocation = tableDir.toURI().toString();
    tables.create(SCHEMA, PartitionSpec.unpartitioned(), String.format("%s/%s", tableLocation, TABLE.name()));
  }

  @After
  public void removeTable() {
    SparkConf sparkConf = spark.sparkContext().conf();
    sparkConf.remove(CATALOG_IMPL);
    sparkConf.remove(WAREHOUSE);
    sparkConf.remove(URI_KEY);
    tables.dropTable(String.format("%s/%s", tableLocation, TABLE.name()));
    tableDir.delete();
    CustomCatalogs.clearCache();
  }

  @Test
  public void withOptions() {

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);
    AssertHelpers.assertThrows("We have not set all properties", IllegalArgumentException.class, () ->
        write(df, ImmutableMap.of(CatalogProperties.CATALOG_IMPL, CATALOG_VAL,
            CatalogProperties.HIVE_URI, URI_VAL))
    );

    write(df, ImmutableMap.of(CatalogProperties.CATALOG_IMPL, CATALOG_VAL,
        CatalogProperties.HIVE_URI, URI_VAL,
        CatalogProperties.WAREHOUSE_LOCATION, tableLocation));

    List<SimpleRecord> dfNew = read(
        ImmutableMap.of(CatalogProperties.CATALOG_IMPL, CATALOG_VAL,
            CatalogProperties.WAREHOUSE_LOCATION, tableLocation,
            CatalogProperties.HIVE_URI, URI_VAL
        ));

    Assert.assertEquals("Data should match", expected, dfNew);
  }

  @Test
  public void withSparkOptions() {

    SparkConf sparkConf = spark.sparkContext().conf();
    sparkConf.set(CATALOG_IMPL, CATALOG_VAL);
    sparkConf.set(URI_KEY, URI_VAL);

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);
    AssertHelpers.assertThrows("We have not set all properties", IllegalArgumentException.class, () ->
        write(df, ImmutableMap.of())
    );

    sparkConf.set(WAREHOUSE, tableLocation);

    write(df, ImmutableMap.of());

    List<SimpleRecord> dfNew = read(ImmutableMap.of());

    Assert.assertEquals("Data should match", expected, dfNew);
  }

  @Test
  public void withBothOptions() {

    SparkConf sparkConf = spark.sparkContext().conf();
    sparkConf.set(CATALOG_IMPL, CATALOG_VAL);
    sparkConf.set(URI_KEY, URI_VAL);

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);
    AssertHelpers.assertThrows("We have not set all properties", IllegalArgumentException.class, () ->
        write(df, ImmutableMap.of())
    );

    write(df, ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, tableLocation));

    List<SimpleRecord> dfNew = read(ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, tableLocation));

    Assert.assertEquals("Data should match", expected, dfNew);
  }

  @Test
  public void withOverwriteOptions() {

    SparkConf sparkConf = spark.sparkContext().conf();
    sparkConf.set(CATALOG_IMPL, CATALOG_VAL);
    sparkConf.set(URI_KEY, "invalid.hostname");
    sparkConf.set(WAREHOUSE, tableLocation);

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);
    AssertHelpers.assertThrows("We have not set all properties", IllegalArgumentException.class, () ->
        write(df, ImmutableMap.of())
    );

    write(df, ImmutableMap.of(CatalogProperties.HIVE_URI, URI_VAL));

    List<SimpleRecord> dfNew = read(ImmutableMap.of(CatalogProperties.HIVE_URI, URI_VAL));

    Assert.assertEquals("Data should match", expected, dfNew);
  }

  private List<SimpleRecord> read(Map<String, String> options) {
    DataFrameReader builder = spark.read().format("iceberg");
    for (Map.Entry<String, String> x : options.entrySet()) {
      builder.option(x.getKey(), x.getValue());
    }
    return builder
        .load(TABLE.toString())
        .orderBy("id")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();
  }

  private void write(Dataset<Row> df, Map<String, String> options) {
    DataFrameWriter<Row> builder = df.select("id", "data").write()
        .format("iceberg")
        .mode("append");
    for (Map.Entry<String, String> x : options.entrySet()) {
      builder.option(x.getKey(), x.getValue());
    }
    builder.save(TABLE.toString());
  }
}
