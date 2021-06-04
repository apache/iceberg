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
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
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
  private static final String CATALOG_IMPL = String.format("%s.%s.%s", CustomCatalogs.ICEBERG_CATALOG_PREFIX,
      CustomCatalogs.ICEBERG_DEFAULT_CATALOG, CatalogProperties.CATALOG_IMPL);
  private static final String WAREHOUSE = String.format("%s.%s.%s", CustomCatalogs.ICEBERG_CATALOG_PREFIX,
      CustomCatalogs.ICEBERG_DEFAULT_CATALOG, CatalogProperties.WAREHOUSE_LOCATION);
  private static final String URI_KEY = String.format("%s.%s.%s", CustomCatalogs.ICEBERG_CATALOG_PREFIX,
      CustomCatalogs.ICEBERG_DEFAULT_CATALOG, CatalogProperties.URI);
  private static final String TEST_CATALOG = "placeholder_catalog";
  private static final String TEST_CATALOG_IMPL = String.format("%s.%s.%s", CustomCatalogs.ICEBERG_CATALOG_PREFIX,
      TEST_CATALOG, CatalogProperties.CATALOG_IMPL);
  private static final String TEST_WAREHOUSE = String.format("%s.%s.%s", CustomCatalogs.ICEBERG_CATALOG_PREFIX,
      TEST_CATALOG, CatalogProperties.WAREHOUSE_LOCATION);
  private static final String TEST_URI_KEY = String.format("%s.%s.%s", CustomCatalogs.ICEBERG_CATALOG_PREFIX,
      TEST_CATALOG, CatalogProperties.URI);
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
    SparkConf sparkConf = spark.sparkContext().conf();
    sparkConf.set(
        String.format("%s.%s", CustomCatalogs.ICEBERG_CATALOG_PREFIX, CustomCatalogs.ICEBERG_DEFAULT_CATALOG),
        "placeholder");
    sparkConf.set(
        String.format("%s.%s", CustomCatalogs.ICEBERG_CATALOG_PREFIX, TEST_CATALOG),
        "placeholder");
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
    AssertHelpers.assertThrows("We have not set all properties", IllegalArgumentException.class,
        "The base path for the catalog's warehouse directory must be set", () ->
        df.select("id", "data").write()
            .format("iceberg")
            .mode("append")
            .save(TABLE.toString())
    );

    sparkConf.set(WAREHOUSE, tableLocation);

    df.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(TABLE.toString());

    List<SimpleRecord> dfNew = spark.read().format("iceberg")
        .load(TABLE.toString())
        .orderBy("id")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();

    Assert.assertEquals("Data should match", expected, dfNew);
  }

  @Test
  public void withSparkCatalog() {

    String catalogTable = String.format("%s.%s", TEST_CATALOG, TABLE.toString());
    SparkConf sparkConf = spark.sparkContext().conf();
    sparkConf.set(TEST_CATALOG_IMPL, CATALOG_VAL);
    sparkConf.set(TEST_URI_KEY, URI_VAL);

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);
    AssertHelpers.assertThrows("We have not set all properties", IllegalArgumentException.class,
        "The base path for the catalog's warehouse directory must be set", () ->
            df.select("id", "data").write()
                .format("iceberg")
                .mode("append")
                .save(catalogTable)
    );

    sparkConf.set(TEST_WAREHOUSE, tableLocation);

    df.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(catalogTable);

    List<SimpleRecord> dfNew = spark.read().format("iceberg")
        .load(catalogTable)
        .orderBy("id")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();

    Assert.assertEquals("Data should match", expected, dfNew);
  }

}
