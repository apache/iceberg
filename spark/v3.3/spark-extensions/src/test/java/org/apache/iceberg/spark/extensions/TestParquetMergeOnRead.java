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

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.SparkParquetWriters;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestParquetMergeOnRead extends SparkExtensionsTestBase {

  private final boolean vectorized;

  public TestParquetMergeOnRead(
      String catalogName, String implementation, Map<String, String> config, boolean vectorized) {
    super(catalogName, implementation, config);
    this.vectorized = vectorized;
  }

  @Parameterized.Parameters(
      name = "catalogName = {0}, implementation = {1}, config = {2}, vectorized = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.SPARK.catalogName(),
        SparkCatalogConfig.SPARK.implementation(),
        SparkCatalogConfig.SPARK.properties(),
        false
      },
      {
        SparkCatalogConfig.SPARK.catalogName(),
        SparkCatalogConfig.SPARK.implementation(),
        SparkCatalogConfig.SPARK.properties(),
        true
      }
    };
  }

  @BeforeClass
  public static void startMetastoreAndSpark() {
    metastore = new TestHiveMetastore();
    metastore.start();
    HiveConf hiveConf = metastore.hiveConf();

    spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
            .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
            .enableHiveSupport()
            .getOrCreate();

    catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);

    try {
      catalog.createNamespace(Namespace.of("default"));
    } catch (AlreadyExistsException ignored) {
      // The default namespace already exists. Ignore the error.
    }
  }

  @AfterClass
  public static void stopMetastoreAndSpark() throws Exception {
    catalog = null;
    metastore.stop();
    metastore = null;
    spark.stop();
    spark = null;
  }

  @After
  public void dropTable() {
    spark.sql("DROP TABLE IF EXISTS default.test_iceberg");
  }

  @Test
  public void testReadOfDataFileWithMultipleSplits() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "a", Types.LongType.get()),
            Types.NestedField.required(1, "b", Types.IntegerType.get()),
            Types.NestedField.optional(2, "c", Types.IntegerType.get()),
            Types.NestedField.optional(3, "d", Types.IntegerType.get()),
            Types.NestedField.optional(5, "e", Types.DoubleType.get()),
            Types.NestedField.required(6, "f", Types.DecimalType.of(9, 2)),
            Types.NestedField.optional(7, "g", Types.DecimalType.of(9, 2)),
            Types.NestedField.optional(8, "h", Types.DecimalType.of(9, 2)));

    int numRows = 5_000_000;
    Iterable<InternalRow> records = RandomData.generateSpark(schema, numRows, 19981);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<InternalRow> writer =
        Parquet.write(Files.localOutput(testFile))
            .schema(schema)
            .createWriterFunc(
                msgType ->
                    SparkParquetWriters.buildWriter(SparkSchemaUtil.convert(schema), msgType))
            .build()) {
      writer.addAll(records);
    }

    Dataset<Row> df = spark.read().parquet(testFile.getAbsolutePath());
    df.createOrReplaceTempView("data");

    String formatSetting = "'write.format.default'='parquet', ";
    String vectorizationSetting = "'read.parquet.vectorization.enabled'='false'";
    if (vectorized) {
      vectorizationSetting = "'read.parquet.vectorization.enabled'='true'";
    }
    String ddl =
        "CREATE TABLE default.test_iceberg ("
            + "a LONG, "
            + "b INT, "
            + "c INT, "
            + "d INT, "
            + "e DOUBLE, "
            + "f DECIMAL(9,2), "
            + "g DECIMAL(9,2), "
            + "h DECIMAL(9,2)) "
            + "USING iceberg "
            + "TBLPROPERTIES("
            + "'format-version'='2', "
            + "'write.delete.mode'='merge-on-read', "
            + "'write.update.mode'='merge-on-read', "
            + "'write.merge.mode'='merge-on-read', "
            + formatSetting
            + vectorizationSetting
            + ")";
    spark.sql(ddl);

    spark.sql("insert overwrite default.test_iceberg select a, b, c, d, e, f, g, h from data");
    assertEquals(
        "test_iceberg should contain 5000000 rows",
        ImmutableList.of(row(5000000L)),
        sql("select count(*) from default.test_iceberg"));
    assertEquals(
        "test_iceberg should contain some rows where e is null",
        ImmutableList.of(row(250279L)),
        sql("select count(*) from default.test_iceberg where e is null"));
    spark.sql("update default.test_iceberg set e = 0.0 where e is null");
    assertEquals(
        "After the update there should be no rows where e is null",
        ImmutableList.of(row(0L)),
        sql("select count(*) from default.test_iceberg where e is null"));
  }
}
