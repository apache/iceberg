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
package org.apache.iceberg.hudi;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.QuickstartUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hudi.catalog.HoodieCatalog;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class TestSnapshotHudiTable extends SparkHudiMigrationTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotHudiTable.class.getName());
  private static final String row1 =
      "{\"name\":\"Michael\",\"addresses\":[{\"city\":\"SanJose\",\"state\":\"CA\"},{\"city\":\"Sandiago\",\"state\":\"CA\"}],"
          + "\"address_nested\":{\"current\":{\"state\":\"NY\",\"city\":\"NewYork\"},\"previous\":{\"state\":\"NJ\",\"city\":\"Newark\"}},"
          + "\"properties\":{\"hair\":\"brown\",\"eye\":\"black\"},\"secondProp\":{\"height\":\"6\"},\"subjects\":[[\"Java\",\"Scala\",\"C++\"],"
          + "[\"Spark\",\"Java\"]],\"id\":1,\"magic_number\":1.123123123123}";
  private static final String row2 =
      "{\"name\":\"Test\",\"addresses\":[{\"city\":\"SanJos123123e\",\"state\":\"CA\"},{\"city\":\"Sand12312iago\",\"state\":\"CA\"}],"
          + "\"address_nested\":{\"current\":{\"state\":\"N12Y\",\"city\":\"NewY1231ork\"}},\"properties\":{\"hair\":\"brown\",\"eye\":\"black\"},"
          + "\"secondProp\":{\"height\":\"6\"},\"subjects\":[[\"Java\",\"Scala\",\"C++\"],[\"Spark\",\"Java\"]],\"id\":2,\"magic_number\":2.123123123123}";
  private static final String row3 =
      "{\"name\":\"Test\",\"addresses\":[{\"city\":\"SanJose\",\"state\":\"CA\"},{\"city\":\"Sandiago\",\"state\":\"CA\"}],"
          + "\"properties\":{\"hair\":\"brown\",\"eye\":\"black\"},\"secondProp\":{\"height\":\"6\"},\"subjects\":"
          + "[[\"Java\",\"Scala\",\"C++\"],[\"Spark\",\"Java\"]],\"id\":3,\"magic_number\":3.123123123123}";
  private static final String row4 =
      "{\"name\":\"John\",\"addresses\":[{\"city\":\"LA\",\"state\":\"CA\"},{\"city\":\"Sandiago\",\"state\":\"CA\"}],"
          + "\"address_nested\":{\"current\":{\"state\":\"NY\",\"city\":\"NewYork\"},\"previous\":{\"state\":\"NJ123\"}},"
          + "\"properties\":{\"hair\":\"b12rown\",\"eye\":\"bla3221ck\"},\"secondProp\":{\"height\":\"633\"},\"subjects\":"
          + "[[\"Spark\",\"Java\"]],\"id\":4,\"magic_number\":4.123123123123}";
  private static final String row5 =
      "{\"name\":\"Jonas\",\"addresses\":[{\"city\":\"Pittsburgh\",\"state\":\"PA\"},{\"city\":\"Sandiago\",\"state\":\"CA\"}],"
          + "\"address_nested\":{\"current\":{\"state\":\"PA\",\"city\":\"Haha\"},\"previous\":{\"state\":\"NJ\"}},"
          + "\"properties\":{\"hair\":\"black\",\"eye\":\"black\"},\"secondProp\":{\"height\":\"7\"},\"subjects\":[[\"Java\",\"Scala\",\"C++\"],"
          + "[\"Spark\",\"Java\"]],\"id\":5,\"magic_number\":5.123123123123}";
  private static final String SNAPSHOT_SOURCE_PROP = "snapshot_source";
  private static final String DELTA_SOURCE_VALUE = "delta";
  private static final String ORIGINAL_LOCATION_PROP = "original_location";
  private static final String NAMESPACE = "delta_conversion_test";
  private static final String defaultSparkCatalog = "spark_catalog";
  private static final String icebergCatalogName = "iceberg_hive";
  private String partitionedIdentifier;
  private String unpartitionedIdentifier;
  private String externalDataFilesIdentifier;
  private final String partitionedTableName = "partitioned_table";
  private final String unpartitionedTableName = "unpartitioned_table";
  private final String externalDataFilesTableName = "external_data_files_table";
  private String partitionedLocation;
  private String unpartitionedLocation;
  private String newIcebergTableLocation;
  private String externalDataFilesTableLocation;

  @Parameterized.Parameters(name = "Catalog Name {0} - Options {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      new Object[] {
        icebergCatalogName,
        SparkSessionCatalog.class.getName(),
        ImmutableMap.of(
            "type",
            "hive",
            "default-namespace",
            "default",
            "parquet-enabled",
            "true",
            "cache-enabled",
            "false" // Spark will delete tables using v1, leaving the cache out of sync
            )
      }
    };
  }

  @Rule public TemporaryFolder temp1 = new TemporaryFolder();
  @Rule public TemporaryFolder temp2 = new TemporaryFolder();
  @Rule public TemporaryFolder temp3 = new TemporaryFolder();
  @Rule public TemporaryFolder temp4 = new TemporaryFolder();

  public TestSnapshotHudiTable(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
    spark.conf().set("spark.sql.catalog." + defaultSparkCatalog, HoodieCatalog.class.getName());
  }

  /**
   * The test hardcode a nested dataframe to test the snapshot feature. The schema of created
   * dataframe is:
   *
   * <pre>
   *  root
   *  |-- address_nested: struct (nullable = true)
   *  |    |-- current: struct (nullable = true)
   *  |    |    |-- city: string (nullable = true)
   *  |    |    |-- state: string (nullable = true)
   *  |    |-- previous: struct (nullable = true)
   *  |    |    |-- city: string (nullable = true)
   *  |    |    |-- state: string (nullable = true)
   *  |-- addresses: array (nullable = true)
   *  |    |-- element: struct (containsNull = true)
   *  |    |    |-- city: string (nullable = true)
   *  |    |    |-- state: string (nullable = true)
   *  |-- id: long (nullable = true)
   *  |-- magic_number: double (nullable = true)
   *  |-- name: string (nullable = true)
   *  |-- properties: struct (nullable = true)
   *  |    |-- eye: string (nullable = true)
   *  |    |-- hair: string (nullable = true)
   *  |-- secondProp: struct (nullable = true)
   *  |    |-- height: string (nullable = true)
   *  |-- subjects: array (nullable = true)
   *  |    |-- element: array (containsNull = true)
   *  |    |    |-- element: string (containsNull = true)
   * </pre>
   *
   * The dataframe content is (by calling df.show()):
   *
   * <pre>
   * +--------------------+--------------------+---+--------------+-------+--------------------+----------+--------------------+
   * |      address_nested|           addresses| id|  magic_number|   name|          properties|secondProp|            subjects|
   * +--------------------+--------------------+---+--------------+-------+--------------------+----------+--------------------+
   * |{{NewYork, NY}, {...|[{SanJose, CA}, {...|  1|1.123123123123|Michael|      {black, brown}|       {6}|[[Java, Scala, C+...|
   * |{{NewY1231ork, N1...|[{SanJos123123e, ...|  2|2.123123123123|   Test|      {black, brown}|       {6}|[[Java, Scala, C+...|
   * |                null|[{SanJose, CA}, {...|  3|3.123123123123|   Test|      {black, brown}|       {6}|[[Java, Scala, C+...|
   * |{{NewYork, NY}, {...|[{LA, CA}, {Sandi...|  4|4.123123123123|   John|{bla3221ck, b12rown}|     {633}|     [[Spark, Java]]|
   * |{{Haha, PA}, {nul...|[{Pittsburgh, PA}...|  5|5.123123123123|  Jonas|      {black, black}|       {7}|[[Java, Scala, C+...|
   * +--------------------+--------------------+---+--------------+-------+--------------------+----------+--------------------+
   * </pre>
   */
  @Before
  public void before() throws IOException {
    File partitionedFolder = temp1.newFolder();
    File unpartitionedFolder = temp2.newFolder();
    File newIcebergTableFolder = temp3.newFolder();
    File externalDataFilesTableFolder = temp4.newFolder();
    partitionedLocation = partitionedFolder.toURI().toString();
    unpartitionedLocation = unpartitionedFolder.toURI().toString();
    newIcebergTableLocation = newIcebergTableFolder.toURI().toString();
    externalDataFilesTableLocation = externalDataFilesTableFolder.toURI().toString();

    spark.sql(String.format("CREATE DATABASE IF NOT EXISTS %s", NAMESPACE));

    partitionedIdentifier = destName(defaultSparkCatalog, partitionedTableName);
    unpartitionedIdentifier = destName(defaultSparkCatalog, unpartitionedTableName);
    externalDataFilesIdentifier = destName(defaultSparkCatalog, externalDataFilesTableName);

    spark.sql(String.format("DROP TABLE IF EXISTS %s", partitionedIdentifier));
    spark.sql(String.format("DROP TABLE IF EXISTS %s", unpartitionedIdentifier));
    spark.sql(String.format("DROP TABLE IF EXISTS %s", externalDataFilesIdentifier));

    // hard code the dataframe
    List<String> jsonList = Lists.newArrayList();
    jsonList.add(row1);
    jsonList.add(row2);
    jsonList.add(row3);
    jsonList.add(row4);
    jsonList.add(row5);
    JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    SQLContext sqlContext = new SQLContext(javaSparkContext);
    JavaRDD<String> rdd = javaSparkContext.parallelize(jsonList);
    Dataset<Row> df = sqlContext.read().json(rdd);

    df.write()
        .format("hudi")
        .options(QuickstartUtils.getQuickstartWriteConfigs())
        .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "magic_number")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "name")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "id")
        .option(HoodieWriteConfig.TABLE_NAME, partitionedIdentifier)
        .mode(SaveMode.Overwrite)
        .save(partitionedLocation);

    df.write()
        .format("hudi")
        .options(QuickstartUtils.getQuickstartWriteConfigs())
        .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "magic_number")
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "name")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "")
        .option(HoodieWriteConfig.TABLE_NAME, unpartitionedIdentifier)
        .mode(SaveMode.Overwrite)
        .save(unpartitionedLocation);
  }

  @Test
  public void TestHudiUnpartitionedTableWrite() {
    Dataset<Row> df = spark.read().format("hudi").load(unpartitionedLocation);
    LOG.info("Generated unpartitioned dataframe shcema: {}", df.schema().treeString());
    LOG.info("Generated unpartitioned dataframe: {}", df.showString(10, 20, false));
  }

  @Test
  public void TestHudiPartitionedTableWrite() {
    Dataset<Row> df = spark.read().format("hudi").load(partitionedLocation);
    LOG.info("Generated partitioned dataframe shcema: {}", df.schema().treeString());
    LOG.info("Generated partitioned dataframe: {}", df.showString(10, 20, false));
  }

  private String destName(String catalogName, String dest) {
    if (catalogName.equals(defaultSparkCatalog)) {
      return NAMESPACE + "." + catalogName + "_" + dest;
    }
    return catalogName + "." + NAMESPACE + "." + catalogName + "_" + dest;
  }
}
