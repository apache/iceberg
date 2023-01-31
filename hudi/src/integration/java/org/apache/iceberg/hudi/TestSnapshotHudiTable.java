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

import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.expr;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.hudi.catalog.HoodieCatalog;
import org.assertj.core.api.Assertions;
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
  private Dataset<Row> typeTestDataframe = typeTestDataFrame();
  private Dataset<Row> nestedDataframe = nestedDataFrame();

  @Parameterized.Parameters(name = "Catalog Name {0} - Options {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      new Object[] {
        icebergCatalogName,
        SparkCatalog.class.getName(),
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

    //    typeTestDataframe.write()
    //        .format("hudi")
    //        .options(QuickstartUtils.getQuickstartWriteConfigs())
    //        .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "decimalCol")
    //        .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "intCol")
    //        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partitionPath")
    //        .option(HoodieWriteConfig.TABLE_NAME, partitionedIdentifier)
    //        .mode(SaveMode.Overwrite)
    //        .save(partitionedLocation);
    writeHoodieTable(
        typeTestDataframe,
        "decimalCol",
        "intCol",
        "partitionPath",
        partitionedLocation,
        partitionedIdentifier);

    //    df.write()
    //        .format("hudi")
    //        .options(QuickstartUtils.getQuickstartWriteConfigs())
    //        .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "magic_number")
    //        .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "name")
    //        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "")
    //        .option(HoodieWriteConfig.TABLE_NAME, unpartitionedIdentifier)
    //        .mode(SaveMode.Overwrite)
    //        .save(unpartitionedLocation);
    writeHoodieTable(
        typeTestDataframe,
        "decimalCol",
        "intCol",
        "",
        unpartitionedLocation,
        unpartitionedIdentifier);
  }

  @Test
  public void testHudiUnpartitionedTableWrite() {
    Dataset<Row> df = spark.read().format("hudi").load(unpartitionedLocation);
    LOG.info("Generated unpartitioned dataframe shcema: {}", df.schema().treeString());
    LOG.info("Generated unpartitioned dataframe: {}", df.showString(10, 20, false));
  }

  @Test
  public void testHudiPartitionedTableWrite() {
    Dataset<Row> df = spark.read().format("hudi").load(partitionedLocation);
    LOG.info("Generated partitioned dataframe shcema: {}", df.schema().treeString());
    LOG.info("Generated partitioned dataframe: {}", df.showString(10, 20, false));
  }

  @Test
  public void testBasicPartitionedTable() {
    LOG.info("Alpha test reference: hoodie table path: {}", partitionedLocation);
    String newTableIdentifier = destName(icebergCatalogName, "alpha_iceberg_table");
    SnapshotHudiTable.Result result =
        HudiToIcebergMigrationSparkIntegration.snapshotHudiTable(
                spark, partitionedLocation, newTableIdentifier)
            .execute();
    Table table = getIcebergTable(newTableIdentifier);
    queryManual(table);
    // checkSnapshotIntegrity(partitionedLocation, newTableIdentifier);
  }

  @Test
  public void referenceIcebergTable() {
    String newTableIdentifier = destName(icebergCatalogName, "reference_iceberg_table");
    typeTestDataframe
        .writeTo(newTableIdentifier)
        .using("iceberg")
        .tableProperty(
            TableProperties.WRITE_DATA_LOCATION,
            "/Users/jonasjiang/Workspace/Apache_Hudi_ws/hudi_table_test/unpartitioned_iceberg_ref")
        .tableProperty(
            TableProperties.WRITE_METADATA_LOCATION,
            "/Users/jonasjiang/Workspace/Apache_Hudi_ws/hudi_table_test/unpartitioned_iceberg_ref/metadata")
        .createOrReplace();
    Table table = getIcebergTable(newTableIdentifier);
    queryManual(table);
  }

  @Test
  public void testBasicUnpartitionedTable() {
    String newTableIdentifier = destName(icebergCatalogName, "alpha_iceberg_table_2");
    SnapshotHudiTable.Result result =
        HudiToIcebergMigrationSparkIntegration.snapshotHudiTable(
                spark, unpartitionedLocation, newTableIdentifier)
            .execute();

    Dataset<Row> hudiResult =
        spark
            .read()
            .format("hudi")
            .option(
                DataSourceReadOptions.QUERY_TYPE_OPT_KEY(),
                DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
            .load(unpartitionedLocation);
    LOG.info("Hudi table contents: {}", hudiResult.showString(10, 20, false));
    Table table = getIcebergTable(newTableIdentifier);
    queryManual(table);
    checkSnapshotIntegrity(unpartitionedLocation, newTableIdentifier);
  }

  private void checkSnapshotIntegrity(String hudiTableLocation, String icebergTableIdentifier) {
    Dataset<Row> hudiResult =
        spark
            .read()
            .format("hudi")
            .option(
                DataSourceReadOptions.QUERY_TYPE_OPT_KEY(),
                DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
            .load(hudiTableLocation);
    Dataset<Row> icebergResult = spark.sql("SELECT * FROM " + icebergTableIdentifier);
    LOG.info("Hudi table contents: {}", hudiResult.showString(10, 20, false));
    LOG.info("Iceberg table contents: {}", icebergResult.showString(10, 20, false));
    // TODO: adjust test technique since hudi tends to return the columns in a different order (put
    // the one used for partitioning last)
    List<Row> hudiTableContents = hudiResult.collectAsList();
    List<Row> icebergTableContents = icebergResult.collectAsList();

    Assertions.assertThat(hudiTableContents).hasSize(icebergTableContents.size());
    Assertions.assertThat(hudiTableContents).containsAll(icebergTableContents);
    Assertions.assertThat(icebergTableContents)
        .containsAll(hudiTableContents); // TODO: may change to containsExactlyInAnyOrderElementsOf
  }

  private void queryManual(Table table) {
    CloseableIterable<Record> records = IcebergGenerics.read(table).build();
    for (Record record : records) {
      LOG.info("Alpha Test Iceberg Record: {}", record);
    }
  }

  private Table getIcebergTable(String icebergTableIdentifier) {
    CatalogPlugin defaultCatalog = spark.sessionState().catalogManager().currentCatalog();
    Spark3Util.CatalogAndIdentifier catalogAndIdent =
        Spark3Util.catalogAndIdentifier(
            "test catalog", spark, icebergTableIdentifier, defaultCatalog);
    return Spark3Util.loadIcebergCatalog(spark, catalogAndIdent.catalog().name())
        .loadTable(TableIdentifier.parse(catalogAndIdent.identifier().toString()));
  }

  private String destName(String catalogName, String dest) {
    if (catalogName.equals(defaultSparkCatalog)) {
      return NAMESPACE + "." + catalogName + "_" + dest;
    }
    return catalogName + "." + NAMESPACE + "." + catalogName + "_" + dest;
  }

  private Dataset<Row> typeTestDataFrame() {
    return spark
        .range(0, 5, 1, 5)
        .withColumnRenamed("id", "longCol")
        .withColumn("intCol", expr("CAST(longCol AS INT)"))
        //        .withColumn("floatCol", expr("CAST(longCol AS FLOAT)"))
        //        .withColumn("doubleCol", expr("CAST(longCol AS DOUBLE)"))
        .withColumn("dateCol", date_add(current_date(), 1))
        //        .withColumn("timestampCol", expr("TO_TIMESTAMP(dateCol)"))
        .withColumn("stringCol", expr("CAST(dateCol AS STRING)"))
        //        .withColumn("booleanCol", expr("longCol > 5"))
        //        .withColumn("binaryCol", expr("CAST(longCol AS BINARY)"))
        //        .withColumn("byteCol", expr("CAST(longCol AS BYTE)"))
        .withColumn("decimalCol", expr("CAST(longCol AS DECIMAL(10, 2))"))
        //        .withColumn("shortCol", expr("CAST(longCol AS SHORT)"))
        .withColumn("mapCol", expr("MAP(stringCol, intCol)")) // Hudi requires Map key to be String
        .withColumn("arrayCol", expr("ARRAY(dateCol)"))
        //        .withColumn("structCol", expr("STRUCT(longCol AS a, longCol AS b)"))
        .withColumn(
            "partitionPath",
            expr("CAST(longCol AS STRING)")); // For test convenience, please put the partition col
    // in the end.
  }

  private Dataset<Row> nestedDataFrame() {
    return spark
        .range(0, 5, 1, 5)
        .withColumn("longCol", expr("id"))
        .withColumn("decimalCol", expr("CAST(longCol AS DECIMAL(10, 2))"))
        .withColumn("magic_number", expr("rand(5) * 100"))
        .withColumn("dateCol", date_add(current_date(), 1))
        .withColumn("dateString", expr("CAST(dateCol AS STRING)"))
        .withColumn("random1", expr("CAST(rand(5) * 100 as LONG)"))
        .withColumn("random2", expr("CAST(rand(51) * 100 as LONG)"))
        .withColumn("random3", expr("CAST(rand(511) * 100 as LONG)"))
        .withColumn("random4", expr("CAST(rand(15) * 100 as LONG)"))
        .withColumn("random5", expr("CAST(rand(115) * 100 as LONG)"))
        .withColumn("innerStruct1", expr("STRUCT(random1, random2)"))
        .withColumn("innerStruct2", expr("STRUCT(random3, random4)"))
        .withColumn("structCol1", expr("STRUCT(innerStruct1, innerStruct2)"))
        .withColumn(
            "innerStruct3",
            expr("STRUCT(SHA1(CAST(random5 AS BINARY)), SHA1(CAST(random1 AS BINARY)))"))
        .withColumn(
            "structCol2",
            expr(
                "STRUCT(innerStruct3, STRUCT(SHA1(CAST(random2 AS BINARY)), SHA1(CAST(random3 AS BINARY))))"))
        .withColumn("arrayCol", expr("ARRAY(random1, random2, random3, random4, random5)"))
        .withColumn("mapCol1", expr("MAP(structCol1, structCol2)"))
        .withColumn("mapCol2", expr("MAP(longCol, dateString)"))
        .withColumn("mapCol3", expr("MAP(dateCol, arrayCol)"))
        .withColumn("structCol3", expr("STRUCT(structCol2, mapCol3, arrayCol)"));
  }

  private void writeHoodieTable(
      Dataset<Row> df,
      String recordKey,
      String preCombineKey,
      String partitionPathField,
      String tableLocation,
      String tableIdentifier) {
    df.write()
        .format("hudi")
        //        .options(QuickstartUtils.getQuickstartWriteConfigs())
        .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), recordKey)
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), preCombineKey)
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), partitionPathField)
        .option(HoodieWriteConfig.TBL_NAME.key(), tableIdentifier)
        .mode(SaveMode.Append)
        .save(tableLocation);
  }
}
