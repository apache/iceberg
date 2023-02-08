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
import org.apache.iceberg.catalog.TableIdentifier;
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
  private static final String HUDI_SOURCE_VALUE = "hudi";
  private static final String ORIGINAL_LOCATION_PROP = "original_location";
  private static final String NAMESPACE = "delta_conversion_test";
  private static final String defaultSparkCatalog = "spark_catalog";
  private static final String icebergCatalogName = "iceberg_hive";
  private String partitionedIdentifier;
  private String unpartitionedIdentifier;
  private String multiCommitIdentifier;
  private final String partitionedTableName = "partitioned_table";
  private final String unpartitionedTableName = "unpartitioned_table";
  private final String multiCommitTableName = "multi_commit_table";
  private String partitionedLocation;
  private String unpartitionedLocation;
  private String newIcebergTableLocation;
  private String multiCommitTableLocation;
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

  @Before
  public void before() throws IOException {
    File partitionedFolder = temp1.newFolder();
    File unpartitionedFolder = temp2.newFolder();
    File newIcebergTableFolder = temp3.newFolder();
    File multiCommitTableFolder = temp4.newFolder();
    partitionedLocation = partitionedFolder.toURI().toString();
    unpartitionedLocation = unpartitionedFolder.toURI().toString();
    newIcebergTableLocation = newIcebergTableFolder.toURI().toString();
    multiCommitTableLocation = multiCommitTableFolder.toURI().toString();

    spark.sql(String.format("CREATE DATABASE IF NOT EXISTS %s", NAMESPACE));

    partitionedIdentifier = destName(defaultSparkCatalog, partitionedTableName);
    unpartitionedIdentifier = destName(defaultSparkCatalog, unpartitionedTableName);
    multiCommitIdentifier = destName(defaultSparkCatalog, multiCommitTableName);

    spark.sql(String.format("DROP TABLE IF EXISTS %s", partitionedIdentifier));
    spark.sql(String.format("DROP TABLE IF EXISTS %s", unpartitionedIdentifier));
    spark.sql(String.format("DROP TABLE IF EXISTS %s", multiCommitIdentifier));
  }

  @Test
  public void testBasicPartitionedTable() {
    writeHoodieTable(
        typeTestDataframe,
        "decimalCol",
        "intCol",
        "partitionPath",
        SaveMode.Overwrite,
        partitionedLocation,
        partitionedIdentifier);
    LOG.info("Alpha test reference: hoodie table path: {}", partitionedLocation);
    String newTableIdentifier = destName(icebergCatalogName, "alpha_iceberg_table");
    SnapshotHudiTable.Result result =
        HudiToIcebergMigrationSparkIntegration.snapshotHudiTable(
                spark, partitionedLocation, newTableIdentifier)
            .execute();
    checkSnapshotIntegrity(partitionedLocation, newTableIdentifier);
    checkIcebergTableLocation(newTableIdentifier, partitionedLocation);
    checkIcebergTableProperties(newTableIdentifier, ImmutableMap.of(), partitionedLocation);
  }

  @Test
  public void testBasicUnpartitionedTable() {
    writeHoodieTable(
        typeTestDataframe,
        "decimalCol",
        "intCol",
        "",
        SaveMode.Overwrite,
        unpartitionedLocation,
        unpartitionedIdentifier);
    String newTableIdentifier = destName(icebergCatalogName, "alpha_iceberg_table_2");
    SnapshotHudiTable.Result result =
        HudiToIcebergMigrationSparkIntegration.snapshotHudiTable(
                spark, unpartitionedLocation, newTableIdentifier)
            .execute();
    checkSnapshotIntegrity(unpartitionedLocation, newTableIdentifier);
    checkIcebergTableLocation(newTableIdentifier, unpartitionedLocation);
    checkIcebergTableProperties(newTableIdentifier, ImmutableMap.of(), unpartitionedLocation);
  }

  @Test
  public void testMultiCommitTable() {
    Dataset<Row> initialDataFrame = multiDataFrame(0, 2);
    writeHoodieTable(
        initialDataFrame,
        "decimalCol",
        "magic_number",
        "partitionPath2",
        SaveMode.Append,
        multiCommitTableLocation,
        multiCommitIdentifier);
    writeHoodieTable(
        initialDataFrame,
        "decimalCol",
        "magic_number",
        "partitionPath2",
        SaveMode.Append,
        multiCommitTableLocation,
        multiCommitIdentifier);
    writeHoodieTable(
        multiDataFrame(2, 5),
        "decimalCol",
        "magic_number",
        "partitionPath2",
        SaveMode.Append,
        multiCommitTableLocation,
        multiCommitIdentifier);
    writeHoodieTable(
        multiDataFrame(0, 1),
        "decimalCol",
        "magic_number",
        "partitionPath2",
        SaveMode.Append,
        multiCommitTableLocation,
        multiCommitIdentifier);
    Dataset<Row> toDelete = multiDataFrame(4, 5);
    writeHoodieTable(
        toDelete,
        "decimalCol",
        "magic_number",
        "partitionPath2",
        SaveMode.Append,
        multiCommitTableLocation,
        multiCommitIdentifier);
    writeHoodieTableOperation(
        toDelete,
        DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL(),
        "decimalCol",
        "magic_number",
        "partitionPath2",
        SaveMode.Append,
        multiCommitTableLocation,
        multiCommitIdentifier);

    String newTableIdentifier = destName(icebergCatalogName, "alpha_iceberg_table_3");
    SnapshotHudiTable.Result result =
        HudiToIcebergMigrationSparkIntegration.snapshotHudiTable(
                spark, multiCommitTableLocation, newTableIdentifier)
            .execute();
    checkSnapshotIntegrity(multiCommitTableLocation, newTableIdentifier);
    checkIcebergTableLocation(newTableIdentifier, multiCommitTableLocation);
    checkIcebergTableProperties(newTableIdentifier, ImmutableMap.of(), multiCommitTableLocation);
  }

  @Test
  public void testSnapshotWithNewLocation() {
    writeHoodieTable(
        typeTestDataframe,
        "decimalCol",
        "intCol",
        "partitionPath",
        SaveMode.Overwrite,
        partitionedLocation,
        partitionedIdentifier);
    String newTableIdentifier = destName(icebergCatalogName, "alpha_iceberg_table_4");
    SnapshotHudiTable.Result result =
        HudiToIcebergMigrationSparkIntegration.snapshotHudiTable(
                spark, partitionedLocation, newTableIdentifier)
            .tableLocation(newIcebergTableLocation)
            .execute();
    checkSnapshotIntegrity(partitionedLocation, newTableIdentifier);
    checkIcebergTableLocation(newTableIdentifier, newIcebergTableLocation);
  }

  @Test
  public void testSnapshotWithAdditionalProperties() {
    writeHoodieTable(
        typeTestDataframe,
        "decimalCol",
        "intCol",
        "partitionPath",
        SaveMode.Overwrite,
        partitionedLocation,
        partitionedIdentifier);
    String newTableIdentifier = destName(icebergCatalogName, "alpha_iceberg_table_5");
    SnapshotHudiTable.Result result =
        HudiToIcebergMigrationSparkIntegration.snapshotHudiTable(
                spark, partitionedLocation, newTableIdentifier)
            .tableProperties(ImmutableMap.of("test", "test"))
            .execute();
    checkSnapshotIntegrity(partitionedLocation, newTableIdentifier);
    checkIcebergTableProperties(
        newTableIdentifier, ImmutableMap.of("test", "test"), partitionedLocation);
  }

  @Test
  public void testSnapshotWithComplexKeyGen() {
    writeHoodieTableKeyGenerator(
        multiDataFrame(0, 1),
        "decimalCol,dateCol",
        "magic_number",
        "zpartitionPath,partitionPath,partitionPath2",
        SaveMode.Append,
        partitionedLocation,
        partitionedIdentifier);
    String newTableIdentifier = destName(icebergCatalogName, "alpha_iceberg_table_6");
    SnapshotHudiTable.Result result =
        HudiToIcebergMigrationSparkIntegration.snapshotHudiTable(
                spark, partitionedLocation, newTableIdentifier)
            .tableProperties(ImmutableMap.of("test", "test"))
            .execute();
    checkSnapshotIntegrity(partitionedLocation, newTableIdentifier);
    checkIcebergTableProperties(
        newTableIdentifier, ImmutableMap.of("test", "test"), partitionedLocation);
  }

  private void checkSnapshotIntegrity(String hudiTableLocation, String icebergTableIdentifier) {
    Dataset<Row> hudiResult =
        spark
            .read()
            .format("hudi")
            .option(
                DataSourceReadOptions.QUERY_TYPE().key(),
                DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
            .load(hudiTableLocation);
    Dataset<Row> icebergResult = spark.sql("SELECT * FROM " + icebergTableIdentifier);
    // TODO: adjust test technique since hudi tends to return the columns in a different order (put
    // the one used for partitioning last)
    List<Row> hudiTableContents = hudiResult.collectAsList();
    List<Row> icebergTableContents = icebergResult.collectAsList();

    Assertions.assertThat(hudiTableContents).hasSize(icebergTableContents.size());
    Assertions.assertThat(hudiTableContents)
        .containsExactlyInAnyOrderElementsOf(icebergTableContents);
  }

  private void checkIcebergTableLocation(String icebergTableIdentifier, String expectedLoacation) {
    Table table = getIcebergTable(icebergTableIdentifier);
    Assertions.assertThat(table.location()).isEqualTo(expectedLoacation);
  }

  private void checkIcebergTableProperties(
      String icebergTableIdentifier,
      Map<String, String> expectedAdditionalProperties,
      String hudiTableLocation) {
    Table icebergTable = getIcebergTable(icebergTableIdentifier);
    ImmutableMap.Builder<String, String> expectedPropertiesBuilder = ImmutableMap.builder();
    // The snapshot action will put some fixed properties to the table
    expectedPropertiesBuilder.put(SNAPSHOT_SOURCE_PROP, HUDI_SOURCE_VALUE);
    expectedPropertiesBuilder.putAll(expectedAdditionalProperties);
    ImmutableMap<String, String> expectedProperties = expectedPropertiesBuilder.build();

    Assertions.assertThat(icebergTable.properties().entrySet())
        .containsAll(expectedProperties.entrySet());
    Assertions.assertThat(icebergTable.properties())
        .containsEntry(ORIGINAL_LOCATION_PROP, hudiTableLocation);
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
        .withColumn("floatCol", expr("CAST(longCol AS FLOAT)"))
        .withColumn("doubleCol", expr("CAST(longCol AS DOUBLE)"))
        .withColumn("dateCol", date_add(current_date(), 1))
        .withColumn("timestampCol", expr("TO_TIMESTAMP(dateCol)"))
        .withColumn("stringCol", expr("CAST(dateCol AS STRING)"))
        .withColumn("booleanCol", expr("longCol > 5"))
        .withColumn("binaryCol", expr("CAST(longCol AS BINARY)"))
        .withColumn("byteCol", expr("CAST(longCol AS BYTE)"))
        .withColumn("decimalCol", expr("CAST(longCol AS DECIMAL(10, 2))"))
        .withColumn("shortCol", expr("CAST(longCol AS SHORT)"))
        .withColumn("mapCol", expr("MAP(stringCol, intCol)")) // Hudi requires Map key to be String
        .withColumn("arrayCol", expr("ARRAY(dateCol)"))
        .withColumn("structCol", expr("STRUCT(longCol AS a, longCol AS b)"))
        .withColumn("partitionPath", expr("CAST(longCol AS STRING)"));
  }

  private Dataset<Row> multiDataFrame(int start, int end) {
    return spark
        .range(start, end, 1, end - start)
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
        .withColumn("zpartitionPath", expr("CAST(dateCol AS STRING)"))
        .withColumn("partitionPath", expr("CAST(id AS STRING)"))
        .withColumn("partitionPath2", expr("CAST(random1 AS STRING)"));
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
      SaveMode saveMode,
      String tableLocation,
      String tableIdentifier) {
    df.write()
        .format("hudi")
        //        .options(QuickstartUtils.getQuickstartWriteConfigs())
        .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), recordKey)
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), preCombineKey)
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), partitionPathField)
        .option(HoodieWriteConfig.TBL_NAME.key(), tableIdentifier)
        .mode(saveMode)
        .save(tableLocation);
  }

  private void writeHoodieTableKeyGenerator(
      Dataset<Row> df,
      String recordKey,
      String preCombineKey,
      String partitionPathField,
      SaveMode saveMode,
      String tableLocation,
      String tableIdentifier) {
    df.write()
        .format("hudi")
        //        .options(QuickstartUtils.getQuickstartWriteConfigs())
        .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), recordKey)
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), preCombineKey)
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), partitionPathField)
        .option(
            DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key(),
            "org.apache.hudi.keygen.ComplexKeyGenerator")
        .option(HoodieWriteConfig.TBL_NAME.key(), tableIdentifier)
        .mode(saveMode)
        .save(tableLocation);
  }

  private void writeHoodieTableOperation(
      Dataset<Row> df,
      String operationKey,
      String recordKey,
      String preCombineKey,
      String partitionPathField,
      SaveMode saveMode,
      String tableLocation,
      String tableIdentifier) {
    df.write()
        .format("hudi")
        .option(DataSourceWriteOptions.OPERATION().key(), operationKey)
        .option(DataSourceWriteOptions.RECORDKEY_FIELD().key(), recordKey)
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), preCombineKey)
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), partitionPathField)
        .option(HoodieWriteConfig.TBL_NAME.key(), tableIdentifier)
        .mode(saveMode)
        .save(tableLocation);
  }
}
