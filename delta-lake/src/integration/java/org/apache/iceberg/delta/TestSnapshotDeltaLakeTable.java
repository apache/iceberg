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
package org.apache.iceberg.delta;

import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.expr;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;
import io.delta.standalone.exceptions.DeltaConcurrentModificationException;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSnapshotDeltaLakeTable extends SparkDeltaLakeSnapshotTestBase {
  private static final String SNAPSHOT_SOURCE_PROP = "snapshot_source";
  private static final String DELTA_SOURCE_VALUE = "delta";
  private static final String ORIGINAL_LOCATION_PROP = "original_location";
  private static final String NAMESPACE = "delta_conversion_test";
  private static final String defaultSparkCatalog = "spark_catalog";
  private static final String icebergCatalogName = "iceberg_hive";
  private String partitionedIdentifier;
  private String unpartitionedIdentifier;
  private String externalDataFilesIdentifier;
  private String typeTestIdentifier;
  private final String partitionedTableName = "partitioned_table";
  private final String unpartitionedTableName = "unpartitioned_table";
  private final String externalDataFilesTableName = "external_data_files_table";
  private final String typeTestTableName = "type_test_table";
  private final String snapshotPartitionedTableName = "iceberg_partitioned_table";
  private final String snapshotUnpartitionedTableName = "iceberg_unpartitioned_table";
  private final String snapshotExternalDataFilesTableName = "iceberg_external_data_files_table";
  private final String snapshotNewTableLocationTableName = "iceberg_new_table_location_table";
  private final String snapshotAdditionalPropertiesTableName =
      "iceberg_additional_properties_table";
  private final String snapshotTypeTestTableName = "iceberg_type_test_table";
  private String partitionedLocation;
  private String unpartitionedLocation;
  private String newIcebergTableLocation;
  private String externalDataFilesTableLocation;
  private String typeTestTableLocation;

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
  @Rule public TemporaryFolder temp5 = new TemporaryFolder();

  public TestSnapshotDeltaLakeTable(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
    spark.conf().set("spark.sql.catalog." + defaultSparkCatalog, DeltaCatalog.class.getName());
  }

  @Before
  public void before() throws IOException {
    File partitionedFolder = temp1.newFolder();
    File unpartitionedFolder = temp2.newFolder();
    File newIcebergTableFolder = temp3.newFolder();
    File externalDataFilesTableFolder = temp4.newFolder();
    File typeTestTableFolder = temp5.newFolder();
    partitionedLocation = partitionedFolder.toURI().toString();
    unpartitionedLocation = unpartitionedFolder.toURI().toString();
    newIcebergTableLocation = newIcebergTableFolder.toURI().toString();
    externalDataFilesTableLocation = externalDataFilesTableFolder.toURI().toString();
    typeTestTableLocation = typeTestTableFolder.toURI().toString();

    spark.sql(String.format("CREATE DATABASE IF NOT EXISTS %s", NAMESPACE));

    partitionedIdentifier = destName(defaultSparkCatalog, partitionedTableName);
    unpartitionedIdentifier = destName(defaultSparkCatalog, unpartitionedTableName);
    externalDataFilesIdentifier = destName(defaultSparkCatalog, externalDataFilesTableName);
    typeTestIdentifier = destName(defaultSparkCatalog, typeTestTableName);

    spark.sql(String.format("DROP TABLE IF EXISTS %s", partitionedIdentifier));
    spark.sql(String.format("DROP TABLE IF EXISTS %s", unpartitionedIdentifier));
    spark.sql(String.format("DROP TABLE IF EXISTS %s", externalDataFilesIdentifier));
    spark.sql(String.format("DROP TABLE IF EXISTS %s", typeTestIdentifier));

    // generate the dataframe
    Dataset<Row> nestedDataFrame = nestedDataFrame();
    Dataset<Row> typeTestDataFrame = typeTestDataFrame();

    // write to delta tables
    writeDeltaTable(nestedDataFrame, partitionedIdentifier, partitionedLocation, "id");
    writeDeltaTable(nestedDataFrame, unpartitionedIdentifier, unpartitionedLocation, null);
    writeDeltaTable(
        nestedDataFrame, externalDataFilesIdentifier, externalDataFilesTableLocation, null);
    writeDeltaTable(typeTestDataFrame, typeTestIdentifier, typeTestTableLocation, "stringCol");

    // Delete a record from the table
    spark.sql("DELETE FROM " + partitionedIdentifier + " WHERE id=3");
    spark.sql("DELETE FROM " + unpartitionedIdentifier + " WHERE id=3");

    // Update a record
    spark.sql("UPDATE " + partitionedIdentifier + " SET id=3 WHERE id=1");
    spark.sql("UPDATE " + unpartitionedIdentifier + " SET id=3 WHERE id=1");
  }

  @After
  public void after() {
    // Drop delta lake tables.
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s", destName(defaultSparkCatalog, partitionedTableName)));
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s", destName(defaultSparkCatalog, unpartitionedTableName)));
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s", destName(defaultSparkCatalog, externalDataFilesTableName)));
    spark.sql(
        String.format("DROP TABLE IF EXISTS %s", destName(defaultSparkCatalog, typeTestTableName)));

    // Drop iceberg tables.
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s", destName(icebergCatalogName, snapshotPartitionedTableName)));
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s",
            destName(icebergCatalogName, snapshotUnpartitionedTableName)));
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s",
            destName(icebergCatalogName, snapshotExternalDataFilesTableName)));
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s",
            destName(icebergCatalogName, snapshotNewTableLocationTableName)));
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s",
            destName(icebergCatalogName, snapshotAdditionalPropertiesTableName)));
    spark.sql(
        String.format(
            "DROP TABLE IF EXISTS %s", destName(icebergCatalogName, snapshotTypeTestTableName)));

    spark.sql(String.format("DROP DATABASE IF EXISTS %s", NAMESPACE));
  }

  @Test
  public void testBasicSnapshotPartitioned() {
    String newTableIdentifier = destName(icebergCatalogName, snapshotPartitionedTableName);
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, partitionedLocation)
            .execute();

    checkSnapshotIntegrity(partitionedLocation, partitionedIdentifier, newTableIdentifier, result);
    checkIcebergTableLocation(newTableIdentifier, partitionedLocation);
  }

  @Test
  public void testBasicSnapshotUnpartitioned() {
    String newTableIdentifier = destName(icebergCatalogName, snapshotUnpartitionedTableName);
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, unpartitionedLocation)
            .execute();

    checkSnapshotIntegrity(
        unpartitionedLocation, unpartitionedIdentifier, newTableIdentifier, result);
    checkIcebergTableLocation(newTableIdentifier, unpartitionedLocation);
  }

  @Test
  public void testSnapshotWithNewLocation() {
    String newTableIdentifier = destName(icebergCatalogName, snapshotNewTableLocationTableName);
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, partitionedLocation)
            .tableLocation(newIcebergTableLocation)
            .execute();

    checkSnapshotIntegrity(partitionedLocation, partitionedIdentifier, newTableIdentifier, result);
    checkIcebergTableLocation(newTableIdentifier, newIcebergTableLocation);
  }

  @Test
  public void testSnapshotWithAdditionalProperties() {
    // add some properties to the original delta table
    spark.sql(
        "ALTER TABLE "
            + unpartitionedIdentifier
            + " SET TBLPROPERTIES ('foo'='bar', 'test0'='test0')");
    String newTableIdentifier = destName(icebergCatalogName, snapshotAdditionalPropertiesTableName);
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, unpartitionedLocation)
            .tableProperty("test1", "test1")
            .tableProperties(
                ImmutableMap.of(
                    "test2", "test2", "test3", "test3", "test4",
                    "test4")) // add additional iceberg table properties
            .execute();

    checkSnapshotIntegrity(
        unpartitionedLocation, unpartitionedIdentifier, newTableIdentifier, result);
    checkIcebergTableLocation(newTableIdentifier, unpartitionedLocation);
    checkIcebergTableProperties(
        newTableIdentifier,
        ImmutableMap.of(
            "foo", "bar", "test0", "test0", "test1", "test1", "test2", "test2", "test3", "test3",
            "test4", "test4"),
        unpartitionedLocation);
  }

  @Test
  public void testSnapshotTableWithExternalDataFiles() {
    // Add parquet files to default.external_data_files_table. The newly added parquet files
    // are not at the same location as the table.
    addExternalDatafiles(externalDataFilesTableLocation, unpartitionedLocation);

    String newTableIdentifier = destName(icebergCatalogName, snapshotExternalDataFilesTableName);
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, externalDataFilesTableLocation)
            .execute();
    checkSnapshotIntegrity(
        externalDataFilesTableLocation, externalDataFilesIdentifier, newTableIdentifier, result);
    checkIcebergTableLocation(newTableIdentifier, externalDataFilesTableLocation);
    checkDataFilePathsIntegrity(newTableIdentifier, externalDataFilesTableLocation);
  }

  @Test
  public void testSnapshotSupportedTypes() {
    String newTableIdentifier = destName(icebergCatalogName, snapshotTypeTestTableName);
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, typeTestTableLocation)
            .tableProperty(TableProperties.PARQUET_VECTORIZATION_ENABLED, "false")
            .execute();
    checkSnapshotIntegrity(typeTestTableLocation, typeTestIdentifier, newTableIdentifier, result);
    checkIcebergTableLocation(newTableIdentifier, typeTestTableLocation);
    checkIcebergTableProperties(
        newTableIdentifier,
        ImmutableMap.of(TableProperties.PARQUET_VECTORIZATION_ENABLED, "false"),
        typeTestTableLocation);
  }

  private void checkSnapshotIntegrity(
      String deltaTableLocation,
      String deltaTableIdentifier,
      String icebergTableIdentifier,
      SnapshotDeltaLakeTable.Result snapshotReport) {
    DeltaLog deltaLog = DeltaLog.forTable(spark.sessionState().newHadoopConf(), deltaTableLocation);

    List<Row> deltaTableContents =
        spark.sql("SELECT * FROM " + deltaTableIdentifier).collectAsList();
    List<Row> icebergTableContents =
        spark.sql("SELECT * FROM " + icebergTableIdentifier).collectAsList();

    Assertions.assertThat(deltaTableContents).hasSize(icebergTableContents.size());
    Assertions.assertThat(deltaLog.update().getAllFiles())
        .hasSize((int) snapshotReport.snapshotDataFilesCount());
    Assertions.assertThat(icebergTableContents).containsAll(deltaTableContents);
    Assertions.assertThat(deltaTableContents).containsAll(icebergTableContents);
  }

  private void checkIcebergTableLocation(String icebergTableIdentifier, String expectedLocation) {
    Table icebergTable = getIcebergTable(icebergTableIdentifier);
    Assertions.assertThat(icebergTable.location()).isEqualTo(expectedLocation);
  }

  private void checkIcebergTableProperties(
      String icebergTableIdentifier,
      Map<String, String> expectedAdditionalProperties,
      String deltaTableLocation) {
    Table icebergTable = getIcebergTable(icebergTableIdentifier);
    ImmutableMap.Builder<String, String> expectedPropertiesBuilder = ImmutableMap.builder();
    // The snapshot action will put some fixed properties to the table
    expectedPropertiesBuilder.put(SNAPSHOT_SOURCE_PROP, DELTA_SOURCE_VALUE);
    expectedPropertiesBuilder.putAll(expectedAdditionalProperties);
    ImmutableMap<String, String> expectedProperties = expectedPropertiesBuilder.build();

    Assertions.assertThat(icebergTable.properties().entrySet())
        .containsAll(expectedProperties.entrySet());
    Assertions.assertThat(icebergTable.properties())
        .containsEntry(ORIGINAL_LOCATION_PROP, deltaTableLocation);
  }

  private void checkDataFilePathsIntegrity(
      String icebergTableIdentifier, String deltaTableLocation) {
    Table icebergTable = getIcebergTable(icebergTableIdentifier);
    DeltaLog deltaLog = DeltaLog.forTable(spark.sessionState().newHadoopConf(), deltaTableLocation);
    // checkSnapshotIntegrity already checks the number of data files in the snapshot iceberg table
    // equals that in the original delta lake table
    List<String> deltaTableDataFilePaths =
        deltaLog.update().getAllFiles().stream()
            .map(f -> getFullFilePath(f.getPath(), deltaLog.getPath().toString()))
            .collect(Collectors.toList());
    icebergTable
        .currentSnapshot()
        .addedDataFiles(icebergTable.io())
        .forEach(
            dataFile -> {
              Assertions.assertThat(URI.create(dataFile.path().toString()).isAbsolute()).isTrue();
              Assertions.assertThat(deltaTableDataFilePaths).contains(dataFile.path().toString());
            });
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

  /**
   * Add parquet files manually to a delta lake table to mock the situation that some data files are
   * not in the same location as the delta lake table. The case that {@link AddFile#getPath()} or
   * {@link RemoveFile#getPath()} returns absolute path.
   *
   * <p>The known <a href="https://github.com/delta-io/connectors/issues/380">issue</a> makes it
   * necessary to manually rebuild the AddFile to avoid deserialization error when committing the
   * transaction.
   */
  private void addExternalDatafiles(
      String targetDeltaTableLocation, String sourceDeltaTableLocation) {
    DeltaLog targetLog =
        DeltaLog.forTable(spark.sessionState().newHadoopConf(), targetDeltaTableLocation);
    OptimisticTransaction transaction = targetLog.startTransaction();
    DeltaLog sourceLog =
        DeltaLog.forTable(spark.sessionState().newHadoopConf(), sourceDeltaTableLocation);
    List<AddFile> newFiles =
        sourceLog.update().getAllFiles().stream()
            .map(
                f ->
                    AddFile.builder(
                            getFullFilePath(f.getPath(), sourceLog.getPath().toString()),
                            f.getPartitionValues(),
                            f.getSize(),
                            System.currentTimeMillis(),
                            true)
                        .build())
            .collect(Collectors.toList());
    try {
      transaction.commit(newFiles, new Operation(Operation.Name.UPDATE), "Delta-Lake/2.2.0");
    } catch (DeltaConcurrentModificationException e) {
      throw new RuntimeException(e);
    }
  }

  private static String getFullFilePath(String path, String tableRoot) {
    URI dataFileUri = URI.create(path);
    try {
      String decodedPath = new URLCodec().decode(path);
      if (dataFileUri.isAbsolute()) {
        return decodedPath;
      } else {
        return tableRoot + File.separator + decodedPath;
      }
    } catch (DecoderException e) {
      throw new IllegalArgumentException(String.format("Cannot decode path %s", path), e);
    }
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
        .withColumn("stringCol", expr("CAST(timestampCol AS STRING)"))
        .withColumn("booleanCol", expr("longCol > 5"))
        .withColumn("binaryCol", expr("CAST(longCol AS BINARY)"))
        .withColumn("byteCol", expr("CAST(longCol AS BYTE)"))
        .withColumn("decimalCol", expr("CAST(longCol AS DECIMAL(10, 2))"))
        .withColumn("shortCol", expr("CAST(longCol AS SHORT)"))
        .withColumn("mapCol", expr("MAP(longCol, decimalCol)"))
        .withColumn("arrayCol", expr("ARRAY(longCol)"))
        .withColumn("structCol", expr("STRUCT(mapCol, arrayCol)"));
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

  private void writeDeltaTable(
      Dataset<Row> df, String identifier, String path, String partitionColumn) {
    if (partitionColumn != null) {
      df.write()
          .format("delta")
          .mode(SaveMode.Append)
          .option("path", path)
          .partitionBy(partitionColumn)
          .saveAsTable(identifier);
    } else {
      df.write().format("delta").mode(SaveMode.Append).option("path", path).saveAsTable(identifier);
    }
  }
}
