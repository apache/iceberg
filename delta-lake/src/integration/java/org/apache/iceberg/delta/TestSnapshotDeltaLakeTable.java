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
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.junit.After;
import org.junit.Assert;
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
  private static final String NAMESPACE = "default";
  private String partitionedIdentifier;
  private String unpartitionedIdentifier;
  private String externalDataFilesIdentifier;
  private static final String defaultSparkCatalog = "spark_catalog";
  private static final String icebergCatalogName = "iceberg_hive";

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

  private final String partitionedTableName = "partitioned_table";
  private final String unpartitionedTableName = "unpartitioned_table";
  private final String externalDataFilesTableName = "external_data_files_table";
  private String partitionedLocation;
  private String unpartitionedLocation;
  private String newIcebergTableLocation;
  private String externalDataFilesTableLocation;

  public TestSnapshotDeltaLakeTable(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
    spark.conf().set("spark.sql.catalog." + defaultSparkCatalog, DeltaCatalog.class.getName());
  }

  @Before
  public void before() {
    try {
      File partitionedFolder = temp1.newFolder();
      File unpartitionedFolder = temp2.newFolder();
      File newIcebergTableFolder = temp3.newFolder();
      File externalDataFilesTableFolder = temp4.newFolder();
      partitionedLocation = partitionedFolder.toURI().toString();
      unpartitionedLocation = unpartitionedFolder.toURI().toString();
      newIcebergTableLocation = newIcebergTableFolder.toURI().toString();
      externalDataFilesTableLocation = externalDataFilesTableFolder.toURI().toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    partitionedIdentifier = destName(defaultSparkCatalog, partitionedTableName);
    unpartitionedIdentifier = destName(defaultSparkCatalog, unpartitionedTableName);
    externalDataFilesIdentifier = destName(defaultSparkCatalog, externalDataFilesTableName);

    spark.sql(String.format("DROP TABLE IF EXISTS %s", partitionedIdentifier));
    spark.sql(String.format("DROP TABLE IF EXISTS %s", unpartitionedIdentifier));
    spark.sql(String.format("DROP TABLE IF EXISTS %s", externalDataFilesIdentifier));

    // Create a partitioned and unpartitioned table, doing a few inserts on each
    IntStream.range(0, 3)
        .forEach(
            i -> {
              List<SimpleRecord> record =
                  Lists.newArrayList(new SimpleRecord(i, UUID.randomUUID().toString()));

              Dataset<Row> df = spark.createDataFrame(record, SimpleRecord.class);

              df.write()
                  .format("delta")
                  .mode(i == 0 ? SaveMode.Overwrite : SaveMode.Append)
                  .partitionBy("id")
                  .option("path", partitionedLocation)
                  .saveAsTable(partitionedIdentifier);

              df.write()
                  .format("delta")
                  .mode(i == 0 ? SaveMode.Overwrite : SaveMode.Append)
                  .option("path", unpartitionedLocation)
                  .saveAsTable(unpartitionedIdentifier);

              df.write()
                  .format("delta")
                  .mode(i == 0 ? SaveMode.Overwrite : SaveMode.Append)
                  .option("path", externalDataFilesTableLocation)
                  .saveAsTable(externalDataFilesIdentifier);
            });

    // Delete a record from the table
    spark.sql("DELETE FROM " + partitionedIdentifier + " WHERE id=0");
    spark.sql("DELETE FROM " + unpartitionedIdentifier + " WHERE id=0");

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
  }

  @Test
  public void testBasicSnapshotPartitioned() {
    String newTableIdentifier = destName(icebergCatalogName, "iceberg_table");
    SnapshotDeltaLakeTable.Result result =
        SnapshotDeltaLakeSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, partitionedLocation)
            .execute();

    checkSnapshotIntegrity(partitionedLocation, partitionedIdentifier, newTableIdentifier, result);
    checkIcebergTableLocation(newTableIdentifier, partitionedLocation);
  }

  @Test
  public void testBasicSnapshotUnpartitioned() {
    String newTableIdentifier = destName(icebergCatalogName, "iceberg_table_unpartitioned");
    SnapshotDeltaLakeTable.Result result =
        SnapshotDeltaLakeSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, unpartitionedLocation)
            .execute();

    checkSnapshotIntegrity(
        unpartitionedLocation, unpartitionedIdentifier, newTableIdentifier, result);
    checkIcebergTableLocation(newTableIdentifier, unpartitionedLocation);
  }

  @Test
  public void testSnapshotWithNewLocation() {
    String newTableIdentifier = destName(icebergCatalogName, "iceberg_table_new_location");
    SnapshotDeltaLakeTable.Result result =
        SnapshotDeltaLakeSparkIntegration.snapshotDeltaLakeTable(
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
    String newTableIdentifier = destName(icebergCatalogName, "iceberg_table_additional_properties");
    SnapshotDeltaLakeTable.Result result =
        SnapshotDeltaLakeSparkIntegration.snapshotDeltaLakeTable(
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

    String newTableIdentifier = destName(icebergCatalogName, "iceberg_table_external_data_files");
    SnapshotDeltaLakeTable.Result result =
        SnapshotDeltaLakeSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, externalDataFilesTableLocation)
            .execute();
    checkSnapshotIntegrity(
        externalDataFilesTableLocation, externalDataFilesIdentifier, newTableIdentifier, result);
    checkIcebergTableLocation(newTableIdentifier, externalDataFilesTableLocation);
    checkDataFilePathsIntegrity(newTableIdentifier, externalDataFilesTableLocation);
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

    Assert.assertEquals(
        "The original table and the transformed one should have the same size",
        deltaTableContents.size(),
        icebergTableContents.size());
    Assert.assertTrue(
        "The original table and the transformed one should have the same contents",
        icebergTableContents.containsAll(deltaTableContents));
    Assert.assertTrue(
        "The original table and the transformed one should have the same contents",
        deltaTableContents.containsAll(icebergTableContents));
    Assert.assertEquals(
        "The number of files in the delta table should be the same as the number of files in the snapshot iceberg table",
        deltaLog.update().getAllFiles().size(),
        snapshotReport.snapshotDataFilesCount());
  }

  private void checkIcebergTableLocation(String icebergTableIdentifier, String expectedLocation) {
    Table icebergTable = getIcebergTable(icebergTableIdentifier);
    Assert.assertEquals(
        "The iceberg table should have the expected location",
        expectedLocation,
        icebergTable.location());
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
    Assert.assertTrue(
        "The snapshot iceberg table should have the expected properties, all in original delta lake table, added by the action and user added ones",
        icebergTable.properties().entrySet().containsAll(expectedProperties.entrySet()));
    Assert.assertTrue(
        "The snapshot iceberg table's property should contains the original location",
        icebergTable.properties().containsKey(ORIGINAL_LOCATION_PROP)
            && icebergTable.properties().get(ORIGINAL_LOCATION_PROP).equals(deltaTableLocation));
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
              Assert.assertTrue(
                  "The data file path should be the same as the original delta table",
                  deltaTableDataFilePaths.contains(dataFile.path().toString()));
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
   * {@link RemoveFile#getPath()} returns absolute path
   *
   * <p>The knowing <a href="https://github.com/delta-io/connectors/issues/380">issue</a> make it
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
      // handle exception here
      throw new RuntimeException(e);
    }
  }

  private static String getFullFilePath(String path, String tableRoot) {
    URI dataFileUri = URI.create(path);
    if (dataFileUri.isAbsolute()) {
      return path;
    } else {
      return tableRoot + File.separator + path;
    }
  }
}
