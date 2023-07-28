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
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;
import io.delta.standalone.exceptions.DeltaConcurrentModificationException;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.util.LocationUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestSnapshotDeltaLakeTable extends SparkDeltaLakeSnapshotTestBase {
  private static final String SNAPSHOT_SOURCE_PROP = "snapshot_source";
  private static final String DELTA_SOURCE_VALUE = "delta";
  private static final String ORIGINAL_LOCATION_PROP = "original_location";
  private static final String NAMESPACE = "delta_conversion_test";
  private static final String defaultSparkCatalog = "spark_catalog";
  private static final String icebergCatalogName = "iceberg_hive";
  private static Dataset<Row> typeTestDataFrame;
  private static Dataset<Row> nestedDataFrame;

  static Stream<Arguments> parameters() {
    return Stream.of(
        Arguments.of(
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
                )));
  }

  @TempDir private Path temp;

  public TestSnapshotDeltaLakeTable(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
    spark.conf().set("spark.sql.catalog." + defaultSparkCatalog, DeltaCatalog.class.getName());
  }

  @BeforeAll
  public static void beforeClass() {
    spark.sql(String.format("CREATE DATABASE IF NOT EXISTS %s", NAMESPACE));

    typeTestDataFrame =
        spark
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
    nestedDataFrame =
        spark
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
            .withColumn("arrayStructCol", expr("ARRAY(innerStruct1, innerStruct1, innerStruct1)"))
            .withColumn("mapCol1", expr("MAP(structCol1, structCol2)"))
            .withColumn("mapCol2", expr("MAP(longCol, dateString)"))
            .withColumn("mapCol3", expr("MAP(dateCol, arrayCol)"))
            .withColumn("structCol3", expr("STRUCT(structCol2, mapCol3, arrayCol)"));
  }

  @AfterAll
  public static void afterClass() {
    spark.sql(String.format("DROP DATABASE IF EXISTS %s CASCADE", NAMESPACE));
  }

  @ParameterizedTest(name = "Catalog Name {0} - Options {2}")
  @MethodSource("parameters")
  public void testBasicSnapshotPartitioned() {
    String partitionedIdentifier = destName(defaultSparkCatalog, "partitioned_table");
    String partitionedLocation = temp.toFile().toURI().toString();

    writeDeltaTable(nestedDataFrame, partitionedIdentifier, partitionedLocation, "id");
    spark.sql("DELETE FROM " + partitionedIdentifier + " WHERE id=3");
    spark.sql("UPDATE " + partitionedIdentifier + " SET id=3 WHERE id=1");

    String newTableIdentifier = destName(icebergCatalogName, "iceberg_partitioned_table");
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, partitionedLocation)
            .execute();

    checkSnapshotIntegrity(
        partitionedLocation, partitionedIdentifier, newTableIdentifier, result, 0);
    checkTagContentAndOrder(partitionedLocation, newTableIdentifier, 0);
    checkIcebergTableLocation(newTableIdentifier, partitionedLocation);
  }

  @ParameterizedTest(name = "Catalog Name {0} - Options {2}")
  @MethodSource("parameters")
  public void testBasicSnapshotUnpartitioned() {
    String unpartitionedIdentifier = destName(defaultSparkCatalog, "unpartitioned_table");
    String unpartitionedLocation = temp.toFile().toURI().toString();

    writeDeltaTable(nestedDataFrame, unpartitionedIdentifier, unpartitionedLocation, null);
    spark.sql("DELETE FROM " + unpartitionedIdentifier + " WHERE id=3");
    spark.sql("UPDATE " + unpartitionedIdentifier + " SET id=3 WHERE id=1");

    String newTableIdentifier = destName(icebergCatalogName, "iceberg_unpartitioned_table");
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, unpartitionedLocation)
            .execute();

    checkSnapshotIntegrity(
        unpartitionedLocation, unpartitionedIdentifier, newTableIdentifier, result, 0);
    checkTagContentAndOrder(unpartitionedLocation, newTableIdentifier, 0);
    checkIcebergTableLocation(newTableIdentifier, unpartitionedLocation);
  }

  @ParameterizedTest(name = "Catalog Name {0} - Options {2}")
  @MethodSource("parameters")
  public void testSnapshotWithNewLocation() {
    String partitionedIdentifier = destName(defaultSparkCatalog, "partitioned_table");
    String partitionedLocation = temp.toFile().toURI().toString();
    String newIcebergTableLocation = temp.toFile().toURI().toString();

    writeDeltaTable(nestedDataFrame, partitionedIdentifier, partitionedLocation, "id");
    spark.sql("DELETE FROM " + partitionedIdentifier + " WHERE id=3");
    spark.sql("UPDATE " + partitionedIdentifier + " SET id=3 WHERE id=1");

    String newTableIdentifier = destName(icebergCatalogName, "iceberg_new_table_location_table");
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, partitionedLocation)
            .tableLocation(newIcebergTableLocation)
            .execute();

    checkSnapshotIntegrity(
        partitionedLocation, partitionedIdentifier, newTableIdentifier, result, 0);
    checkTagContentAndOrder(partitionedLocation, newTableIdentifier, 0);
    checkIcebergTableLocation(newTableIdentifier, newIcebergTableLocation);
  }

  @ParameterizedTest(name = "Catalog Name {0} - Options {2}")
  @MethodSource("parameters")
  public void testSnapshotWithAdditionalProperties() {
    String unpartitionedIdentifier = destName(defaultSparkCatalog, "unpartitioned_table");
    String unpartitionedLocation = temp.toFile().toURI().toString();

    writeDeltaTable(nestedDataFrame, unpartitionedIdentifier, unpartitionedLocation, null);
    spark.sql("DELETE FROM " + unpartitionedIdentifier + " WHERE id=3");
    spark.sql("UPDATE " + unpartitionedIdentifier + " SET id=3 WHERE id=1");

    // add some properties to the original delta table
    spark.sql(
        "ALTER TABLE "
            + unpartitionedIdentifier
            + " SET TBLPROPERTIES ('foo'='bar', 'test0'='test0')");

    String newTableIdentifier = destName(icebergCatalogName, "iceberg_additional_properties_table");
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
        unpartitionedLocation, unpartitionedIdentifier, newTableIdentifier, result, 0);
    checkTagContentAndOrder(unpartitionedLocation, newTableIdentifier, 0);
    checkIcebergTableLocation(newTableIdentifier, unpartitionedLocation);
    checkIcebergTableProperties(
        newTableIdentifier,
        ImmutableMap.of(
            "foo", "bar", "test0", "test0", "test1", "test1", "test2", "test2", "test3", "test3",
            "test4", "test4"),
        unpartitionedLocation);
  }

  @ParameterizedTest(name = "Catalog Name {0} - Options {2}")
  @MethodSource("parameters")
  public void testSnapshotTableWithExternalDataFiles() {
    String unpartitionedIdentifier = destName(defaultSparkCatalog, "unpartitioned_table");
    String externalDataFilesIdentifier = destName(defaultSparkCatalog, "external_data_files_table");
    String unpartitionedLocation = temp.toFile().toURI().toString();
    String externalDataFilesTableLocation = temp.toFile().toURI().toString();

    writeDeltaTable(nestedDataFrame, unpartitionedIdentifier, unpartitionedLocation, null);
    spark.sql("DELETE FROM " + unpartitionedIdentifier + " WHERE id=3");
    spark.sql("UPDATE " + unpartitionedIdentifier + " SET id=3 WHERE id=1");

    writeDeltaTable(
        nestedDataFrame, externalDataFilesIdentifier, externalDataFilesTableLocation, null);
    // Add parquet files to default.external_data_files_table. The newly added parquet files
    // are not at the same location as the table.
    addExternalDatafiles(externalDataFilesTableLocation, unpartitionedLocation);

    String newTableIdentifier = destName(icebergCatalogName, "iceberg_external_data_files_table");
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, externalDataFilesTableLocation)
            .execute();
    checkSnapshotIntegrity(
        externalDataFilesTableLocation, externalDataFilesIdentifier, newTableIdentifier, result, 0);
    checkTagContentAndOrder(externalDataFilesTableLocation, newTableIdentifier, 0);
    checkIcebergTableLocation(newTableIdentifier, externalDataFilesTableLocation);
    checkDataFilePathsIntegrity(newTableIdentifier, externalDataFilesTableLocation);
  }

  @ParameterizedTest(name = "Catalog Name {0} - Options {2}")
  @MethodSource("parameters")
  public void testSnapshotSupportedTypes() {
    String typeTestIdentifier = destName(defaultSparkCatalog, "type_test_table");
    String typeTestTableLocation = temp.toFile().toURI().toString();

    writeDeltaTable(typeTestDataFrame, typeTestIdentifier, typeTestTableLocation, "stringCol");
    String newTableIdentifier = destName(icebergCatalogName, "iceberg_type_test_table");
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, typeTestTableLocation)
            .execute();
    checkSnapshotIntegrity(
        typeTestTableLocation, typeTestIdentifier, newTableIdentifier, result, 0);
    checkTagContentAndOrder(typeTestTableLocation, newTableIdentifier, 0);
    checkIcebergTableLocation(newTableIdentifier, typeTestTableLocation);
    checkIcebergTableProperties(newTableIdentifier, ImmutableMap.of(), typeTestTableLocation);
  }

  @ParameterizedTest(name = "Catalog Name {0} - Options {2}")
  @MethodSource("parameters")
  public void testSnapshotVacuumTable() throws IOException {
    String vacuumTestIdentifier = destName(defaultSparkCatalog, "vacuum_test_table");
    String vacuumTestTableLocation = temp.toFile().toURI().toString();

    writeDeltaTable(nestedDataFrame, vacuumTestIdentifier, vacuumTestTableLocation, null);
    Random random = new Random();
    for (int i = 0; i < 13; i++) {
      spark.sql(
          "UPDATE "
              + vacuumTestIdentifier
              + " SET magic_number = "
              + random.nextDouble()
              + " WHERE id = 1");
    }

    boolean deleteResult =
        Files.deleteIfExists(
            Paths.get(
                URI.create(
                    vacuumTestTableLocation.concat("/_delta_log/00000000000000000000.json"))));
    Assertions.assertThat(deleteResult).isTrue();
    spark.sql("VACUUM " + vacuumTestIdentifier + " RETAIN 0 HOURS");

    String newTableIdentifier = destName(icebergCatalogName, "iceberg_vacuum_table");
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, vacuumTestTableLocation)
            .execute();
    checkSnapshotIntegrity(
        vacuumTestTableLocation, vacuumTestIdentifier, newTableIdentifier, result, 13);
    checkTagContentAndOrder(vacuumTestTableLocation, newTableIdentifier, 13);
    checkIcebergTableLocation(newTableIdentifier, vacuumTestTableLocation);
  }

  @ParameterizedTest(name = "Catalog Name {0} - Options {2}")
  @MethodSource("parameters")
  public void testSnapshotLogCleanTable() throws IOException {
    String logCleanTestIdentifier = destName(defaultSparkCatalog, "log_clean_test_table");
    String logCleanTestTableLocation = temp.toFile().toURI().toString();

    writeDeltaTable(nestedDataFrame, logCleanTestIdentifier, logCleanTestTableLocation, "id");
    Random random = new Random();
    for (int i = 0; i < 25; i++) {
      spark.sql(
          "UPDATE "
              + logCleanTestIdentifier
              + " SET magic_number = "
              + random.nextDouble()
              + " WHERE id = 1");
    }

    boolean deleteResult =
        Files.deleteIfExists(
            Paths.get(
                URI.create(
                    logCleanTestTableLocation.concat("/_delta_log/00000000000000000000.json"))));
    Assertions.assertThat(deleteResult).isTrue();

    String newTableIdentifier = destName(icebergCatalogName, "iceberg_log_clean_table");
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, logCleanTestTableLocation)
            .execute();
    checkSnapshotIntegrity(
        logCleanTestTableLocation, logCleanTestIdentifier, newTableIdentifier, result, 10);
    checkTagContentAndOrder(logCleanTestTableLocation, newTableIdentifier, 10);
    checkIcebergTableLocation(newTableIdentifier, logCleanTestTableLocation);
  }

  private void checkSnapshotIntegrity(
      String deltaTableLocation,
      String deltaTableIdentifier,
      String icebergTableIdentifier,
      SnapshotDeltaLakeTable.Result snapshotReport,
      long firstConstructableVersion) {
    DeltaLog deltaLog = DeltaLog.forTable(spark.sessionState().newHadoopConf(), deltaTableLocation);

    List<Row> deltaTableContents =
        spark.sql("SELECT * FROM " + deltaTableIdentifier).collectAsList();
    List<Row> icebergTableContents =
        spark.sql("SELECT * FROM " + icebergTableIdentifier).collectAsList();

    Assertions.assertThat(deltaTableContents).hasSize(icebergTableContents.size());
    Assertions.assertThat(snapshotReport.snapshotDataFilesCount())
        .isEqualTo(countDataFilesInDeltaLakeTable(deltaLog, firstConstructableVersion));
    Assertions.assertThat(icebergTableContents)
        .containsExactlyInAnyOrderElementsOf(deltaTableContents);
  }

  private void checkTagContentAndOrder(
      String deltaTableLocation, String icebergTableIdentifier, long firstConstructableVersion) {
    DeltaLog deltaLog = DeltaLog.forTable(spark.sessionState().newHadoopConf(), deltaTableLocation);
    long currentVersion = deltaLog.snapshot().getVersion();
    Table icebergTable = getIcebergTable(icebergTableIdentifier);
    Map<String, SnapshotRef> icebergSnapshotRefs = icebergTable.refs();
    List<Snapshot> icebergSnapshots = Lists.newArrayList(icebergTable.snapshots());

    Assertions.assertThat(icebergSnapshots.size())
        .isEqualTo(currentVersion - firstConstructableVersion + 1);

    for (int i = 0; i < icebergSnapshots.size(); i++) {
      long deltaVersion = firstConstructableVersion + i;
      Snapshot currentIcebergSnapshot = icebergSnapshots.get(i);

      String expectedVersionTag = "delta-version-" + deltaVersion;
      icebergSnapshotRefs.get(expectedVersionTag);
      Assertions.assertThat(icebergSnapshotRefs.get(expectedVersionTag)).isNotNull();
      Assertions.assertThat(icebergSnapshotRefs.get(expectedVersionTag).isTag()).isTrue();
      Assertions.assertThat(icebergSnapshotRefs.get(expectedVersionTag).snapshotId())
          .isEqualTo(currentIcebergSnapshot.snapshotId());

      Timestamp deltaVersionTimestamp = deltaLog.getCommitInfoAt(deltaVersion).getTimestamp();
      Assertions.assertThat(deltaVersionTimestamp).isNotNull();
      String expectedTimestampTag = "delta-ts-" + deltaVersionTimestamp.getTime();

      Assertions.assertThat(icebergSnapshotRefs.get(expectedTimestampTag)).isNotNull();
      Assertions.assertThat(icebergSnapshotRefs.get(expectedTimestampTag).isTag()).isTrue();
      Assertions.assertThat(icebergSnapshotRefs.get(expectedTimestampTag).snapshotId())
          .isEqualTo(currentIcebergSnapshot.snapshotId());
    }
  }

  private void checkIcebergTableLocation(String icebergTableIdentifier, String expectedLocation) {
    Table icebergTable = getIcebergTable(icebergTableIdentifier);
    Assertions.assertThat(icebergTable.location())
        .isEqualTo(LocationUtil.stripTrailingSlash(expectedLocation));
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

  private void writeDeltaTable(
      Dataset<Row> df, String identifier, String path, String partitionColumn) {
    spark.sql(String.format("DROP TABLE IF EXISTS %s", identifier));
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

  private long countDataFilesInDeltaLakeTable(DeltaLog deltaLog, long firstConstructableVersion) {
    long dataFilesCount = 0;

    List<AddFile> initialDataFiles =
        deltaLog.getSnapshotForVersionAsOf(firstConstructableVersion).getAllFiles();
    dataFilesCount += initialDataFiles.size();

    Iterator<VersionLog> versionLogIterator =
        deltaLog.getChanges(
            firstConstructableVersion + 1, false // not throw exception when data loss detected
            );

    while (versionLogIterator.hasNext()) {
      VersionLog versionLog = versionLogIterator.next();
      List<Action> addFiles =
          versionLog.getActions().stream()
              .filter(action -> action instanceof AddFile)
              .collect(Collectors.toList());
      dataFilesCount += addFiles.size();
    }

    return dataFilesCount;
  }
}
