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

import static io.delta.kernel.internal.util.Utils.toCloseableIterator;
import static io.delta.kernel.utils.CloseableIterable.inMemoryIterable;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.expr;
import static org.assertj.core.api.Assertions.assertThat;

import io.delta.kernel.Operation;
import io.delta.kernel.Scan;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.TableImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.utils.CloseableIterator;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.util.LocationUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSnapshotDeltaLakeTable extends SparkDeltaLakeSnapshotTestBase {
  private static final String SNAPSHOT_SOURCE_PROP = "snapshot_source";
  private static final String DELTA_SOURCE_VALUE = "delta";
  private static final String ORIGINAL_LOCATION_PROP = "original_location";
  private static final String NAMESPACE = "delta_conversion_test";
  private static final String DEFAULT_SPARK_CATALOG = "spark_catalog";
  private static final String ICEBERG_CATALOG_NAME = "iceberg_hive";
  private static final Map<String, String> CONFIG =
      ImmutableMap.of(
          "type", "hive",
          "default-namespace", "default",
          "parquet-enabled", "true",
          "cache-enabled",
              "false" // Spark will delete tables using v1, leaving the cache out of sync
          );
  private static Dataset<org.apache.spark.sql.Row> typeTestDataFrame;
  private static Dataset<org.apache.spark.sql.Row> nestedDataFrame;

  @TempDir private File tempA;
  @TempDir private File tempB;

  public TestSnapshotDeltaLakeTable() {
    super(ICEBERG_CATALOG_NAME, SparkCatalog.class.getName(), CONFIG);
    spark.conf().set("spark.sql.catalog." + DEFAULT_SPARK_CATALOG, DeltaCatalog.class.getName());
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
            .withColumn("timestampStrCol", expr("CAST(timestampCol AS STRING)"))
            .withColumn("stringCol", date_format(col("timestampCol"), "yyyy/M/d"))
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

  @Test
  public void testBasicSnapshotPartitioned() {
    String partitionedIdentifier = destName(DEFAULT_SPARK_CATALOG, "partitioned_table");
    String partitionedLocation = tempA.toURI().toString();

    writeDeltaTable(nestedDataFrame, partitionedIdentifier, partitionedLocation, "id");
    spark.sql("DELETE FROM " + partitionedIdentifier + " WHERE id=3");
    spark.sql("UPDATE " + partitionedIdentifier + " SET id=3 WHERE id=1");

    String newTableIdentifier = destName(ICEBERG_CATALOG_NAME, "iceberg_partitioned_table");
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, partitionedLocation)
            .execute();

    checkSnapshotIntegrity(
        partitionedLocation, partitionedIdentifier, newTableIdentifier, result, 0);
    checkTagContentAndOrder(partitionedLocation, newTableIdentifier, 0);
    checkIcebergTableLocation(newTableIdentifier, partitionedLocation);
  }

  @Test
  public void testBasicSnapshotUnpartitioned() {
    String unpartitionedIdentifier = destName(DEFAULT_SPARK_CATALOG, "unpartitioned_table");
    String unpartitionedLocation = tempA.toURI().toString();

    writeDeltaTable(nestedDataFrame, unpartitionedIdentifier, unpartitionedLocation);
    spark.sql("DELETE FROM " + unpartitionedIdentifier + " WHERE id=3");
    spark.sql("UPDATE " + unpartitionedIdentifier + " SET id=3 WHERE id=1");

    String newTableIdentifier = destName(ICEBERG_CATALOG_NAME, "iceberg_unpartitioned_table");
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, unpartitionedLocation)
            .execute();

    checkSnapshotIntegrity(
        unpartitionedLocation, unpartitionedIdentifier, newTableIdentifier, result, 0);
    checkTagContentAndOrder(unpartitionedLocation, newTableIdentifier, 0);
    checkIcebergTableLocation(newTableIdentifier, unpartitionedLocation);
  }

  @Test
  public void testSnapshotWithNewLocation() {
    String partitionedIdentifier = destName(DEFAULT_SPARK_CATALOG, "partitioned_table");
    String partitionedLocation = tempA.toURI().toString();
    String newIcebergTableLocation = tempB.toURI().toString();

    writeDeltaTable(nestedDataFrame, partitionedIdentifier, partitionedLocation, "id");
    spark.sql("DELETE FROM " + partitionedIdentifier + " WHERE id=3");
    spark.sql("UPDATE " + partitionedIdentifier + " SET id=3 WHERE id=1");

    String newTableIdentifier = destName(ICEBERG_CATALOG_NAME, "iceberg_new_table_location_table");
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

  @Test
  public void testSnapshotWithAdditionalProperties() {
    String unpartitionedIdentifier = destName(DEFAULT_SPARK_CATALOG, "unpartitioned_table");
    String unpartitionedLocation = tempA.toURI().toString();

    writeDeltaTable(nestedDataFrame, unpartitionedIdentifier, unpartitionedLocation);
    spark.sql("DELETE FROM " + unpartitionedIdentifier + " WHERE id=3");
    spark.sql("UPDATE " + unpartitionedIdentifier + " SET id=3 WHERE id=1");

    // add some properties to the original delta table
    spark.sql(
        "ALTER TABLE "
            + unpartitionedIdentifier
            + " SET TBLPROPERTIES ('foo'='bar', 'test0'='test0')");

    String newTableIdentifier =
        destName(ICEBERG_CATALOG_NAME, "iceberg_additional_properties_table");
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

  @Test
  public void testSnapshotTableWithExternalDataFiles() {
    String unpartitionedIdentifier = destName(DEFAULT_SPARK_CATALOG, "unpartitioned_table");
    String externalDataFilesIdentifier =
        destName(DEFAULT_SPARK_CATALOG, "external_data_files_table");
    String unpartitionedLocation = tempA.toURI().toString();
    String externalDataFilesTableLocation = tempB.toURI().toString();

    writeDeltaTable(nestedDataFrame, unpartitionedIdentifier, unpartitionedLocation);
    spark.sql("DELETE FROM " + unpartitionedIdentifier + " WHERE id=3");
    spark.sql("UPDATE " + unpartitionedIdentifier + " SET id=3 WHERE id=1");

    writeDeltaTable(nestedDataFrame, externalDataFilesIdentifier, externalDataFilesTableLocation);
    // Add parquet files to default.external_data_files_table. The newly added parquet files
    // are not at the same location as the table.
    addExternalDatafiles(externalDataFilesTableLocation, unpartitionedLocation);

    String newTableIdentifier = destName(ICEBERG_CATALOG_NAME, "iceberg_external_data_files_table");
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

  @Test
  public void testSnapshotSupportedTypes() {
    String typeTestIdentifier = destName(DEFAULT_SPARK_CATALOG, "type_test_table");
    String typeTestTableLocation = tempA.toURI().toString();

    writeDeltaTable(
        typeTestDataFrame,
        typeTestIdentifier,
        typeTestTableLocation,
        "stringCol",
        "timestampStrCol",
        "booleanCol",
        "longCol");
    String newTableIdentifier = destName(ICEBERG_CATALOG_NAME, "iceberg_type_test_table");
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

  @Test
  public void testSnapshotVacuumTable() throws IOException {
    String vacuumTestIdentifier = destName(DEFAULT_SPARK_CATALOG, "vacuum_test_table");
    String vacuumTestTableLocation = tempA.toURI().toString();

    writeDeltaTable(nestedDataFrame, vacuumTestIdentifier, vacuumTestTableLocation);
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
    assertThat(deleteResult).isTrue();
    spark.sql("VACUUM " + vacuumTestIdentifier + " RETAIN 0 HOURS");

    String newTableIdentifier = destName(ICEBERG_CATALOG_NAME, "iceberg_vacuum_table");
    SnapshotDeltaLakeTable.Result result =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeTable(
                spark, newTableIdentifier, vacuumTestTableLocation)
            .execute();
    checkSnapshotIntegrity(
        vacuumTestTableLocation, vacuumTestIdentifier, newTableIdentifier, result, 13);
    checkTagContentAndOrder(vacuumTestTableLocation, newTableIdentifier, 13);
    checkIcebergTableLocation(newTableIdentifier, vacuumTestTableLocation);
  }

  @Test
  public void testSnapshotLogCleanTable() throws IOException {
    String logCleanTestIdentifier = destName(DEFAULT_SPARK_CATALOG, "log_clean_test_table");
    String logCleanTestTableLocation = tempA.toURI().toString();

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
    assertThat(deleteResult).isTrue();

    String newTableIdentifier = destName(ICEBERG_CATALOG_NAME, "iceberg_log_clean_table");
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
    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Engine engine = DefaultEngine.create(hadoopConf);
    Table deltaTable = Table.forPath(engine, deltaTableLocation);

    List<org.apache.spark.sql.Row> deltaTableContents =
        spark.sql("SELECT * FROM " + deltaTableIdentifier).collectAsList();
    List<org.apache.spark.sql.Row> icebergTableContents =
        spark.sql("SELECT * FROM " + icebergTableIdentifier).collectAsList();

    assertThat(deltaTableContents).hasSize(icebergTableContents.size());
    assertThat(snapshotReport.snapshotDataFilesCount())
        .isEqualTo(countDataFilesInDeltaLakeTable(engine, deltaTable, firstConstructableVersion));
    assertThat(icebergTableContents).containsExactlyInAnyOrderElementsOf(deltaTableContents);
  }

  private void checkTagContentAndOrder(
      String deltaTableLocation, String icebergTableIdentifier, long firstConstructableVersion) {
    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Engine engine = DefaultEngine.create(hadoopConf);
    Table deltaTable = Table.forPath(engine, deltaTableLocation);
    long currentVersion = deltaTable.getLatestSnapshot(engine).getVersion();
    org.apache.iceberg.Table icebergTable = getIcebergTable(icebergTableIdentifier);
    Map<String, SnapshotRef> icebergSnapshotRefs = icebergTable.refs();
    List<Snapshot> icebergSnapshots = Lists.newArrayList(icebergTable.snapshots());

    assertThat(icebergSnapshots).hasSize((int) (currentVersion - firstConstructableVersion + 1));

    for (int i = 0; i < icebergSnapshots.size(); i++) {
      long deltaVersion = firstConstructableVersion + i;
      Snapshot currentIcebergSnapshot = icebergSnapshots.get(i);

      String expectedVersionTag = "delta-version-" + deltaVersion;
      icebergSnapshotRefs.get(expectedVersionTag);
      assertThat(icebergSnapshotRefs.get(expectedVersionTag)).isNotNull();
      assertThat(icebergSnapshotRefs.get(expectedVersionTag).isTag()).isTrue();
      assertThat(icebergSnapshotRefs.get(expectedVersionTag).snapshotId())
          .isEqualTo(currentIcebergSnapshot.snapshotId());

      io.delta.kernel.Snapshot deltaSnapshot =
          deltaTable.getSnapshotAsOfVersion(engine, deltaVersion);
      long deltaVersionTimestamp = deltaSnapshot.getTimestamp(engine);
      assertThat(deltaVersionTimestamp).isGreaterThan(0);
      String expectedTimestampTag = "delta-ts-" + deltaVersionTimestamp;

      assertThat(icebergSnapshotRefs.get(expectedTimestampTag)).isNotNull();
      assertThat(icebergSnapshotRefs.get(expectedTimestampTag).isTag()).isTrue();
      assertThat(icebergSnapshotRefs.get(expectedTimestampTag).snapshotId())
          .isEqualTo(currentIcebergSnapshot.snapshotId());
    }
  }

  private void checkIcebergTableLocation(String icebergTableIdentifier, String expectedLocation) {
    org.apache.iceberg.Table icebergTable = getIcebergTable(icebergTableIdentifier);
    assertThat(icebergTable.location())
        .isEqualTo(LocationUtil.stripTrailingSlash(expectedLocation));
  }

  private void checkIcebergTableProperties(
      String icebergTableIdentifier,
      Map<String, String> expectedAdditionalProperties,
      String deltaTableLocation) {
    org.apache.iceberg.Table icebergTable = getIcebergTable(icebergTableIdentifier);
    ImmutableMap.Builder<String, String> expectedPropertiesBuilder = ImmutableMap.builder();
    // The snapshot action will put some fixed properties to the table
    expectedPropertiesBuilder.put(SNAPSHOT_SOURCE_PROP, DELTA_SOURCE_VALUE);
    expectedPropertiesBuilder.putAll(expectedAdditionalProperties);
    ImmutableMap<String, String> expectedProperties = expectedPropertiesBuilder.build();

    assertThat(icebergTable.properties().entrySet()).containsAll(expectedProperties.entrySet());
    assertThat(icebergTable.properties()).containsEntry(ORIGINAL_LOCATION_PROP, deltaTableLocation);
  }

  private void checkDataFilePathsIntegrity(
      String icebergTableIdentifier, String deltaTableLocation) {
    org.apache.iceberg.Table icebergTable = getIcebergTable(icebergTableIdentifier);
    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Engine engine = DefaultEngine.create(hadoopConf);
    Table deltaTable = Table.forPath(engine, deltaTableLocation);
    io.delta.kernel.Snapshot deltaSnapshot = deltaTable.getLatestSnapshot(engine);

    // checkSnapshotIntegrity already checks the number of data files in the snapshot iceberg table
    // equals that in the original delta lake table
    List<String> deltaTableDataFilePaths = Lists.newArrayList();
    Scan scan = deltaSnapshot.getScanBuilder().build();
    CloseableIterator<FilteredColumnarBatch> scanFileIter = scan.getScanFiles(engine);
    while (scanFileIter.hasNext()) {
      FilteredColumnarBatch batch = scanFileIter.next();
      CloseableIterator<Row> rows = batch.getRows();
      while (rows.hasNext()) {
        Row scanFileRow = rows.next();
        if (!scanFileRow.isNullAt(scanFileRow.getSchema().indexOf("add"))) {
          Row addFileRow = scanFileRow.getStruct(scanFileRow.getSchema().indexOf("add"));
          AddFile addFile = new AddFile(addFileRow);
          deltaTableDataFilePaths.add(getFullFilePath(addFile.getPath(), deltaTableLocation));
        }
      }
    }

    icebergTable
        .currentSnapshot()
        .addedDataFiles(icebergTable.io())
        .forEach(
            dataFile -> {
              assertThat(URI.create(dataFile.location()).isAbsolute()).isTrue();
              assertThat(deltaTableDataFilePaths).contains(dataFile.location());
            });
  }

  private org.apache.iceberg.Table getIcebergTable(String icebergTableIdentifier) {
    CatalogPlugin defaultCatalog = spark.sessionState().catalogManager().currentCatalog();
    Spark3Util.CatalogAndIdentifier catalogAndIdent =
        Spark3Util.catalogAndIdentifier(
            "test catalog", spark, icebergTableIdentifier, defaultCatalog);
    return Spark3Util.loadIcebergCatalog(spark, catalogAndIdent.catalog().name())
        .loadTable(TableIdentifier.parse(catalogAndIdent.identifier().toString()));
  }

  private String destName(String catalogName, String dest) {
    if (catalogName.equals(DEFAULT_SPARK_CATALOG)) {
      return NAMESPACE + "." + catalogName + "_" + dest;
    }
    return catalogName + "." + NAMESPACE + "." + catalogName + "_" + dest;
  }

  /**
   * Add parquet files manually to a delta lake table to mock the situation that some data files are
   * not in the same location as the delta lake table. This simulates the case where data files have
   * absolute paths.
   *
   * <p>Note: This method uses Spark SQL to add external files since Kernel API doesn't support
   * write operations directly.
   */
  private void addExternalDatafiles(
      String targetDeltaTableLocation, String sourceDeltaTableLocation) {
    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    Engine engine = DefaultEngine.create(hadoopConf);
    Table targetTable = Table.forPath(engine, targetDeltaTableLocation);
    Transaction transaction =
        targetTable
            .createTransactionBuilder(engine, "Delta-Lake/4.0.0", Operation.MANUAL_UPDATE)
            .build(engine);
    Table sourceTable = Table.forPath(engine, sourceDeltaTableLocation);
    io.delta.kernel.Snapshot sourceSnapshot = sourceTable.getLatestSnapshot(engine);
    List<Row> addFileActions = Lists.newArrayList();
    Scan scan = sourceSnapshot.getScanBuilder().build();
    try (CloseableIterator<FilteredColumnarBatch> batches = scan.getScanFiles(engine)) {
      while (batches.hasNext()) {
        FilteredColumnarBatch batch = batches.next();
        try (CloseableIterator<Row> rows = batch.getRows()) {
          while (rows.hasNext()) {
            Row row = rows.next();
            if (DeltaActionUtils.isAdd(row)) {
              addFileActions.add(
                  rebuildAddFile(DeltaActionUtils.getAdd(row), sourceDeltaTableLocation));
            }
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to scan source Delta table", e);
    }
    try {
      transaction.commit(engine, inMemoryIterable(toCloseableIterator(addFileActions.iterator())));
    } catch (Exception e) {
      throw new RuntimeException("Failed to commit external AddFiles", e);
    }
  }

  private Row rebuildAddFile(AddFile original, String sourceTableLocation) {
    String absolutePath = getFullFilePath(original.getPath(), sourceTableLocation);
    return SingleAction.createAddFileSingleAction(
        AddFile.createAddFileRow(
            AddFile.FULL_SCHEMA,
            absolutePath,
            original.getPartitionValues(),
            original.getSize(),
            original.getModificationTime(),
            true,
            Optional.empty(),
            original.getTags(),
            original.getBaseRowId(),
            original.getDefaultRowCommitVersion(),
            original.getStats()));
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
      Dataset<org.apache.spark.sql.Row> df,
      String identifier,
      String path,
      String... partitionColumns) {
    spark.sql(String.format("DROP TABLE IF EXISTS %s", identifier));
    if (partitionColumns.length > 0) {
      df.write()
          .format("delta")
          .mode(SaveMode.Append)
          .option("path", path)
          .partitionBy(partitionColumns)
          .saveAsTable(identifier);
    } else {
      df.write().format("delta").mode(SaveMode.Append).option("path", path).saveAsTable(identifier);
    }
  }

  private long countDataFilesInDeltaLakeTable(
      Engine engine, Table deltaTable, long firstConstructableVersion) {
    long dataFilesCount = 0L;

    // Count files in initial snapshot
    io.delta.kernel.Snapshot initialSnapshot =
        deltaTable.getSnapshotAsOfVersion(engine, firstConstructableVersion);
    Scan initialScan = initialSnapshot.getScanBuilder().build();
    try (CloseableIterator<FilteredColumnarBatch> scanIter = initialScan.getScanFiles(engine)) {
      while (scanIter.hasNext()) {
        FilteredColumnarBatch batch = scanIter.next();
        try (CloseableIterator<Row> rows = batch.getRows()) {
          while (rows.hasNext()) {
            Row row = rows.next();
            if (DeltaActionUtils.isAdd(row)) {
              dataFilesCount++;
            }
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to scan initial Delta snapshot at version " + firstConstructableVersion, e);
    }

    // Count files in subsequent versions
    long latestVersion = deltaTable.getLatestSnapshot(engine).getVersion();
    TableImpl tableImpl = (TableImpl) deltaTable;
    try {
      for (long version = firstConstructableVersion + 1; version <= latestVersion; version++) {
        try (CloseableIterator<ColumnarBatch> changes =
            tableImpl.getChanges(
                engine, version, version, Set.of(DeltaLogActionUtils.DeltaAction.ADD))) {
          while (changes.hasNext()) {
            ColumnarBatch batch = changes.next();
            try (CloseableIterator<Row> rows = batch.getRows()) {
              while (rows.hasNext()) {
                Row row = rows.next();
                if (DeltaActionUtils.isAdd(row)) {
                  dataFilesCount++;
                }
              }
            }
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to read Delta change logs after version " + firstConstructableVersion, e);
    }

    return dataFilesCount;
  }
}
