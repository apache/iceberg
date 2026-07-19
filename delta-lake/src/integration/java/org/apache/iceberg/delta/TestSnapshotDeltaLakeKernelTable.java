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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.delta.kernel.defaults.engine.DefaultEngine;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.util.LocationUtil;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.delta.catalog.DeltaCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestSnapshotDeltaLakeKernelTable extends SparkDeltaLakeSnapshotTestBase {
  private static final String NAMESPACE = "delta_conversion_test_ns";
  private static final String DEFAULT_SPARK_CATALOG = "spark_catalog";
  private static final String ICEBERG_CATALOG_NAME = "iceberg_hive";
  private static final Map<String, String> BASE_SPARK_CONFIG =
      ImmutableMap.of(
          "type",
          "hive",
          "default-namespace",
          "default",
          "parquet-enabled",
          "true",
          "cache-enabled",
          "false" // Spark will delete tables using v1, leaving the cache out of sync
          );

  private static final int INITIAL_EMPTY_COMMIT = 1;

  private static Dataset<Row> genericDataFrame;

  @TempDir private File sourceLocation;

  public TestSnapshotDeltaLakeKernelTable() {
    super(ICEBERG_CATALOG_NAME, SparkCatalog.class.getName(), BASE_SPARK_CONFIG);
    spark.conf().set("spark.sql.catalog." + DEFAULT_SPARK_CATALOG, DeltaCatalog.class.getName());
  }

  @BeforeAll
  public static void beforeClass() {
    spark.sql(String.format("CREATE DATABASE IF NOT EXISTS %s", NAMESPACE));
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
              new StructField("created_at", DataTypes.TimestampType, true, Metadata.empty()),
              new StructField("event_name", DataTypes.StringType, true, Metadata.empty()),
              new StructField("is_active", DataTypes.BooleanType, true, Metadata.empty()),
              new StructField("price", DataTypes.DoubleType, true, Metadata.empty())
            });
    List<Row> data =
        Arrays.asList(
            RowFactory.create(1, Timestamp.valueOf("2025-01-01 10:00:00"), "Signup", true, 0.00),
            RowFactory.create(
                2, Timestamp.valueOf("2025-01-02 14:30:00"), "Purchase", true, 199.99),
            RowFactory.create(
                3, Timestamp.valueOf("2025-01-03 09:15:00"), "Deactivation", false, 0.00),
            RowFactory.create(4, Timestamp.valueOf("2025-01-03 09:16:00"), "Refund", false, 0.00),
            RowFactory.create(5, Timestamp.valueOf("2025-01-03 09:17:00"), "Refund", false, 0.00));
    genericDataFrame = spark.createDataFrame(data, schema);
  }

  @AfterAll
  public static void afterClass() {
    spark.sql(String.format("DROP DATABASE IF EXISTS %s CASCADE", NAMESPACE));
  }

  @Test
  public void testBasicPartitionedInsertsOnly() {
    String sourceTable = toFullTableName(DEFAULT_SPARK_CATALOG, "partitioned_table");
    String sourceTableLocation = sourceLocation.toURI().toString();

    writeDeltaTable(genericDataFrame, sourceTable, sourceTableLocation, "id");
    spark.sql("INSERT INTO " + sourceTable + " VALUES (10, current_date(), null, null, null);");
    spark.sql("INSERT INTO " + sourceTable + " VALUES (11, current_date(), null, null, null);");
    spark.sql("INSERT INTO " + sourceTable + " VALUES (12, current_date(), null, null, null);");

    String newTableIdentifier = toFullTableName(ICEBERG_CATALOG_NAME, "iceberg_partitioned_table");

    // Act
    SnapshotDeltaLakeTable conversionAction =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeKernelTable(
            spark, newTableIdentifier, sourceTableLocation);
    SnapshotDeltaLakeTable.Result result = conversionAction.execute();

    // Assert
    checkLatestSnapshotIntegrity(sourceTable, newTableIdentifier);
    checkTagContentAndOrder(sourceTable, sourceTableLocation, newTableIdentifier, 0);
    checkIcebergTableLocation(newTableIdentifier, sourceTableLocation);
  }

  @Test
  public void testInsertUpdateDeleteSqls() {
    String sourceTable = toFullTableName(DEFAULT_SPARK_CATALOG, "crud_table");
    String sourceTableLocation = sourceLocation.toURI().toString();

    writeDeltaTable(genericDataFrame, sourceTable, sourceTableLocation, "id");
    spark.sql("INSERT INTO " + sourceTable + " VALUES (10, current_date(), null, null, null);");
    spark.sql("DELETE FROM " + sourceTable + " WHERE id=3;");
    spark.sql("UPDATE " + sourceTable + " SET id=3 WHERE id=1;");
    spark.sql("INSERT INTO " + sourceTable + " VALUES (11, current_date(), null, null, null);");

    String newTableIdentifier = toFullTableName(ICEBERG_CATALOG_NAME, "iceberg_crud_table");

    // Act
    SnapshotDeltaLakeTable conversionAction =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeKernelTable(
            spark, newTableIdentifier, sourceTableLocation);
    SnapshotDeltaLakeTable.Result result = conversionAction.execute();

    // Assert
    checkLatestSnapshotIntegrity(sourceTable, newTableIdentifier);
    checkTagContentAndOrder(sourceTable, sourceTableLocation, newTableIdentifier, 0);
    checkIcebergTableLocation(newTableIdentifier, sourceTableLocation);
  }

  @Test
  public void testConversionAfterVacuum() throws IOException {
    String sourceTable = toFullTableName(DEFAULT_SPARK_CATALOG, "vacuumed_table");
    String sourceTableLocation = sourceLocation.toURI().toString();

    writeDeltaTable(genericDataFrame, sourceTable, sourceTableLocation, "id");
    for (int i = 0; i < 5; i++) {
      spark.sql(
          "UPDATE "
              + sourceTable
              + " SET price="
              + ThreadLocalRandom.current().nextDouble(1000)
              + " where id="
              + i
              + ";");
    }
    spark.sql("UPDATE " + sourceTable + " SET created_at=current_date() ;");
    spark.sql("INSERT INTO " + sourceTable + " VALUES (10, current_date(), null, null, null);");
    spark.sql("INSERT INTO " + sourceTable + " VALUES (11, current_date(), null, null, null);");
    spark.sql("DELETE FROM " + sourceTable + " WHERE id>=10;");
    spark.sql("VACUUM " + sourceTable + " RETAIN 0 HOURS");
    spark.sql(
        "INSERT INTO " + sourceTable + " VALUES (12, current_date(), 'after_vacuum', null, null);");
    spark.sql("UPDATE " + sourceTable + " SET id=13 WHERE id=5;");

    // Checkpoint generated. Simulate logs clean-up
    assertThat(deleteDeltaLogFile("00000000000000000000.json")).isTrue();
    assertThat(deleteDeltaLogFile("00000000000000000001.json")).isTrue();
    assertThat(deleteDeltaLogFile("00000000000000000002.json")).isTrue();

    String newTableIdentifier = toFullTableName(ICEBERG_CATALOG_NAME, "iceberg_vacuumed_table");

    // Act
    SnapshotDeltaLakeTable conversionAction =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeKernelTable(
            spark, newTableIdentifier, sourceTableLocation);
    SnapshotDeltaLakeTable.Result result = conversionAction.execute();

    // Assert
    checkLatestSnapshotIntegrity(sourceTable, newTableIdentifier);
    checkTagContentAndOrder(sourceTable, sourceTableLocation, newTableIdentifier, 10);
    checkIcebergTableLocation(newTableIdentifier, sourceTableLocation);
  }

  @Test
  public void testConversionWithDeletionVectors() {
    String sourceTable = toFullTableName(DEFAULT_SPARK_CATALOG, "dv_table");
    String sourceTableLocation = sourceLocation.toURI().toString();

    spark.sql(String.format("DROP TABLE IF EXISTS %s", sourceTable));

    // 10k records to force usage of DVs in Update operation
    Dataset<Row> dvDf =
        spark
            .range(10000)
            .selectExpr(
                "CAST(id AS int) AS id",
                "current_timestamp() AS created_at",
                "CAST(id AS string) AS event_name",
                "CAST(id % 2 == 0 AS boolean) AS is_active",
                "CAST(id AS double) AS price");

    dvDf.write()
        .format("delta")
        .mode(SaveMode.Append)
        .option("path", sourceTableLocation)
        .option("delta.enableDeletionVectors", "true")
        .option("delta.enableInCommitTimestamps", "true")
        .option("delta.enableRowTracking", "true")
        .saveAsTable(sourceTable);

    spark.sql("UPDATE " + sourceTable + " SET id=-1 WHERE id=1 OR id=2;");

    String newTableIdentifier = toFullTableName(ICEBERG_CATALOG_NAME, "iceberg_dv_table");

    // Act
    SnapshotDeltaLakeTable conversionAction =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeKernelTable(
            spark, newTableIdentifier, sourceTableLocation);
    conversionAction.execute();

    // Assert
    checkLatestSnapshotIntegrity(sourceTable, newTableIdentifier);
    checkTagContentAndOrder(sourceTable, sourceTableLocation, newTableIdentifier, 0);
    checkIcebergTableLocation(newTableIdentifier, sourceTableLocation);
  }

  @ParameterizedTest
  @CsvSource(
      useHeadersInDisplayName = false,
      value = {
        "ALTER TABLE %s ADD COLUMNS (new_col string), ADD COLUMNS",
        "ALTER TABLE %s ALTER COLUMN price COMMENT 'new price comment', CHANGE COLUMN",
        "ALTER TABLE %s SET TBLPROPERTIES ('new_prop' = 'value'), SET TBLPROPERTIES",
        "COMMENT ON TABLE %s IS 'new table comment', SET TBLPROPERTIES"
      })
  public void testSchemaEvolutionAddColumn(String sqlTemplate, String expectedOperation) {
    String sourceTable = toFullTableName(DEFAULT_SPARK_CATALOG, "add_column_table");
    String sourceTableLocation = sourceLocation.toURI().toString();

    writeDeltaTable(genericDataFrame, sourceTable, sourceTableLocation);

    // Schema evolution
    spark.sql(String.format(sqlTemplate, sourceTable));

    String newTableIdentifier = toFullTableName(ICEBERG_CATALOG_NAME, "iceberg_add_column_table");

    // Act & Assert
    SnapshotDeltaLakeTable conversionAction =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeKernelTable(
            spark, newTableIdentifier, sourceTableLocation);

    assertThatThrownBy(conversionAction::execute)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            String.format(
                "Cannot convert Delta table: schema evolution operation '%s' is not supported (detected at Delta version 1).",
                expectedOperation));
  }

  private void checkLatestSnapshotIntegrity(
      String deltaTableIdentifier, String icebergTableIdentifier) {
    checkSnapshotIntegrityForQuery(
        "SELECT * FROM " + deltaTableIdentifier, "SELECT * FROM " + icebergTableIdentifier);
  }

  private void checkSnapshotIntegrityForQuery(String deltaSql, String icebergSql) {
    List<Row> deltaTableContents = spark.sql(deltaSql).collectAsList();
    List<Row> icebergTableContents = spark.sql(icebergSql).collectAsList();

    assertThat(deltaTableContents).hasSize(icebergTableContents.size());
    assertThat(icebergTableContents).containsExactlyInAnyOrderElementsOf(deltaTableContents);
  }

  private void checkTagContentAndOrder(
      String deltaTableIdentifier,
      String deltaTableLocation,
      String icebergTableIdentifier,
      long firstConstructableVersion) {
    DefaultEngine deltaEngine = DefaultEngine.create(spark.sessionState().newHadoopConf());
    io.delta.kernel.Table deltaTable =
        io.delta.kernel.Table.forPath(deltaEngine, deltaTableLocation);
    io.delta.kernel.Snapshot latestSnapshot = deltaTable.getLatestSnapshot(deltaEngine);
    long currentVersion = latestSnapshot.getVersion();
    Table icebergTable = getIcebergTable(icebergTableIdentifier);
    Map<String, SnapshotRef> icebergSnapshotRefs = icebergTable.refs();
    List<Snapshot> icebergSnapshots = Lists.newArrayList(icebergTable.snapshots());

    assertThat(icebergSnapshots)
        .hasSize((int) (currentVersion - firstConstructableVersion + 1) + INITIAL_EMPTY_COMMIT);

    for (int i = 1; i < icebergSnapshots.size(); i++) {
      long deltaVersion = firstConstructableVersion + i - INITIAL_EMPTY_COMMIT;
      Snapshot currentIcebergSnapshot = icebergSnapshots.get(i);

      String expectedVersionTag = "delta-version-" + deltaVersion;
      icebergSnapshotRefs.get(expectedVersionTag);
      assertThat(icebergSnapshotRefs.get(expectedVersionTag)).isNotNull();
      assertThat(icebergSnapshotRefs.get(expectedVersionTag).isTag()).isTrue();
      assertThat(icebergSnapshotRefs.get(expectedVersionTag).snapshotId())
          .isEqualTo(currentIcebergSnapshot.snapshotId());

      long deltaVersionTs =
          deltaTable.getSnapshotAsOfVersion(deltaEngine, deltaVersion).getTimestamp(deltaEngine);
      String expectedTimestampTag = "delta-ts-" + deltaVersionTs;

      assertThat(icebergSnapshotRefs.get(expectedTimestampTag)).isNotNull();
      assertThat(icebergSnapshotRefs.get(expectedTimestampTag).isTag()).isTrue();
      assertThat(icebergSnapshotRefs.get(expectedTimestampTag).snapshotId())
          .isEqualTo(currentIcebergSnapshot.snapshotId());
      checkSnapshotIntegrityForQuery(
          "SELECT * FROM " + deltaTableIdentifier + " VERSION AS OF " + deltaVersion,
          "SELECT * FROM "
              + icebergTableIdentifier
              + " VERSION AS OF '"
              + expectedVersionTag
              + "'");
    }
  }

  private void writeDeltaTable(
      Dataset<Row> df, String identifier, String path, String... partitionColumns) {
    spark.sql(String.format("DROP TABLE IF EXISTS %s", identifier));
    DataFrameWriter<Row> delta =
        df.write()
            .format("delta")
            .mode(SaveMode.Append)
            .option("path", path)
            .option("delta.enableInCommitTimestamps", "true")
            .option("delta.enableRowTracking", "true"); // Increase delta writer version to 7

    if (partitionColumns.length > 0) {
      delta = delta.partitionBy(partitionColumns);
    }
    delta.saveAsTable(identifier);
  }

  private String toFullTableName(String catalogName, String dest) {
    if (catalogName.equals(DEFAULT_SPARK_CATALOG)) {
      return NAMESPACE + "." + catalogName + "_" + dest;
    }
    return catalogName + "." + NAMESPACE + "." + catalogName + "_" + dest;
  }

  private boolean deleteDeltaLogFile(String logName) throws IOException {
    String tablePath = sourceLocation.toPath().toString();
    return Files.deleteIfExists(Paths.get(tablePath, "/_delta_log/", logName));
  }

  private void checkIcebergTableLocation(String icebergTableIdentifier, String expectedLocation) {
    Table icebergTable = getIcebergTable(icebergTableIdentifier);
    assertThat(icebergTable.location())
        .isEqualTo(LocationUtil.stripTrailingSlash(expectedLocation));
  }

  private Table getIcebergTable(String icebergTableIdentifier) {
    CatalogPlugin defaultCatalog = spark.sessionState().catalogManager().currentCatalog();
    Spark3Util.CatalogAndIdentifier catalogAndIdent =
        Spark3Util.catalogAndIdentifier(
            "test catalog", spark, icebergTableIdentifier, defaultCatalog);
    return Spark3Util.loadIcebergCatalog(spark, catalogAndIdent.catalog().name())
        .loadTable(TableIdentifier.parse(catalogAndIdent.identifier().toString()));
  }
}
