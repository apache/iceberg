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
import static org.assertj.core.api.Assertions.assertThat;

import io.delta.standalone.DeltaLog;
import java.io.File;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

  @TempDir private File sourceLocation;
  @TempDir private File destinationLocation;

  public TestSnapshotDeltaLakeKernelTable() {
    super(ICEBERG_CATALOG_NAME, SparkCatalog.class.getName(), BASE_SPARK_CONFIG);
    spark.conf().set("spark.sql.catalog." + DEFAULT_SPARK_CATALOG, DeltaCatalog.class.getName());
  }

  @BeforeAll
  public static void beforeClass() {
    spark.sql(String.format("CREATE DATABASE IF NOT EXISTS %s", NAMESPACE));
  }

  @AfterAll
  public static void afterClass() {
    spark.sql(String.format("DROP DATABASE IF EXISTS %s CASCADE", NAMESPACE));
  }

  @Test
  public void testBasicPartitionedInsertsOnly() {
    String sourceTable = toFullTableName(DEFAULT_SPARK_CATALOG, "partitioned_table");
    String sourceTableLocation = sourceLocation.toURI().toString();

    Dataset<Row> df = spark.range(0, 5, 1, 5).withColumn("dateCol", date_add(current_date(), 1));
    writeDeltaTable(df, sourceTable, sourceTableLocation, "id");

    String newTableIdentifier = toFullTableName(ICEBERG_CATALOG_NAME, "iceberg_partitioned_table");

    // Act
    SnapshotDeltaLakeTable conversionAction =
        DeltaLakeToIcebergMigrationSparkIntegration.snapshotDeltaLakeKernelTable(
            spark, newTableIdentifier, sourceTableLocation);
    SnapshotDeltaLakeTable.Result result = conversionAction.execute();

    // Assert
    checkSnapshotIntegrity(sourceTableLocation, sourceTable, newTableIdentifier, result);
    checkTagContentAndOrder(sourceTableLocation, newTableIdentifier, 0);
    checkIcebergTableLocation(newTableIdentifier, sourceTableLocation);
  }

  private void checkSnapshotIntegrity(
      String deltaTableLocation,
      String deltaTableIdentifier,
      String icebergTableIdentifier,
      SnapshotDeltaLakeTable.Result snapshotReport) {

    List<Row> deltaTableContents =
        spark.sql("SELECT * FROM " + deltaTableIdentifier).collectAsList();
    List<Row> icebergTableContents =
        spark.sql("SELECT * FROM " + icebergTableIdentifier).collectAsList();

    assertThat(deltaTableContents).hasSize(icebergTableContents.size());
    assertThat(icebergTableContents).containsExactlyInAnyOrderElementsOf(deltaTableContents);
  }

  private void checkTagContentAndOrder(
      String deltaTableLocation, String icebergTableIdentifier, long firstConstructableVersion) {
    DeltaLog deltaLog = DeltaLog.forTable(spark.sessionState().newHadoopConf(), deltaTableLocation);
    long currentVersion = deltaLog.snapshot().getVersion();
    Table icebergTable = getIcebergTable(icebergTableIdentifier);
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

      Timestamp deltaVersionTimestamp = deltaLog.getCommitInfoAt(deltaVersion).getTimestamp();
      assertThat(deltaVersionTimestamp).isNotNull();
      String expectedTimestampTag = "delta-ts-" + deltaVersionTimestamp.getTime();

      assertThat(icebergSnapshotRefs.get(expectedTimestampTag)).isNotNull();
      assertThat(icebergSnapshotRefs.get(expectedTimestampTag).isTag()).isTrue();
      assertThat(icebergSnapshotRefs.get(expectedTimestampTag).snapshotId())
          .isEqualTo(currentIcebergSnapshot.snapshotId());
    }
  }

  private void writeDeltaTable(
      Dataset<Row> df, String identifier, String path, String... partitionColumns) {
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

  private String toFullTableName(String catalogName, String dest) {
    if (catalogName.equals(DEFAULT_SPARK_CATALOG)) {
      return NAMESPACE + "." + catalogName + "_" + dest;
    }
    return catalogName + "." + NAMESPACE + "." + catalogName + "_" + dest;
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
