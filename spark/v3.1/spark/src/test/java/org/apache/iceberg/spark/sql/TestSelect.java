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
package org.apache.iceberg.spark.sql;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class TestSelect extends SparkCatalogTestBase {
  private int scanEventCount = 0;
  private ScanEvent lastScanEvent = null;

  public TestSelect(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);

    // register a scan event listener to validate pushdown
    Listeners.register(
        event -> {
          scanEventCount += 1;
          lastScanEvent = event;
        },
        ScanEvent.class);
  }

  @Before
  public void createTables() {
    sql("CREATE TABLE %s (id bigint, data string, float float) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', float('NaN'))", tableName);

    this.scanEventCount = 0;
    this.lastScanEvent = null;
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testSelect() {
    List<Object[]> expected =
        ImmutableList.of(row(1L, "a", 1.0F), row(2L, "b", 2.0F), row(3L, "c", Float.NaN));

    assertEquals("Should return all expected rows", expected, sql("SELECT * FROM %s", tableName));
  }

  @Test
  public void testSelectRewrite() {
    List<Object[]> expected = ImmutableList.of(row(3L, "c", Float.NaN));

    assertEquals(
        "Should return all expected rows",
        expected,
        sql("SELECT * FROM %s where float = float('NaN')", tableName));

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals(
        "Should push down expected filter",
        "(float IS NOT NULL AND is_nan(float))",
        Spark3Util.describe(lastScanEvent.filter()));
  }

  @Test
  public void testProjection() {
    List<Object[]> expected = ImmutableList.of(row(1L), row(2L), row(3L));

    assertEquals("Should return all expected rows", expected, sql("SELECT id FROM %s", tableName));

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals(
        "Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());
    Assert.assertEquals(
        "Should project only the id column",
        validationCatalog.loadTable(tableIdent).schema().select("id").asStruct(),
        lastScanEvent.projection().asStruct());
  }

  @Test
  public void testExpressionPushdown() {
    List<Object[]> expected = ImmutableList.of(row("b"));

    assertEquals(
        "Should return all expected rows",
        expected,
        sql("SELECT data FROM %s WHERE id = 2", tableName));

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals(
        "Should push down expected filter",
        "(id IS NOT NULL AND id = 2)",
        Spark3Util.describe(lastScanEvent.filter()));
    Assert.assertEquals(
        "Should project only id and data columns",
        validationCatalog.loadTable(tableIdent).schema().select("id", "data").asStruct(),
        lastScanEvent.projection().asStruct());
  }

  @Test
  public void testMetadataTables() {
    Assume.assumeFalse(
        "Spark session catalog does not support metadata tables",
        "spark_catalog".equals(catalogName));

    assertEquals(
        "Snapshot metadata table",
        ImmutableList.of(row(ANY, ANY, null, "append", ANY, ANY)),
        sql("SELECT * FROM %s.snapshots", tableName));
  }

  @Test
  public void testSnapshotInTableName() {
    Assume.assumeFalse(
        "Spark session catalog does not support extended table names",
        "spark_catalog".equals(catalogName));

    // get the snapshot ID of the last write and get the current row set as expected
    long snapshotId = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
    List<Object[]> expected = sql("SELECT * FROM %s", tableName);

    // create a second snapshot
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);

    String prefix = "snapshot_id_";
    // read the table at the snapshot
    List<Object[]> actual = sql("SELECT * FROM %s.%s", tableName, prefix + snapshotId);
    assertEquals("Snapshot at specific ID, prefix " + prefix, expected, actual);

    // read the table using DataFrameReader option
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.SNAPSHOT_ID, snapshotId)
            .load(tableName);
    List<Object[]> fromDF = rowsToJava(df.collectAsList());
    assertEquals("Snapshot at specific ID " + snapshotId, expected, fromDF);
  }

  @Test
  public void testTimestampInTableName() {
    Assume.assumeFalse(
        "Spark session catalog does not support extended table names",
        "spark_catalog".equals(catalogName));

    // get a timestamp just after the last write and get the current row set as expected
    long snapshotTs = validationCatalog.loadTable(tableIdent).currentSnapshot().timestampMillis();
    long timestamp = waitUntilAfter(snapshotTs + 2);
    List<Object[]> expected = sql("SELECT * FROM %s", tableName);

    // create a second snapshot
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);

    String prefix = "at_timestamp_";
    // read the table at the snapshot
    List<Object[]> actual = sql("SELECT * FROM %s.%s", tableName, prefix + timestamp);
    assertEquals("Snapshot at timestamp, prefix " + prefix, expected, actual);

    // read the table using DataFrameReader option
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.AS_OF_TIMESTAMP, timestamp)
            .load(tableName);
    List<Object[]> fromDF = rowsToJava(df.collectAsList());
    assertEquals("Snapshot at timestamp " + timestamp, expected, fromDF);
  }

  @Test
  public void testSpecifySnapshotAndTimestamp() {
    // get the snapshot ID of the last write
    long snapshotId = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
    // get a timestamp just after the last write
    long timestamp =
        validationCatalog.loadTable(tableIdent).currentSnapshot().timestampMillis() + 2;

    // create a second snapshot
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);

    AssertHelpers.assertThrows(
        "Should not be able to specify both snapshot id and timestamp",
        IllegalArgumentException.class,
        String.format(
            "Cannot specify both snapshot-id (%s) and as-of-timestamp (%s)", snapshotId, timestamp),
        () -> {
          spark
              .read()
              .format("iceberg")
              .option(SparkReadOptions.SNAPSHOT_ID, snapshotId)
              .option(SparkReadOptions.AS_OF_TIMESTAMP, timestamp)
              .load(tableName)
              .collectAsList();
        });
  }
}
