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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Table;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class TestSelect extends SparkCatalogTestBase {
  private int scanEventCount = 0;
  private ScanEvent lastScanEvent = null;
  private String binaryTableName = tableName("binary_table");

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
    sql("DROP TABLE IF EXISTS %s", binaryTableName);
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
  public void testVersionAsOf() {
    // get the snapshot ID of the last write and get the current row set as expected
    long snapshotId = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
    List<Object[]> expected = sql("SELECT * FROM %s", tableName);

    // create a second snapshot
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);

    // read the table at the snapshot
    List<Object[]> actual1 = sql("SELECT * FROM %s VERSION AS OF %s", tableName, snapshotId);
    assertEquals("Snapshot at specific ID", expected, actual1);

    // read the table at the snapshot
    // HIVE time travel syntax
    List<Object[]> actual2 =
        sql("SELECT * FROM %s FOR SYSTEM_VERSION AS OF %s", tableName, snapshotId);
    assertEquals("Snapshot at specific ID", expected, actual2);

    // read the table using DataFrameReader option: versionAsOf
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.VERSION_AS_OF, snapshotId)
            .load(tableName);
    List<Object[]> fromDF = rowsToJava(df.collectAsList());
    assertEquals("Snapshot at specific ID " + snapshotId, expected, fromDF);
  }

  @Test
  public void testTagReference() {
    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createTag("test_tag", snapshotId).commit();
    List<Object[]> expected = sql("SELECT * FROM %s", tableName);

    // create a second snapshot, read the table at the tag
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);
    List<Object[]> actual1 = sql("SELECT * FROM %s VERSION AS OF 'test_tag'", tableName);
    assertEquals("Snapshot at specific tag reference name", expected, actual1);

    // read the table at the tag
    // HIVE time travel syntax
    List<Object[]> actual2 = sql("SELECT * FROM %s FOR SYSTEM_VERSION AS OF 'test_tag'", tableName);
    assertEquals("Snapshot at specific tag reference name", expected, actual2);

    // Spark session catalog does not support extended table names
    if (!"spark_catalog".equals(catalogName)) {
      // read the table using the "tag_" prefix in the table name
      List<Object[]> actual3 = sql("SELECT * FROM %s.tag_test_tag", tableName);
      assertEquals("Snapshot at specific tag reference name, prefix", expected, actual3);
    }

    // read the table using DataFrameReader option: tag
    Dataset<Row> df =
        spark.read().format("iceberg").option(SparkReadOptions.TAG, "test_tag").load(tableName);
    List<Object[]> fromDF = rowsToJava(df.collectAsList());
    assertEquals("Snapshot at specific tag reference name", expected, fromDF);
  }

  @Test
  public void testUseSnapshotIdForTagReferenceAsOf() {
    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId1 = table.currentSnapshot().snapshotId();

    // create a second snapshot, read the table at the snapshot
    List<Object[]> actual = sql("SELECT * FROM %s", tableName);
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);

    table.refresh();
    long snapshotId2 = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createTag(Long.toString(snapshotId1), snapshotId2).commit();

    // currently Spark version travel ignores the type of the AS OF
    // this means if a tag name matches a snapshot ID, it will always choose snapshotID to travel
    // to.
    List<Object[]> travelWithStringResult =
        sql("SELECT * FROM %s VERSION AS OF '%s'", tableName, snapshotId1);
    assertEquals("Snapshot at specific tag reference name", actual, travelWithStringResult);

    List<Object[]> travelWithLongResult =
        sql("SELECT * FROM %s VERSION AS OF %s", tableName, snapshotId1);
    assertEquals("Snapshot at specific tag reference name", actual, travelWithLongResult);
  }

  @Test
  public void testBranchReference() {
    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch("test_branch", snapshotId).commit();
    List<Object[]> expected = sql("SELECT * FROM %s", tableName);

    // create a second snapshot, read the table at the branch
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);
    List<Object[]> actual1 = sql("SELECT * FROM %s VERSION AS OF 'test_branch'", tableName);
    assertEquals("Snapshot at specific branch reference name", expected, actual1);

    // read the table at the branch
    // HIVE time travel syntax
    List<Object[]> actual2 =
        sql("SELECT * FROM %s FOR SYSTEM_VERSION AS OF 'test_branch'", tableName);
    assertEquals("Snapshot at specific branch reference name", expected, actual2);

    // Spark session catalog does not support extended table names
    if (!"spark_catalog".equals(catalogName)) {
      // read the table using the "branch_" prefix in the table name
      List<Object[]> actual3 = sql("SELECT * FROM %s.branch_test_branch", tableName);
      assertEquals("Snapshot at specific branch reference name, prefix", expected, actual3);
    }

    // read the table using DataFrameReader option: branch
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.BRANCH, "test_branch")
            .load(tableName);
    List<Object[]> fromDF = rowsToJava(df.collectAsList());
    assertEquals("Snapshot at specific branch reference name", expected, fromDF);
  }

  @Test
  public void testUnknownReferenceAsOf() {
    Assertions.assertThatThrownBy(
            () -> sql("SELECT * FROM %s VERSION AS OF 'test_unknown'", tableName))
        .hasMessageContaining("Cannot find matching snapshot ID or reference name for version")
        .isInstanceOf(ValidationException.class);
  }

  @Test
  public void testTimestampAsOf() {
    long snapshotTs = validationCatalog.loadTable(tableIdent).currentSnapshot().timestampMillis();
    long timestamp = waitUntilAfter(snapshotTs + 1000);
    waitUntilAfter(timestamp + 1000);
    // AS OF expects the timestamp if given in long format will be of seconds precision
    long timestampInSeconds = TimeUnit.MILLISECONDS.toSeconds(timestamp);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String formattedDate = sdf.format(new Date(timestamp));

    List<Object[]> expected = sql("SELECT * FROM %s", tableName);

    // create a second snapshot
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);

    // read the table at the timestamp in long format i.e 1656507980463.
    List<Object[]> actualWithLongFormat =
        sql("SELECT * FROM %s TIMESTAMP AS OF %s", tableName, timestampInSeconds);
    assertEquals("Snapshot at timestamp", expected, actualWithLongFormat);

    // read the table at the timestamp in date format i.e 2022-06-29 18:40:37
    List<Object[]> actualWithDateFormat =
        sql("SELECT * FROM %s TIMESTAMP AS OF '%s'", tableName, formattedDate);
    assertEquals("Snapshot at timestamp", expected, actualWithDateFormat);

    // HIVE time travel syntax
    // read the table at the timestamp in long format i.e 1656507980463.
    List<Object[]> actualWithLongFormatInHiveSyntax =
        sql("SELECT * FROM %s FOR SYSTEM_TIME AS OF %s", tableName, timestampInSeconds);
    assertEquals("Snapshot at specific ID", expected, actualWithLongFormatInHiveSyntax);

    // read the table at the timestamp in date format i.e 2022-06-29 18:40:37
    List<Object[]> actualWithDateFormatInHiveSyntax =
        sql("SELECT * FROM %s FOR SYSTEM_TIME AS OF '%s'", tableName, formattedDate);
    assertEquals("Snapshot at specific ID", expected, actualWithDateFormatInHiveSyntax);

    // read the table using DataFrameReader option
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.TIMESTAMP_AS_OF, formattedDate)
            .load(tableName);
    List<Object[]> fromDF = rowsToJava(df.collectAsList());
    assertEquals("Snapshot at timestamp " + timestamp, expected, fromDF);
  }

  @Test
  public void testInvalidTimeTravelBasedOnBothAsOfAndTableIdentifier() {
    // get the snapshot ID of the last write
    long snapshotId = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
    // get a timestamp just after the last write
    long timestamp =
        validationCatalog.loadTable(tableIdent).currentSnapshot().timestampMillis() + 2;

    String timestampPrefix = "at_timestamp_";
    String snapshotPrefix = "snapshot_id_";

    // create a second snapshot
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);

    // using snapshot in table identifier and VERSION AS OF
    AssertHelpers.assertThrows(
        "Cannot do time-travel based on both table identifier and AS OF",
        IllegalArgumentException.class,
        "Cannot do time-travel based on both table identifier and AS OF",
        () -> {
          sql(
              "SELECT * FROM %s.%s VERSION AS OF %s",
              tableName, snapshotPrefix + snapshotId, snapshotId);
        });

    // using snapshot in table identifier and TIMESTAMP AS OF
    AssertHelpers.assertThrows(
        "Cannot do time-travel based on both table identifier and AS OF",
        IllegalArgumentException.class,
        "Cannot do time-travel based on both table identifier and AS OF",
        () -> {
          sql(
              "SELECT * FROM %s.%s VERSION AS OF %s",
              tableName, timestampPrefix + timestamp, snapshotId);
        });

    // using timestamp in table identifier and VERSION AS OF
    AssertHelpers.assertThrows(
        "Cannot do time-travel based on both table identifier and AS OF",
        IllegalArgumentException.class,
        "Cannot do time-travel based on both table identifier and AS OF",
        () -> {
          sql(
              "SELECT * FROM %s.%s TIMESTAMP AS OF %s",
              tableName, snapshotPrefix + snapshotId, timestamp);
        });

    // using timestamp in table identifier and TIMESTAMP AS OF
    AssertHelpers.assertThrows(
        "Cannot do time-travel based on both table identifier and AS OF",
        IllegalArgumentException.class,
        "Cannot do time-travel based on both table identifier and AS OF",
        () -> {
          sql(
              "SELECT * FROM %s.%s TIMESTAMP AS OF %s",
              tableName, timestampPrefix + timestamp, timestamp);
        });
  }

  @Test
  public void testInvalidTimeTravelAgainstBranchIdentifierWithAsOf() {
    long snapshotId = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
    validationCatalog.loadTable(tableIdent).manageSnapshots().createBranch("b1").commit();

    // create a second snapshot
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);

    // using branch_b1 in the table identifier and VERSION AS OF
    Assertions.assertThatThrownBy(
            () -> sql("SELECT * FROM %s.branch_b1 VERSION AS OF %s", tableName, snapshotId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot do time-travel based on both table identifier and AS OF");

    // using branch_b1 in the table identifier and TIMESTAMP AS OF
    Assertions.assertThatThrownBy(
            () -> sql("SELECT * FROM %s.branch_b1 TIMESTAMP AS OF now()", tableName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot do time-travel based on both table identifier and AS OF");
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
            "Can specify only one of snapshot-id (%s), as-of-timestamp (%s)",
            snapshotId, timestamp),
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

  @Test
  public void testBinaryInFilter() {
    sql("CREATE TABLE %s (id bigint, binary binary) USING iceberg", binaryTableName);
    sql("INSERT INTO %s VALUES (1, X''), (2, X'1111'), (3, X'11')", binaryTableName);
    List<Object[]> expected = ImmutableList.of(row(2L, new byte[] {0x11, 0x11}));

    assertEquals(
        "Should return all expected rows",
        expected,
        sql("SELECT id, binary FROM %s where binary > X'11'", binaryTableName));
  }

  @Test
  public void testComplexTypeFilter() {
    String complexTypeTableName = tableName("complex_table");
    sql(
        "CREATE TABLE %s (id INT, complex STRUCT<c1:INT,c2:STRING>) USING iceberg",
        complexTypeTableName);
    sql(
        "INSERT INTO TABLE %s VALUES (1, named_struct(\"c1\", 3, \"c2\", \"v1\"))",
        complexTypeTableName);
    sql(
        "INSERT INTO TABLE %s VALUES (2, named_struct(\"c1\", 2, \"c2\", \"v2\"))",
        complexTypeTableName);

    List<Object[]> result =
        sql(
            "SELECT id FROM %s WHERE complex = named_struct(\"c1\", 3, \"c2\", \"v1\")",
            complexTypeTableName);

    assertEquals("Should return all expected rows", ImmutableList.of(row(1)), result);
    sql("DROP TABLE IF EXISTS %s", complexTypeTableName);
  }
}
