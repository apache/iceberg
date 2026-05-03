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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSessionPropertyTimeTravel extends ExtensionsTestBase {

  @Parameter(index = 3)
  private int formatVersion;

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}, formatVersion = {3}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties(),
        2
      },
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties(),
        2
      },
      {
        SparkCatalogConfig.SPARK_SESSION.catalogName(),
        SparkCatalogConfig.SPARK_SESSION.implementation(),
        SparkCatalogConfig.SPARK_SESSION.properties(),
        2
      }
    };
  }

  @BeforeEach
  public void createTable() {
    sql(
        "CREATE TABLE %s (id bigint, data string, float float) USING iceberg"
            + " TBLPROPERTIES ('format-version'='%s')",
        tableName, formatVersion);
    sql(
        "INSERT INTO %s VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', CAST('NaN' AS FLOAT))",
        tableName);
  }

  @AfterEach
  public void removeTables() {
    spark.conf().unset(SparkSQLProperties.AS_OF_TIMESTAMP);
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testSessionPropertyTimeTravel() {
    Table table = validationCatalog.loadTable(tableIdent);
    long timestampBeforeAnySnapshots = 1L;
    long snapshotTs = table.currentSnapshot().timestampMillis();
    long timestamp = waitUntilAfter(snapshotTs + 2);

    List<Object[]> expected = sql("SELECT * FROM %s ORDER BY id", tableName);

    // create a second snapshot
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);

    // session property with a valid timestamp works for simple SQL queries
    spark.conf().set(SparkSQLProperties.AS_OF_TIMESTAMP, String.valueOf(timestamp));
    List<Object[]> actualWithSessionProperty = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertEquals("Should time-travel using session property", expected, actualWithSessionProperty);

    // session property with a valid timestamp works for DataFrame reads
    Dataset<Row> dfSession = spark.read().format("iceberg").load(tableName).orderBy("id");
    assertEquals(
        "DataFrame should time-travel using session property",
        expected,
        rowsToJava(dfSession.collectAsList()));

    // session property with timestamp before any snapshots raises exception
    spark
        .conf()
        .set(SparkSQLProperties.AS_OF_TIMESTAMP, String.valueOf(timestampBeforeAnySnapshots));
    assertThatThrownBy(() -> sql("SELECT * FROM %s", tableName))
        .hasMessageContaining("Cannot find a snapshot older than")
        .isInstanceOf(IllegalArgumentException.class);
  }

  @TestTemplate
  public void testSessionPropertyWithTableSelectorThrowsException() {
    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();
    String tagName = "test_tag";
    String branchName = "test_branch";

    table.manageSnapshots().createTag(tagName, snapshotId).commit();
    table.manageSnapshots().createBranch(branchName, snapshotId).commit();

    long snapshotTs = waitUntilAfter(table.currentSnapshot().timestampMillis() + 1000);
    long timestampInSeconds = TimeUnit.MILLISECONDS.toSeconds(snapshotTs);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String formattedDate = sdf.format(new Date(snapshotTs));

    // create a second snapshot
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0)", tableName);

    // set session property so it is active for all assertions below
    spark.conf().set(SparkSQLProperties.AS_OF_TIMESTAMP, String.valueOf(snapshotTs));

    // snapshot_id_ SQL prefix combined with session property raises exception
    assertThatThrownBy(() -> sql("SELECT * FROM %s.snapshot_id_%s", tableName, snapshotId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot set both snapshot-id and as-of-timestamp to select which table snapshot to scan");

    // at_timestamp_ SQL prefix combined with session property raises exception
    assertThatThrownBy(() -> sql("SELECT * FROM %s.at_timestamp_%s", tableName, snapshotTs))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot set both snapshot-id and as-of-timestamp to select which table snapshot to scan");

    // VERSION AS OF snapshot SQL combined with session property raises exception
    assertThatThrownBy(() -> sql("SELECT * FROM %s VERSION AS OF %s", tableName, snapshotId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot set both snapshot-id and as-of-timestamp to select which table snapshot to scan");

    // VERSION_AS_OF DataFrame option combined with session property raises exception
    assertThatThrownBy(
            () ->
                spark
                    .read()
                    .format("iceberg")
                    .option(SparkReadOptions.VERSION_AS_OF, snapshotId)
                    .load(tableName)
                    .collectAsList())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot set both snapshot-id and as-of-timestamp to select which table snapshot to scan");

    // VERSION AS OF tag SQL combined with session property raises exception
    assertThatThrownBy(() -> sql("SELECT * FROM %s VERSION AS OF '%s'", tableName, tagName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot set both snapshot-id and as-of-timestamp to select which table snapshot to scan");

    if (!"spark_catalog".equals(catalogName)) {
      // tag_ SQL prefix combined with session property raises exception
      assertThatThrownBy(() -> sql("SELECT * FROM %s.tag_%s", tableName, tagName))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(
              "Cannot set both snapshot-id and as-of-timestamp to select which table snapshot to scan");
    }

    // VERSION AS OF branch SQL combined with session property raises exception
    assertThatThrownBy(() -> sql("SELECT * FROM %s VERSION AS OF '%s'", tableName, branchName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot override ref, already set snapshot id=%s", snapshotId);

    if (!"spark_catalog".equals(catalogName)) {
      // branch_ SQL prefix combined with session property raises exception
      assertThatThrownBy(() -> sql("SELECT * FROM %s.branch_%s", tableName, branchName))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Cannot override ref, already set snapshot id=%s", snapshotId);
    }

    // BRANCH DataFrame option combined with session property raises exception
    assertThatThrownBy(
            () ->
                spark
                    .read()
                    .format("iceberg")
                    .option(SparkReadOptions.BRANCH, branchName)
                    .load(tableName)
                    .collectAsList())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot override ref, already set snapshot id=%s", snapshotId);

    // TIMESTAMP AS OF SQL combined with session property raises exception
    assertThatThrownBy(
            () -> sql("SELECT * FROM %s TIMESTAMP AS OF %s", tableName, timestampInSeconds))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot set both snapshot-id and as-of-timestamp to select which table snapshot to scan");

    // TIMESTAMP_AS_OF DataFrame option combined with session property raises exception
    assertThatThrownBy(
            () ->
                spark
                    .read()
                    .format("iceberg")
                    .option(SparkReadOptions.TIMESTAMP_AS_OF, formattedDate)
                    .load(tableName)
                    .collectAsList())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot set both snapshot-id and as-of-timestamp to select which table snapshot to scan");
  }

  @TestTemplate
  public void testSessionPropertyWithMultiTableJoin() {
    String table1Name = tableName("table1");
    TableIdentifier table1Identifier = TableIdentifier.of(Namespace.of("default"), "table1");
    String table2Name = tableName("table2");
    TableIdentifier table2Identifier = TableIdentifier.of(Namespace.of("default"), "table2");

    sql("CREATE OR REPLACE TABLE %s (id bigint, data string) USING iceberg", table1Name);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", table1Name);

    sql("CREATE OR REPLACE TABLE %s (id bigint, value string) USING iceberg", table2Name);
    sql("INSERT INTO %s VALUES (1, 'x'), (2, 'y')", table2Name);

    long timestamp1 =
        waitUntilAfter(
            validationCatalog.loadTable(table1Identifier).currentSnapshot().timestampMillis()
                + 1000);
    long timestamp2 =
        waitUntilAfter(
            validationCatalog.loadTable(table2Identifier).currentSnapshot().timestampMillis()
                + 1000);
    long timestampSnapshot1 = Long.max(timestamp1, timestamp2);

    List<Object[]> expectedJoinSnapshot1 = ImmutableList.of(row(1L, "a", "x"), row(2L, "b", "y"));

    sql("INSERT INTO %s VALUES (3, 'c'), (4, 'd')", table1Name);
    sql("INSERT INTO %s VALUES (3, 'z'), (4, 'w')", table2Name);

    long timestamp3 =
        waitUntilAfter(
            validationCatalog.loadTable(table1Identifier).currentSnapshot().timestampMillis()
                + 1000);
    long timestamp4 =
        waitUntilAfter(
            validationCatalog.loadTable(table2Identifier).currentSnapshot().timestampMillis()
                + 1000);
    long timestampSnapshot2 = Long.max(timestamp3, timestamp4);

    List<Object[]> expectedJoinSnapshot2 =
        ImmutableList.of(
            row(1L, "a", "x"), row(2L, "b", "y"), row(3L, "c", "z"), row(4L, "d", "w"));

    List<Object[]> actual1 =
        sql(
            "SELECT t1.id, t1.data, t2.value FROM %s t1 FULL OUTER JOIN %s t2 ON t1.id = t2.id ORDER BY t1.id",
            table1Name, table2Name);
    assertEquals(
        "Without session property, last snapshot for both table should be read.",
        expectedJoinSnapshot2,
        actual1);

    spark.conf().set(SparkSQLProperties.AS_OF_TIMESTAMP, String.valueOf(timestampSnapshot1));

    List<Object[]> actual2 =
        sql(
            "SELECT t1.id, t1.data, t2.value FROM %s t1 FULL OUTER JOIN %s t2 ON t1.id = t2.id ORDER BY t1.id",
            table1Name, table2Name);
    assertEquals(
        "Session property should apply to both tables in join", expectedJoinSnapshot1, actual2);

    assertThatThrownBy(
            () ->
                sql(
                    "SELECT t1.id, t1.data, t2.value FROM %s TIMESTAMP AS OF %s t1 FULL OUTER JOIN %s t2 ON t1.id = t2.id ORDER BY t1.id",
                    table1Name, timestampSnapshot2 / 1000, table2Name))
        .hasMessageContaining(
            "Cannot set both snapshot-id and as-of-timestamp to select which table snapshot to scan")
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(
            () ->
                sql(
                    "SELECT t2.id, t1.data, t2.value FROM %s t1 FULL OUTER JOIN %s TIMESTAMP AS OF %s t2 ON t1.id = t2.id ORDER BY t2.id",
                    table1Name, table2Name, timestampSnapshot2 / 1000))
        .hasMessageContaining(
            "Cannot set both snapshot-id and as-of-timestamp to select which table snapshot to scan")
        .isInstanceOf(IllegalArgumentException.class);

    sql("DROP TABLE IF EXISTS %s", table1Name);
    sql("DROP TABLE IF EXISTS %s", table2Name);
  }
}
