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

package org.apache.iceberg.spark.data;

import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestParquetReader extends SparkTestBaseWithCatalog {
  private static SparkSession spark = null;
  private static String catalogName = "spark_catalog";
  private static String implementation = SparkSessionCatalog.class.getName();
  private static Map<String, String> config = ImmutableMap.of(
      "type", "hive",
      "default-namespace", "default",
      "parquet-enabled", "true",
      "cache-enabled", "false"
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  public TestParquetReader() {
    super(catalogName, implementation, config);
    spark.sessionState().catalogManager().catalog(catalogName);
  }

  @BeforeClass
  public static void startSpark() {
    TestParquetReader.spark = SparkSession.builder().master("local[2]")
            .config("spark.sql.parquet.writeLegacyFormat", true)
            .getOrCreate();
    spark.sessionState().catalogManager().catalog("spark_catalog");
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestParquetReader.spark;
    TestParquetReader.spark = null;
    currentSpark.stop();
  }

  @Test
  public void testHiveStyleThreeLevelList1() throws Exception {
    String tableName = "default.testHiveStyleThreeLevelList";
    File location = temp.newFolder();
    sql(String.format("CREATE TABLE %s (col1 ARRAY<STRUCT<col2 INT>>)" +
        " STORED AS parquet" +
        " LOCATION '%s'", tableName, location));

    int testValue = 12345;
    sql(String.format("INSERT INTO %s VALUES (ARRAY(STRUCT(%s)))", tableName, testValue));
    List<Object[]> expected = sql(String.format("SELECT * FROM %s", tableName));

    // migrate table
    SparkActions.get().migrateTable(tableName).execute();

    // check migrated table is returning expected result
    List<Object[]> results = sql(String.format("SELECT * FROM %s", tableName));
    Assert.assertTrue(results.size() > 0);
    assertEquals("Output must match", expected, results);
  }

  @Test
  public void testHiveStyleThreeLevelListWithNestedStruct() throws Exception {
    String tableName = "default.testHiveStyleThreeLevelListWithNestedStruct";
    File location = temp.newFolder();
    sql(String.format("CREATE TABLE %s (col1 ARRAY<STRUCT<col2 STRUCT<col3 INT>>>)" +
        " STORED AS parquet" +
        " LOCATION '%s'", tableName, location));

    int testValue = 12345;
    sql(String.format("INSERT INTO %s VALUES (ARRAY(STRUCT(STRUCT(%s))))", tableName, testValue));
    List<Object[]> expected = sql(String.format("SELECT * FROM %s", tableName));

    // migrate table
    SparkActions.get().migrateTable(tableName).execute();

    // check migrated table is returning expected result
    List<Object[]> results = sql(String.format("SELECT * FROM %s", tableName));
    Assert.assertTrue(results.size() > 0);
    assertEquals("Output must match", expected, results);
  }

  @Test
  public void testHiveStyleThreeLevelLists() throws Exception {
    String tableName = "default.testHiveStyleThreeLevelLists";
    File location = temp.newFolder();
    sql(String.format("CREATE TABLE %s (col1 ARRAY<STRUCT<col2 INT>>, col3 ARRAY<STRUCT<col4 INT>>)" +
        " STORED AS parquet" +
        " LOCATION '%s'", tableName, location));

    int testValue1 = 12345;
    int testValue2 = 987654;
    sql(String.format("INSERT INTO %s VALUES (ARRAY(STRUCT(%s)), ARRAY(STRUCT(%s)))",
        tableName, testValue1, testValue2));
    List<Object[]> expected = sql(String.format("SELECT * FROM %s", tableName));

    // migrate table
    SparkActions.get().migrateTable(tableName).execute();

    // check migrated table is returning expected result
    List<Object[]> results = sql(String.format("SELECT * FROM %s", tableName));
    Assert.assertTrue(results.size() > 0);
    assertEquals("Output must match", expected, results);
  }

  @Test
  public void testHiveStyleStructOfThreeLevelLists() throws Exception {
    String tableName = "default.testHiveStyleStructOfThreeLevelLists";
    File location = temp.newFolder();
    sql(String.format("CREATE TABLE %s (col1 STRUCT<col2 ARRAY<STRUCT<col3 INT>>>)" +
        " STORED AS parquet" +
        " LOCATION '%s'", tableName, location));

    int testValue1 = 12345;
    sql(String.format("INSERT INTO %s VALUES (STRUCT(STRUCT(ARRAY(STRUCT(%s)))))",
        tableName, testValue1));
    List<Object[]> expected = sql(String.format("SELECT * FROM %s", tableName));

    // migrate table
    SparkActions.get().migrateTable(tableName).execute();

    // check migrated table is returning expected result
    List<Object[]> results = sql(String.format("SELECT * FROM %s", tableName));
    Assert.assertTrue(results.size() > 0);
    assertEquals("Output must match", expected, results);
  }
}
