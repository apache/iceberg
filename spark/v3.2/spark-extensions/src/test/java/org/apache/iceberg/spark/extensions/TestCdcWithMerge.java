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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.ActionsProvider;
import org.apache.iceberg.actions.Cdc;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_NONE;
import static org.apache.iceberg.spark.actions.BaseCdcSparkAction.RECORD_TYPE;

public class TestCdcWithMerge extends SparkRowLevelOperationsTestBase {
  public TestCdcWithMerge(String catalogName, String implementation, Map<String, String> config, String fileFormat,
                          boolean vectorized, String distributionMode) {
    super(catalogName, implementation, config, fileFormat, vectorized, distributionMode);
  }

  /**
   * Overwrite the parameters in the parent class. We need different combinations.
   */
  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}," +
      " format = {3}, vectorized = {4}, distributionMode = {5}")
  public static Object[][] parameters() {
    return new Object[][]{
        {"testhive", SparkCatalog.class.getName(),
            ImmutableMap.of(
                "type", "hive",
                "default-namespace", "default"
            ),
            "parquet",
            true,
            WRITE_DISTRIBUTION_MODE_NONE
        },
        {"testhive", SparkCatalog.class.getName(),
            ImmutableMap.of(
                "type", "hive",
                "default-namespace", "default"
            ),
            "parquet",
            false,
            WRITE_DISTRIBUTION_MODE_NONE
        }
    };
  }

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of(TableProperties.MERGE_MODE, "copy-on-write");
  }

  @BeforeClass
  public static void setupSparkConf() {
    spark.conf().set("spark.sql.shuffle.partitions", "4");
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS source");
  }

  private ActionsProvider actions() {
    return SparkActions.get();
  }

  @Test
  public void testMergeWithOnlyUpdateClause() throws ParseException, NoSuchTableException {
    createAndInitTable("id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" +
            "{ \"id\": 6, \"dep\": \"emp-id-six\" }");

    createOrReplaceView("source", "id INT, dep STRING",
        "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n" +
            "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n" +
            "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    sql("MERGE INTO %s AS t USING source AS s " +
        "ON t.id == s.id " +
        "WHEN MATCHED AND t.id = 1 THEN " +
        "  UPDATE SET *", tableName);

    Table testTable = Spark3Util.loadIcebergTable(spark, tableName);
    Cdc.Result result = actions().generateCdcRecords(testTable).execute();
    Dataset<Row> resultDF = (Dataset<Row>) result.cdcRecords();

    // verify results
    List<Object[]> actualRecords = rowsToJava(resultDF.sort(RECORD_TYPE, "id").collectAsList());
    long snapshotId = testTable.currentSnapshot().snapshotId();
    long snapshotTs = testTable.currentSnapshot().timestampMillis();
    ImmutableList<Object[]> expectedRows = ImmutableList.of(
        row(1, "emp-id-one", "D", snapshotId, snapshotTs, 0),
        row(6, "emp-id-six", "D", snapshotId, snapshotTs, 0),
        row(1, "emp-id-1", "I", snapshotId, snapshotTs, 0),
        row(6, "emp-id-six", "I", snapshotId, snapshotTs, 0)
    );
    assertEquals("Should have expected rows", expectedRows, actualRecords);
  }

  @Test
  public void testMergeWithPositionDelete() throws ParseException, NoSuchTableException {
    createAndInitTable("id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" +
            "{ \"id\": 6, \"dep\": \"emp-id-six\" }");

    withV2MergeOnRead();

    createOrReplaceView("source", "id INT, dep STRING",
        "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n" +
            "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n" +
            "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    sql("MERGE INTO %s AS t USING source AS s " +
        "ON t.id == s.id " +
        "WHEN MATCHED AND t.id = 1 THEN " +
        "  UPDATE SET *", tableName);

    Table testTable = Spark3Util.loadIcebergTable(spark, tableName);
    Cdc.Result result = actions().generateCdcRecords(testTable).execute();
    Dataset<Row> resultDF = (Dataset<Row>) result.cdcRecords();

    // verify results
    List<Object[]> actualRecords = rowsToJava(resultDF.sort(RECORD_TYPE, "id").collectAsList());
    long snapshotId = testTable.currentSnapshot().snapshotId();
    long snapshotTs = testTable.currentSnapshot().timestampMillis();
    ImmutableList<Object[]> expectedRows = ImmutableList.of(
        row(1, "emp-id-one", "D", snapshotId, snapshotTs, 0),
        row(1, "emp-id-1", "I", snapshotId, snapshotTs, 0)
    );
    assertEquals("Should have expected rows", expectedRows, actualRecords);
  }

  private void withV2MergeOnRead() {
    sql("ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')", tableName, TableProperties.FORMAT_VERSION, "2");
    sql("ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')", tableName, TableProperties.MERGE_MODE, "merge-on-read");
  }
}
