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

package org.apache.iceberg.flink;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * In order to get faster execution speed, we only use HadoopCatalog and Avro format. we use streaming mode in the
 * {@link TestFlinkTableSource#testInferedParallelism()} method to get the operator parallelism, and use batch mode in
 * other methods.
 */
@RunWith(Parameterized.class)
public class TestFlinkTableSource extends FlinkTestBase {

  private static final String CATALOG_NAME = "test_catalog";
  private static final String DATABASE_NAME = "test_db";
  private static final String TABLE_NAME = "test_table";
  private final String expectedFilterPushDownExplain = "FilterPushDown";
  private final FileFormat format = FileFormat.AVRO;
  private static String warehouse;

  private TableEnvironment tEnv;
  private boolean isStreamingJob;
  private int scanEventCount = 0;
  private ScanEvent lastScanEvent = null;


  public TestFlinkTableSource(boolean isStreamingJob) {
    this.isStreamingJob = isStreamingJob;

    // register a scan event listener to validate pushdown
    Listeners.register(event -> {
      scanEventCount += 1;
      lastScanEvent = event;
    }, ScanEvent.class);
  }

  @Parameterized.Parameters(name = "isStreaming={0}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (Boolean isStreaming : new Boolean[] {true, false}) {
      parameters.add(new Object[] {isStreaming});
    }
    return parameters;
  }

  @BeforeClass
  public static void createWarehouse() throws IOException {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue("The warehouse should be deleted", warehouseFile.delete());
    // before variables
    warehouse = "file:" + warehouseFile;
  }

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        EnvironmentSettings.Builder settingsBuilder = EnvironmentSettings
            .newInstance()
            .useBlinkPlanner();
        if (isStreamingJob) {
          settingsBuilder.inStreamingMode();
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
          env.enableCheckpointing(400);
          tEnv = StreamTableEnvironment.create(env, settingsBuilder.build());
        } else {
          settingsBuilder.inBatchMode();
          tEnv = TableEnvironment.create(settingsBuilder.build());
        }
      }
    }
    return tEnv;
  }

  @Before
  public void before() {
    sql("CREATE CATALOG %s WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')", CATALOG_NAME,
        warehouse);
    sql("USE CATALOG %s", CATALOG_NAME);
    sql("CREATE DATABASE %s", DATABASE_NAME);
    sql("USE %s", DATABASE_NAME);
    sql("CREATE TABLE %s (id INT, data VARCHAR,d DOUBLE) WITH ('write.format.default'='%s')", TABLE_NAME,
        format.name());
    this.scanEventCount = 0;
    this.lastScanEvent = null;
  }

  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", DATABASE_NAME, TABLE_NAME);
    sql("DROP DATABASE IF EXISTS %s", DATABASE_NAME);
    sql("DROP CATALOG IF EXISTS %s", CATALOG_NAME);
  }

  @Test
  public void testLimitPushDown() {
    useBatchModeAndInsertData();

    String querySql = String.format("SELECT * FROM %s LIMIT 1", TABLE_NAME);
    String explain = getTableEnv().explainSql(querySql);
    String expectedExplain = "LimitPushDown : 1";
    Object[] expectRecord = new Object[] {1, "iceberg", 10.0};
    Assert.assertTrue("Explain should contain LimitPushDown", explain.contains(expectedExplain));
    List<Object[]> result = sql(querySql);
    Assert.assertEquals("Should have 1 record", 1, result.size());
    Assert.assertArrayEquals("Should produce the expected records", expectRecord, result.get(0));

    AssertHelpers.assertThrows("Invalid limit number: -1 ", SqlParserException.class,
        () -> sql("SELECT * FROM %s LIMIT -1", TABLE_NAME));

    Assert.assertEquals("Should have 0 record", 0, sql("SELECT * FROM %s LIMIT 0", TABLE_NAME).size());

    String sqlLimitExceed = String.format("SELECT * FROM %s LIMIT 4", TABLE_NAME);
    List<Object[]> resultExceed = sql(sqlLimitExceed);
    Assert.assertEquals("Should have 3 records", 3, resultExceed.size());
    List<Object[]> expectedList = Lists.newArrayList();
    expectedList.add(new Object[] {1, "iceberg", 10.0});
    expectedList.add(new Object[] {2, "b", 20.0});
    expectedList.add(new Object[] {3, null, 30.0});
    Assert.assertArrayEquals("Should produce the expected records", expectedList.toArray(), resultExceed.toArray());

    String sqlMixed = String.format("SELECT * FROM %s WHERE id = 1 LIMIT 2", TABLE_NAME);
    List<Object[]> mixedResult = sql(sqlMixed);
    Assert.assertEquals("Should have 1 record", 1, mixedResult.size());
    Assert.assertArrayEquals("Should produce the expected records", expectRecord, mixedResult.get(0));
  }

  @Test
  public void testNoFilterPushDown() {
    useBatchModeAndInsertData();
    String sql = String.format("SELECT * FROM %s ", TABLE_NAME);
    String explain = getTableEnv().explainSql(sql);
    Assert.assertFalse("Explain should not contain FilterPushDown", explain.contains(expectedFilterPushDownExplain));
    Assert.assertEquals("Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());
  }

  @Test
  public void testFilterPushDownEqual() {
    useBatchModeAndInsertData();
    String sqlLiteralRight = String.format("SELECT * FROM %s WHERE id = 1 ", TABLE_NAME);
    // scanEventCount + 1
    String explain = getTableEnv().explainSql(sqlLiteralRight);
    String expectedFilter = "ref(name=\"id\") == 1";
    Object[] expectRecord = new Object[] {1, "iceberg", 10.0};
    Assert.assertTrue("Explain should contain the push down filter", explain.contains(expectedFilter));

    List<Object[]> result = sql(sqlLiteralRight);
    Assert.assertEquals("Should have 1 record", 1, result.size());
    Assert.assertArrayEquals("Should produce the expected record", expectRecord, result.get(0));

    // Because we add infer  parallelism, all data files will be scanned first.
    // Flink will call FlinkInputFormat#createInputSplits method to scan the data files,
    // plus the operation to get the execution plan, so there are three scan event.
    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownEqualNull() {
    useBatchModeAndInsertData();
    String sqlEqualNull = String.format("SELECT * FROM %s WHERE data = NULL ", TABLE_NAME);
    String explainEqualNull = getTableEnv().explainSql(sqlEqualNull);
    Assert.assertFalse("Explain should not contain FilterPushDown",
        explainEqualNull.contains(expectedFilterPushDownExplain));

    List<Object[]> result = sql(sqlEqualNull);
    Assert.assertEquals("Should have 0 record", 0, result.size());
    Assert.assertNull("Should not push down a filter", lastScanEvent);
  }

  @Test
  public void testFilterPushDownEqualLiteralOnLeft() {
    useBatchModeAndInsertData();
    String sqlLiteralLeft = String.format("SELECT * FROM %s WHERE 1 = id ", TABLE_NAME);
    String explainLeft = getTableEnv().explainSql(sqlLiteralLeft);
    String expectedFilter = "ref(name=\"id\") == 1";
    Object[] expectRecord = new Object[] {1, "iceberg", 10.0};
    Assert.assertTrue("Explain should contain the push down filter", explainLeft.contains(expectedFilter));

    List<Object[]> resultLeft = sql(sqlLiteralLeft);
    Assert.assertEquals("Should have 1 record", 1, resultLeft.size());
    Assert.assertArrayEquals("Should produce the expected record", expectRecord, resultLeft.get(0));

    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownNoEqual() {
    useBatchModeAndInsertData();
    String sqlNE = String.format("SELECT * FROM %s WHERE id <> 1 ", TABLE_NAME);
    String explainNE = getTableEnv().explainSql(sqlNE);
    String expectedFilter = "ref(name=\"id\") != 1";
    Assert.assertTrue("Explain should contain the push down filter", explainNE.contains(expectedFilter));

    List<Object[]> resultNE = sql(sqlNE);
    Assert.assertEquals("Should have 2 records", 2, resultNE.size());

    List<Object[]> expectedNE = Lists.newArrayList();
    expectedNE.add(new Object[] {2, "b", 20.0});
    expectedNE.add(new Object[] {3, null, 30.0});
    Assert.assertArrayEquals("Should produce the expected record", expectedNE.toArray(), resultNE.toArray());
    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownNoEqualNull() {
    useBatchModeAndInsertData();
    String sqlNotEqualNull = String.format("SELECT * FROM %s WHERE data <> NULL ", TABLE_NAME);
    String explainNotEqualNull = getTableEnv().explainSql(sqlNotEqualNull);
    Assert.assertFalse("Explain should not contain FilterPushDown", explainNotEqualNull.contains(
        expectedFilterPushDownExplain));

    List<Object[]> resultNE = sql(sqlNotEqualNull);
    Assert.assertEquals("Should have 0 records", 0, resultNE.size());
    Assert.assertNull("Should not push down a filter", lastScanEvent);
  }

  @Test
  public void testFilterPushDownAnd() {
    useBatchModeAndInsertData();
    String sqlAnd = String.format("SELECT * FROM %s WHERE id = 1 AND data = 'iceberg' ", TABLE_NAME);
    String explainAnd = getTableEnv().explainSql(sqlAnd);
    Object[] expectRecord = new Object[] {1, "iceberg", 10.0};
    String expectedFilter = "ref(name=\"id\") == 1,ref(name=\"data\") == \"iceberg\"";
    Assert.assertTrue("Explain should contain the push down filter", explainAnd.contains(expectedFilter));

    List<Object[]> resultAnd = sql(sqlAnd);
    Assert.assertEquals("Should have 1 record", 1, resultAnd.size());
    Assert.assertArrayEquals("Should produce the expected record", expectRecord, resultAnd.get(0));

    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    String expected = "(ref(name=\"id\") == 1 and ref(name=\"data\") == \"iceberg\")";
    Assert.assertEquals("Should contain the push down filter", expected, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownOr() {
    useBatchModeAndInsertData();
    String sqlOr = String.format("SELECT * FROM %s WHERE id = 1 OR data = 'b' ", TABLE_NAME);
    String explainOr = getTableEnv().explainSql(sqlOr);
    String expectedFilter = "(ref(name=\"id\") == 1 or ref(name=\"data\") == \"b\")";
    Assert.assertTrue("Explain should contain the push down filter", explainOr.contains(expectedFilter));

    List<Object[]> resultOr = sql(sqlOr);
    Assert.assertEquals("Should have 2 record", 2, resultOr.size());

    List<Object[]> expectedOR = Lists.newArrayList();
    expectedOR.add(new Object[] {1, "iceberg", 10.0});
    expectedOR.add(new Object[] {2, "b", 20.0});
    Assert.assertArrayEquals("Should produce the expected record", expectedOR.toArray(), resultOr.toArray());

    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownGreaterThan() {
    useBatchModeAndInsertData();
    String sqlGT = String.format("SELECT * FROM %s WHERE id > 1 ", TABLE_NAME);
    String explainGT = getTableEnv().explainSql(sqlGT);
    String expectedFilter = "ref(name=\"id\") > 1";
    Assert.assertTrue("Explain should contain the push down filter", explainGT.contains(expectedFilter));

    List<Object[]> resultGT = sql(sqlGT);
    Assert.assertEquals("Should have 2 record", 2, resultGT.size());

    List<Object[]> expectedGT = Lists.newArrayList();
    expectedGT.add(new Object[] {2, "b", 20.0});
    expectedGT.add(new Object[] {3, null, 30.0});
    Assert.assertArrayEquals("Should produce the expected record", expectedGT.toArray(), resultGT.toArray());

    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownGreaterThanNull() {
    useBatchModeAndInsertData();
    String sqlGT = String.format("SELECT * FROM %s WHERE data > null ", TABLE_NAME);
    String explainGT = getTableEnv().explainSql(sqlGT);
    Assert.assertFalse("Explain should not contain FilterPushDown", explainGT.contains(expectedFilterPushDownExplain));

    List<Object[]> resultGT = sql(sqlGT);
    Assert.assertEquals("Should have 0 record", 0, resultGT.size());
    Assert.assertNull("Should not push down a filter", lastScanEvent);
  }

  @Test
  public void testFilterPushDownGreaterThanLiteralOnLeft() {
    useBatchModeAndInsertData();
    String sqlGT = String.format("SELECT * FROM %s WHERE 3 > id ", TABLE_NAME);
    String explainGT = getTableEnv().explainSql(sqlGT);
    String expectedFilter = "ref(name=\"id\") < 3";
    Assert.assertTrue("Explain should contain the push down filter", explainGT.contains(expectedFilter));

    List<Object[]> resultGT = sql(sqlGT);
    Assert.assertEquals("Should have 2 records", 2, resultGT.size());

    List<Object[]> expectedGT = Lists.newArrayList();
    expectedGT.add(new Object[] {1, "iceberg", 10.0});
    expectedGT.add(new Object[] {2, "b", 20.0});
    Assert.assertArrayEquals("Should produce the expected record", expectedGT.toArray(), resultGT.toArray());

    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownGreaterThanEqual() {
    useBatchModeAndInsertData();
    String sqlGTE = String.format("SELECT * FROM %s WHERE id >= 2 ", TABLE_NAME);
    String explainGTE = getTableEnv().explainSql(sqlGTE);
    String expectedFilter = "ref(name=\"id\") >= 2";
    Assert.assertTrue("Explain should contain the push down filter", explainGTE.contains(expectedFilter));

    List<Object[]> resultGTE = sql(sqlGTE);
    Assert.assertEquals("Should have 2 records", 2, resultGTE.size());

    List<Object[]> expectedGTE = Lists.newArrayList();
    expectedGTE.add(new Object[] {2, "b", 20.0});
    expectedGTE.add(new Object[] {3, null, 30.0});
    Assert.assertArrayEquals("Should produce the expected record", expectedGTE.toArray(), resultGTE.toArray());

    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownGreaterThanEqualNull() {
    useBatchModeAndInsertData();
    String sqlGTE = String.format("SELECT * FROM %s WHERE data >= null ", TABLE_NAME);
    String explainGTE = getTableEnv().explainSql(sqlGTE);
    Assert.assertFalse("Explain should not contain FilterPushDown", explainGTE.contains(expectedFilterPushDownExplain));

    List<Object[]> resultGT = sql(sqlGTE);
    Assert.assertEquals("Should have 0 record", 0, resultGT.size());
    Assert.assertNull("Should not push down a filter", lastScanEvent);
  }

  @Test
  public void testFilterPushDownGreaterThanEqualLiteralOnLeft() {
    useBatchModeAndInsertData();
    String sqlGTE = String.format("SELECT * FROM %s WHERE 2 >= id ", TABLE_NAME);
    String explainGTE = getTableEnv().explainSql(sqlGTE);
    String expectedFilter = "ref(name=\"id\") <= 2";
    Assert.assertTrue("Explain should contain the push down filter", explainGTE.contains(expectedFilter));

    List<Object[]> resultGTE = sql(sqlGTE);
    Assert.assertEquals("Should have 2 records", 2, resultGTE.size());

    List<Object[]> expectedGTE = Lists.newArrayList();
    expectedGTE.add(new Object[] {1, "iceberg", 10.0});
    expectedGTE.add(new Object[] {2, "b", 20.0});
    Assert.assertArrayEquals("Should produce the expected record", expectedGTE.toArray(), resultGTE.toArray());

    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownLessThan() {
    useBatchModeAndInsertData();
    String sqlLT = String.format("SELECT * FROM %s WHERE id < 2 ", TABLE_NAME);
    String explainLT = getTableEnv().explainSql(sqlLT);
    String expectedFilter = "ref(name=\"id\") < 2";
    Object[] expectRecord = new Object[] {1, "iceberg", 10.0};
    Assert.assertTrue("Explain should contain the push down filter", explainLT.contains(expectedFilter));

    List<Object[]> resultLT = sql(sqlLT);
    Assert.assertEquals("Should have 1 record", 1, resultLT.size());
    Assert.assertArrayEquals("Should produce the expected record", expectRecord, resultLT.get(0));

    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownLessThanNull() {
    useBatchModeAndInsertData();
    String sqlLT = String.format("SELECT * FROM %s WHERE data < null ", TABLE_NAME);
    String explainLT = getTableEnv().explainSql(sqlLT);
    Assert.assertFalse("Explain should not contain FilterPushDown", explainLT.contains(expectedFilterPushDownExplain));

    List<Object[]> resultGT = sql(sqlLT);
    Assert.assertEquals("Should have 0 record", 0, resultGT.size());
    Assert.assertNull("Should not push down a filter", lastScanEvent);
  }

  @Test
  public void testFilterPushDownLessThanLiteralOnLeft() {
    useBatchModeAndInsertData();
    String sqlLT = String.format("SELECT * FROM %s WHERE 2 < id ", TABLE_NAME);
    String explainLT = getTableEnv().explainSql(sqlLT);
    Object[] expectRecord = new Object[] {3, null, 30.0};
    String expectedFilter = "ref(name=\"id\") > 2";
    Assert.assertTrue("Explain should contain the push down filter", explainLT.contains(expectedFilter));

    List<Object[]> resultLT = sql(sqlLT);
    Assert.assertEquals("Should have 1 record", 1, resultLT.size());
    Assert.assertArrayEquals("Should produce the expected record", expectRecord, resultLT.get(0));

    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownLessThanEqual() {
    useBatchModeAndInsertData();
    String sqlLTE = String.format("SELECT * FROM %s WHERE id <= 1 ", TABLE_NAME);
    String explainLTE = getTableEnv().explainSql(sqlLTE);
    Object[] expectRecord = new Object[] {1, "iceberg", 10.0};
    String expectedFilter = "ref(name=\"id\") <= 1";
    Assert.assertTrue("Explain should contain the push down filter", explainLTE.contains(expectedFilter));

    List<Object[]> resultLTE = sql(sqlLTE);
    Assert.assertEquals("Should have 1 record", 1, resultLTE.size());
    Assert.assertArrayEquals("Should produce the expected record", expectRecord, resultLTE.get(0));

    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownLessThanEqualNull() {
    useBatchModeAndInsertData();
    String sqlLTE = String.format("SELECT * FROM %s WHERE data <= null ", TABLE_NAME);
    String explainLTE = getTableEnv().explainSql(sqlLTE);
    Assert.assertFalse("Explain should not contain FilterPushDown", explainLTE.contains(expectedFilterPushDownExplain));

    List<Object[]> resultGT = sql(sqlLTE);
    Assert.assertEquals("Should have 0 record", 0, resultGT.size());
    Assert.assertNull("Should not push down a filter", lastScanEvent);
  }

  @Test
  public void testFilterPushDownLessThanEqualLiteralOnLeft() {
    useBatchModeAndInsertData();
    String sqlLTE = String.format("SELECT * FROM %s WHERE 3 <= id  ", TABLE_NAME);
    String explainLTE = getTableEnv().explainSql(sqlLTE);
    Object[] expectRecord = new Object[] {3, null, 30.0};
    String expectedFilter = "ref(name=\"id\") >= 3";
    Assert.assertTrue("Explain should contain the push down filter", explainLTE.contains(expectedFilter));

    List<Object[]> resultLTE = sql(sqlLTE);
    Assert.assertEquals("Should have 1 record", 1, resultLTE.size());
    Assert.assertArrayEquals("Should produce the expected record", expectRecord, resultLTE.get(0));

    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownIn() {
    useBatchModeAndInsertData();
    String sqlIN = String.format("SELECT * FROM %s WHERE id IN (1,2) ", TABLE_NAME);
    String explainIN = getTableEnv().explainSql(sqlIN);
    String expectedFilter = "(ref(name=\"id\") == 1 or ref(name=\"id\") == 2)";
    Assert.assertTrue("Explain should contain the push down filter", explainIN.contains(expectedFilter));
    List<Object[]> resultIN = sql(sqlIN);
    Assert.assertEquals("Should have 2 records", 2, resultIN.size());

    List<Object[]> expectedIN = Lists.newArrayList();
    expectedIN.add(new Object[] {1, "iceberg", 10.0});
    expectedIN.add(new Object[] {2, "b", 20.0});
    Assert.assertArrayEquals("Should produce the expected record", expectedIN.toArray(), resultIN.toArray());
    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownInNull() {
    useBatchModeAndInsertData();
    String sqlInNull = String.format("SELECT * FROM %s WHERE data IN ('iceberg',NULL) ", TABLE_NAME);
    String explainInNull = getTableEnv().explainSql(sqlInNull);
    Object[] expectRecord = new Object[] {1, "iceberg", 10.0};
    Assert.assertFalse("Explain should not contain FilterPushDown",
        explainInNull.contains(expectedFilterPushDownExplain));

    List<Object[]> result = sql(sqlInNull);
    Assert.assertEquals("Should have 1 record", 1, result.size());
    Assert.assertArrayEquals("Should produce the expected record", expectRecord, result.get(0));
    Assert.assertEquals("Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());
  }

  @Test
  public void testFilterPushDownNotIn() {
    useBatchModeAndInsertData();
    String sqlNotIn = String.format("SELECT * FROM %s WHERE id NOT IN (3,2) ", TABLE_NAME);
    String explainNotIn = getTableEnv().explainSql(sqlNotIn);
    Object[] expectRecord = new Object[] {1, "iceberg", 10.0};
    String expectedFilter = "ref(name=\"id\") != 3,ref(name=\"id\") != 2";
    Assert.assertTrue("Explain should contain the push down filter", explainNotIn.contains(expectedFilter));

    List<Object[]> resultNotIn = sql(sqlNotIn);
    Assert.assertEquals("Should have 1 record", 1, resultNotIn.size());
    Assert.assertArrayEquals("Should produce the expected record", expectRecord, resultNotIn.get(0));
    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    String expectedScan = "(ref(name=\"id\") != 3 and ref(name=\"id\") != 2)";
    Assert.assertEquals("Should contain the push down filter", expectedScan, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownNotInNull() {
    useBatchModeAndInsertData();
    String sqlNotInNull = String.format("SELECT * FROM %s WHERE id NOT IN (1,2,NULL) ", TABLE_NAME);
    String explainNotInNull = getTableEnv().explainSql(sqlNotInNull);
    Assert.assertFalse("Explain should not contain FilterPushDown",
        explainNotInNull.contains(expectedFilterPushDownExplain));
    List<Object[]> resultGT = sql(sqlNotInNull);
    Assert.assertEquals("Should have 0 record", 0, resultGT.size());
    Assert.assertEquals("Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());
  }

  @Test
  public void testFilterPushDownIsNotNull() {
    useBatchModeAndInsertData();
    String sqlNotNull = String.format("SELECT * FROM %s WHERE data IS NOT NULL", TABLE_NAME);
    String explainNotNull = getTableEnv().explainSql(sqlNotNull);
    String expectedFilter = "not_null(ref(name=\"data\"))";
    Assert.assertTrue("Explain should contain the push down filter", explainNotNull.contains(expectedFilter));

    List<Object[]> resultNotNull = sql(sqlNotNull);
    Assert.assertEquals("Should have 2 record", 2, resultNotNull.size());

    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {1, "iceberg", 10.0});
    expected.add(new Object[] {2, "b", 20.0});
    Assert.assertArrayEquals("Should produce the expected record", expected.toArray(), resultNotNull.toArray());

    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownIsNull() {
    useBatchModeAndInsertData();
    String sqlNull = String.format("SELECT * FROM %s WHERE data IS  NULL", TABLE_NAME);
    String explainNull = getTableEnv().explainSql(sqlNull);
    Object[] expectRecord = new Object[] {3, null, 30.0};
    String expectedFilter = "is_null(ref(name=\"data\"))";
    Assert.assertTrue("Explain should contain the push down filter", explainNull.contains(expectedFilter));

    List<Object[]> resultNull = sql(sqlNull);
    Assert.assertEquals("Should have 1 record", 1, resultNull.size());
    Assert.assertArrayEquals("Should produce the expected record", expectRecord, resultNull.get(0));

    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownNot() {
    useBatchModeAndInsertData();
    String sqlNot = String.format("SELECT * FROM %s WHERE NOT (id = 1 OR id = 2 ) ", TABLE_NAME);
    Object[] expectRecord = new Object[] {3, null, 30.0};
    String explainNot = getTableEnv().explainSql(sqlNot);
    String expectedFilter = "ref(name=\"id\") != 1,ref(name=\"id\") != 2";
    Assert.assertTrue("Explain should contain the push down filter", explainNot.contains(expectedFilter));

    List<Object[]> resultNot = sql(sqlNot);
    Assert.assertEquals("Should have 1 record", 1, resultNot.size());
    Assert.assertArrayEquals("Should produce the expected record", expectRecord, resultNot.get(0));

    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    expectedFilter = "(ref(name=\"id\") != 1 and ref(name=\"id\") != 2)";
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownBetween() {
    useBatchModeAndInsertData();
    String sqlBetween = String.format("SELECT * FROM %s WHERE id BETWEEN 1 AND 2 ", TABLE_NAME);
    String explainBetween = getTableEnv().explainSql(sqlBetween);
    String expectedFilter = "ref(name=\"id\") >= 1,ref(name=\"id\") <= 2";
    Assert.assertTrue("Explain should contain the push down filter", explainBetween.contains(expectedFilter));

    List<Object[]> resultBetween = sql(sqlBetween);
    Assert.assertEquals("Should have 2 record", 2, resultBetween.size());

    List<Object[]> expectedBetween = Lists.newArrayList();
    expectedBetween.add(new Object[] {1, "iceberg", 10.0});
    expectedBetween.add(new Object[] {2, "b", 20.0});
    Assert.assertArrayEquals("Should produce the expected record", expectedBetween.toArray(), resultBetween.toArray());

    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    String expected = "(ref(name=\"id\") >= 1 and ref(name=\"id\") <= 2)";
    Assert.assertEquals("Should contain the push down filter", expected, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownNotBetween() {
    useBatchModeAndInsertData();
    String sqlNotBetween = String.format("SELECT * FROM %s WHERE id  NOT BETWEEN 2 AND 3 ", TABLE_NAME);
    String explainNotBetween = getTableEnv().explainSql(sqlNotBetween);
    Object[] expectRecord = new Object[] {1, "iceberg", 10.0};
    String expectedFilter = "(ref(name=\"id\") < 2 or ref(name=\"id\") > 3)";
    Assert.assertTrue("Explain should contain the push down filter", explainNotBetween.contains(expectedFilter));

    List<Object[]> resultNotBetween = sql(sqlNotBetween);
    Assert.assertEquals("Should have 1 record", 1, resultNotBetween.size());
    Assert.assertArrayEquals("Should produce the expected record", expectRecord, resultNotBetween.get(0));

    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterPushDownLike() {
    useBatchModeAndInsertData();
    String sqlLike = "SELECT * FROM " + TABLE_NAME + " WHERE data LIKE 'ice%' ";
    Object[] expectRecord = new Object[] {1, "iceberg", 10.0};
    String explainLike = getTableEnv().explainSql(sqlLike);
    String expectedFilter = "ref(name=\"data\") startsWith \"\"ice\"\"";
    Assert.assertTrue("the like sql Explain should contain the push down filter", explainLike.contains(expectedFilter));

    sqlLike = "SELECT * FROM " + TABLE_NAME + " WHERE data LIKE 'ice%%' ";
    List<Object[]> resultLike = sql(sqlLike);
    Assert.assertEquals("Should have 1 record", 1, resultLike.size());
    Assert.assertArrayEquals("The like result should produce the expected record", expectRecord, resultLike.get(0));
    Assert.assertEquals("Should create 3 scans", 3, scanEventCount);
    Assert.assertEquals("Should contain the push down filter", expectedFilter, lastScanEvent.filter().toString());
  }

  @Test
  public void testFilterNotPushDownLike() {
    useBatchModeAndInsertData();
    String sqlNoPushDown = "SELECT * FROM " + TABLE_NAME + " WHERE data LIKE '%i' ";
    String explainNoPushDown = getTableEnv().explainSql(sqlNoPushDown);
    Object[] expectRecord = new Object[] {1, "iceberg", 10.0};
    Assert.assertFalse("Explain should not contain FilterPushDown",
        explainNoPushDown.contains(expectedFilterPushDownExplain));
    sqlNoPushDown = "SELECT * FROM " + TABLE_NAME + " WHERE data LIKE '%%i' ";
    List<Object[]> resultLike = sql(sqlNoPushDown);
    Assert.assertEquals("Should have 1 record", 0, resultLike.size());
    Assert.assertEquals("Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());

    sqlNoPushDown = "SELECT * FROM " + TABLE_NAME + " WHERE data LIKE '%i%' ";
    explainNoPushDown = getTableEnv().explainSql(sqlNoPushDown);
    Assert.assertFalse("Explain should not contain FilterPushDown",
        explainNoPushDown.contains(expectedFilterPushDownExplain));
    sqlNoPushDown = "SELECT * FROM " + TABLE_NAME + " WHERE data LIKE '%%i%%' ";
    resultLike = sql(sqlNoPushDown);
    Assert.assertEquals("Should have 1 record", 1, resultLike.size());
    Assert.assertArrayEquals("Should produce the expected record", expectRecord, resultLike.get(0));
    Assert.assertEquals("Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());

    sqlNoPushDown = "SELECT * FROM  " + TABLE_NAME + "  WHERE data LIKE '%ice%g' ";
    explainNoPushDown = getTableEnv().explainSql(sqlNoPushDown);
    Assert.assertFalse("Explain should not contain FilterPushDown",
        explainNoPushDown.contains(expectedFilterPushDownExplain));
    sqlNoPushDown = "SELECT * FROM  " + TABLE_NAME + "  WHERE data LIKE '%%ice%%g' ";
    resultLike = sql(sqlNoPushDown);
    Assert.assertEquals("Should have 1 record", 1, resultLike.size());
    Assert.assertArrayEquals("Should produce the expected record", expectRecord, resultLike.get(0));
    Assert.assertEquals("Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());

    sqlNoPushDown = "SELECT * FROM  " + TABLE_NAME + "  WHERE data LIKE '%' ";
    explainNoPushDown = getTableEnv().explainSql(sqlNoPushDown);
    Assert.assertFalse("Explain should not contain FilterPushDown",
        explainNoPushDown.contains(expectedFilterPushDownExplain));
    sqlNoPushDown = "SELECT * FROM  " + TABLE_NAME + "  WHERE data LIKE '%%' ";
    resultLike = sql(sqlNoPushDown);
    Assert.assertEquals("Should have 2 records", 2, resultLike.size());
    List<Object[]> expectedRecords = Lists.newArrayList();
    expectedRecords.add(new Object[] {1, "iceberg", 10.0});
    expectedRecords.add(new Object[] {2, "b", 20.0});
    Assert.assertArrayEquals("Should produce the expected record", expectedRecords.toArray(), resultLike.toArray());
    Assert.assertEquals("Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());

    sqlNoPushDown = "SELECT * FROM  " + TABLE_NAME + "  WHERE data LIKE 'iceber_' ";
    explainNoPushDown = getTableEnv().explainSql(sqlNoPushDown);
    Assert.assertFalse("Explain should not contain FilterPushDown",
        explainNoPushDown.contains(expectedFilterPushDownExplain));
    resultLike = sql(sqlNoPushDown);
    Assert.assertEquals("Should have 1 record", 1, resultLike.size());
    Assert.assertArrayEquals("Should produce the expected record", expectRecord, resultLike.get(0));
    Assert.assertEquals("Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());

    sqlNoPushDown = "SELECT * FROM  " + TABLE_NAME + "  WHERE data LIKE 'i%g' ";
    explainNoPushDown = getTableEnv().explainSql(sqlNoPushDown);
    Assert.assertFalse("Explain should not contain FilterPushDown",
        explainNoPushDown.contains(expectedFilterPushDownExplain));
    sqlNoPushDown = "SELECT * FROM  " + TABLE_NAME + "  WHERE data LIKE 'i%%g' ";
    resultLike = sql(sqlNoPushDown);
    Assert.assertEquals("Should have 1 record", 1, resultLike.size());
    Assert.assertArrayEquals("Should produce the expected record", expectRecord, resultLike.get(0));
    Assert.assertEquals("Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());
  }

  @Test
  public void testFilterPushDown2Literal() {
    useBatchModeAndInsertData();
    String sql2Literal = String.format("SELECT * FROM %s WHERE 1 > 0 ", TABLE_NAME);
    String explain2Literal = getTableEnv().explainSql(sql2Literal);
    Assert.assertFalse("Explain should not contain FilterPushDown",
        explain2Literal.contains(expectedFilterPushDownExplain));
    Assert.assertEquals("Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());
  }

  /**
   * NaN is not supported by flink now, so we add the test case to assert the parse error, when we upgrade the flink
   * that supports NaN, we will delele the method, and add some test case to test NaN.
   */
  @Test
  public void testSqlParseError() {
    useBatchModeAndInsertData();
    String sqlParseErrorEqual = String.format("SELECT * FROM %s WHERE d = CAST('NaN' AS DOUBLE) ", TABLE_NAME);
    AssertHelpers.assertThrows("The NaN is not supported by flink now. ",
        NumberFormatException.class, () -> sql(sqlParseErrorEqual));

    String sqlParseErrorNotEqual = String.format("SELECT * FROM %s WHERE d <> CAST('NaN' AS DOUBLE) ", TABLE_NAME);
    AssertHelpers.assertThrows("The NaN is not supported by flink now. ",
        NumberFormatException.class, () -> sql(sqlParseErrorNotEqual));

    String sqlParseErrorGT = String.format("SELECT * FROM %s WHERE d > CAST('NaN' AS DOUBLE) ", TABLE_NAME);
    AssertHelpers.assertThrows("The NaN is not supported by flink now. ",
        NumberFormatException.class, () -> sql(sqlParseErrorGT));

    String sqlParseErrorLT = String.format("SELECT * FROM %s WHERE d < CAST('NaN' AS DOUBLE) ", TABLE_NAME);
    AssertHelpers.assertThrows("The NaN is not supported by flink now. ",
        NumberFormatException.class, () -> sql(sqlParseErrorLT));

    String sqlParseErrorGTE = String.format("SELECT * FROM %s WHERE d >= CAST('NaN' AS DOUBLE) ", TABLE_NAME);
    AssertHelpers.assertThrows("The NaN is not supported by flink now. ",
        NumberFormatException.class, () -> sql(sqlParseErrorGTE));

    String sqlParseErrorLTE = String.format("SELECT * FROM %s WHERE d <= CAST('NaN' AS DOUBLE) ", TABLE_NAME);
    AssertHelpers.assertThrows("The NaN is not supported by flink now. ",
        NumberFormatException.class, () -> sql(sqlParseErrorLTE));
  }

  /**
   * The sql can be executed in both streaming and batch mode, in order to get the parallelism, we convert the flink
   * Table to flink DataStream, so we only use streaming mode here.
   *
   * @throws TableNotExistException table not exist exception
   */
  @Test
  public void testInferedParallelism() throws TableNotExistException {
    Assume.assumeTrue("The execute mode should  be streaming mode", isStreamingJob);

    // Empty table ,parallelism at least 1
    Table tableEmpty = sqlQuery("SELECT * FROM %s", TABLE_NAME);
    assertParallelismEquals(tableEmpty, 1);

    sql("INSERT INTO %s  VALUES (1,'hello',10.0)", TABLE_NAME);
    sql("INSERT INTO %s  VALUES (2,'iceberg',20.0)", TABLE_NAME);

    // Make sure to generate 2 CombinedScanTasks
    Optional<Catalog> catalog = tEnv.getCatalog(CATALOG_NAME);
    Assert.assertTrue("Conversion should succeed", catalog.isPresent());
    FlinkCatalog flinkCatalog = (FlinkCatalog) catalog.get();
    org.apache.iceberg.Table table = flinkCatalog.loadIcebergTable(new ObjectPath(DATABASE_NAME, TABLE_NAME));
    Stream<FileScanTask> stream = StreamSupport.stream(table.newScan().planFiles().spliterator(), false);
    Optional<FileScanTask> fileScanTaskOptional = stream.max(Comparator.comparing(FileScanTask::length));
    Assert.assertTrue("Conversion should succeed", fileScanTaskOptional.isPresent());
    long maxFileLen = fileScanTaskOptional.get().length();
    sql("ALTER TABLE %s SET ('read.split.open-file-cost'='1', 'read.split.target-size'='%s')", TABLE_NAME,
        maxFileLen);

    // 2 splits ,the parallelism is  2
    Table tableSelect = sqlQuery("SELECT * FROM %s", TABLE_NAME);
    assertParallelismEquals(tableSelect, 2);

    // 2 splits  and limit is 1 ,the parallelism is  1
    Table tableLimit = sqlQuery("SELECT * FROM %s LIMIT 1", TABLE_NAME);
    assertParallelismEquals(tableLimit, 1);
  }

  private Table sqlQuery(String sql, Object... args) {
    return getTableEnv().sqlQuery(String.format(sql, args));
  }

  private void assertParallelismEquals(Table table, int expected) {
    Assert.assertTrue("The table environment should be StreamTableEnvironment",
        getTableEnv() instanceof StreamTableEnvironment);
    StreamTableEnvironment stenv = (StreamTableEnvironment) getTableEnv();
    DataStream<Row> ds = stenv.toAppendStream(table, Row.class);
    int parallelism = ds.getTransformation().getParallelism();
    Assert.assertEquals("Should produce the expected parallelism ", expected, parallelism);
  }

  private void useBatchModeAndInsertData() {
    Assume.assumeFalse("Should  be supported on batch mode", isStreamingJob);
    sql("INSERT INTO %s VALUES (1,'iceberg',10),(2,'b',20),(3,CAST(NULL AS VARCHAR),30)", TABLE_NAME);
  }
}
