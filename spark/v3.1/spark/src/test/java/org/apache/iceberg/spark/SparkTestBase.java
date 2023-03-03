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
package org.apache.iceberg.spark;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ui.SQLExecutionUIData;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import scala.collection.JavaConverters;

public abstract class SparkTestBase {

  protected static final Object ANY = new Object();

  protected static TestHiveMetastore metastore = null;
  protected static HiveConf hiveConf = null;
  protected static SparkSession spark = null;
  protected static HiveCatalog catalog = null;

  @BeforeClass
  public static void startMetastoreAndSpark() {
    SparkTestBase.metastore = new TestHiveMetastore();
    metastore.start();
    SparkTestBase.hiveConf = metastore.hiveConf();

    SparkTestBase.spark =
        SparkSession.builder()
            .master("local[2]")
            .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
            .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
            .enableHiveSupport()
            .getOrCreate();

    SparkTestBase.catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);

    try {
      catalog.createNamespace(Namespace.of("default"));
    } catch (AlreadyExistsException ignored) {
      // the default namespace already exists. ignore the create error
    }
  }

  @AfterClass
  public static void stopMetastoreAndSpark() throws Exception {
    SparkTestBase.catalog = null;
    if (metastore != null) {
      metastore.stop();
      SparkTestBase.metastore = null;
    }
    if (spark != null) {
      spark.stop();
      SparkTestBase.spark = null;
    }
  }

  protected long waitUntilAfter(long timestampMillis) {
    long current = System.currentTimeMillis();
    while (current <= timestampMillis) {
      current = System.currentTimeMillis();
    }
    return current;
  }

  protected List<Object[]> sql(String query, Object... args) {
    List<Row> rows = spark.sql(String.format(query, args)).collectAsList();
    if (rows.size() < 1) {
      return ImmutableList.of();
    }

    return rowsToJava(rows);
  }

  protected List<Object[]> rowsToJava(List<Row> rows) {
    return rows.stream().map(this::toJava).collect(Collectors.toList());
  }

  private Object[] toJava(Row row) {
    return IntStream.range(0, row.size())
        .mapToObj(
            pos -> {
              if (row.isNullAt(pos)) {
                return null;
              }

              Object value = row.get(pos);
              if (value instanceof Row) {
                return toJava((Row) value);
              } else if (value instanceof scala.collection.Seq) {
                return row.getList(pos);
              } else if (value instanceof scala.collection.Map) {
                return row.getJavaMap(pos);
              } else {
                return value;
              }
            })
        .toArray(Object[]::new);
  }

  protected Object scalarSql(String query, Object... args) {
    List<Object[]> rows = sql(query, args);
    Assert.assertEquals("Scalar SQL should return one row", 1, rows.size());
    Object[] row = Iterables.getOnlyElement(rows);
    Assert.assertEquals("Scalar SQL should return one value", 1, row.length);
    return row[0];
  }

  protected Object[] row(Object... values) {
    return values;
  }

  protected void assertEquals(
      String context, List<Object[]> expectedRows, List<Object[]> actualRows) {
    Assert.assertEquals(
        context + ": number of results should match", expectedRows.size(), actualRows.size());
    for (int row = 0; row < expectedRows.size(); row += 1) {
      Object[] expected = expectedRows.get(row);
      Object[] actual = actualRows.get(row);
      Assert.assertEquals("Number of columns should match", expected.length, actual.length);
      for (int col = 0; col < actualRows.get(row).length; col += 1) {
        String newContext = String.format("%s: row %d col %d", context, row + 1, col + 1);
        assertEquals(newContext, expected, actual);
      }
    }
  }

  protected void assertEquals(String context, Object[] expectedRow, Object[] actualRow) {
    Assert.assertEquals("Number of columns should match", expectedRow.length, actualRow.length);
    for (int col = 0; col < actualRow.length; col += 1) {
      Object expectedValue = expectedRow[col];
      Object actualValue = actualRow[col];
      if (expectedValue != null && expectedValue.getClass().isArray()) {
        String newContext = String.format("%s (nested col %d)", context, col + 1);
        assertEquals(newContext, (Object[]) expectedValue, (Object[]) actualValue);
      } else if (expectedValue != ANY) {
        Assert.assertEquals(context + " contents should match", expectedValue, actualValue);
      }
    }
  }

  protected static String dbPath(String dbName) {
    return metastore.getDatabasePath(dbName);
  }

  protected void withSQLConf(Map<String, String> conf, Action action) {
    SQLConf sqlConf = SQLConf.get();

    Map<String, String> currentConfValues = Maps.newHashMap();
    conf.keySet()
        .forEach(
            confKey -> {
              if (sqlConf.contains(confKey)) {
                String currentConfValue = sqlConf.getConfString(confKey);
                currentConfValues.put(confKey, currentConfValue);
              }
            });

    conf.forEach(
        (confKey, confValue) -> {
          if (SQLConf.staticConfKeys().contains(confKey)) {
            throw new RuntimeException("Cannot modify the value of a static config: " + confKey);
          }
          sqlConf.setConfString(confKey, confValue);
        });

    try {
      action.invoke();
    } finally {
      conf.forEach(
          (confKey, confValue) -> {
            if (currentConfValues.containsKey(confKey)) {
              sqlConf.setConfString(confKey, currentConfValues.get(confKey));
            } else {
              sqlConf.unsetConf(confKey);
            }
          });
    }
  }

  private Map<Long, SQLExecutionUIData> currentExecutionUIDataMap() throws TimeoutException {
    spark.sparkContext().listenerBus().waitUntilEmpty(10000);
    return JavaConverters.seqAsJavaList(spark.sharedState().statusStore().executionsList()).stream()
        .collect(Collectors.toMap(data -> data.executionId(), data -> data));
  }

  protected void checkMetrics(Callable sparkCallable, Map<String, String> expectedMetrics)
      throws Exception {
    Set<Long> originalExecutionIds = currentExecutionUIDataMap().keySet();
    sparkCallable.call();
    Map<Long, SQLExecutionUIData> currentExecutions = currentExecutionUIDataMap();
    Set<Long> currentExecutionIds = currentExecutions.keySet();
    currentExecutionIds.removeAll(originalExecutionIds);
    Assert.assertEquals(currentExecutionIds.size(), 1);
    SQLExecutionUIData currentExecution =
        currentExecutions.get(currentExecutionIds.stream().findFirst().get());

    Map<Long, String> metricsIds = Maps.newHashMap();
    JavaConverters.seqAsJavaList(currentExecution.metrics()).stream()
        .forEach(
            metricsDeclaration -> {
              if (expectedMetrics.containsKey(metricsDeclaration.name())) {
                metricsIds.put(metricsDeclaration.accumulatorId(), metricsDeclaration.name());
              }
            });
    Assert.assertEquals(
        "Expected metric name not match",
        expectedMetrics.keySet(),
        Sets.newHashSet(metricsIds.values()));

    Map<Object, String> currentMetrics =
        JavaConverters.mapAsJavaMap(
                spark.sharedState().statusStore().executionMetrics(currentExecution.executionId()))
            .entrySet().stream()
            .filter(x -> metricsIds.containsKey(x.getKey()))
            .collect(Collectors.toMap(x -> metricsIds.get(x.getKey()), x -> x.getValue()));
    Assert.assertEquals("Expected metric value not match", expectedMetrics, currentMetrics);
  }

  @FunctionalInterface
  protected interface Action {
    void invoke();
  }
}
