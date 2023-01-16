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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.util.QueryExecutionListener;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

public abstract class SparkTestBase extends SparkTestHelperBase {

  protected static TestHiveMetastore metastore = null;
  protected static HiveConf hiveConf = null;
  protected static SparkSession spark = null;
  protected static JavaSparkContext sparkContext = null;
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
            .config("spark.sql.legacy.respectNullabilityInTextDatasetConversion", "true")
            .enableHiveSupport()
            .getOrCreate();

    SparkTestBase.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

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
      SparkTestBase.sparkContext = null;
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

  protected static String dbPath(String dbName) {
    return metastore.getDatabasePath(dbName);
  }

  protected void withUnavailableFiles(Iterable<? extends ContentFile<?>> files, Action action) {
    Iterable<String> fileLocations = Iterables.transform(files, file -> file.path().toString());
    withUnavailableLocations(fileLocations, action);
  }

  private void move(String location, String newLocation) {
    Path path = Paths.get(URI.create(location));
    Path tempPath = Paths.get(URI.create(newLocation));

    try {
      Files.move(path, tempPath);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to move: " + location, e);
    }
  }

  protected void withUnavailableLocations(Iterable<String> locations, Action action) {
    for (String location : locations) {
      move(location, location + "_temp");
    }

    try {
      action.invoke();
    } finally {
      for (String location : locations) {
        move(location + "_temp", location);
      }
    }
  }

  protected void withDefaultTimeZone(String zoneId, Action action) {
    TimeZone currentZone = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(zoneId));
      action.invoke();
    } finally {
      TimeZone.setDefault(currentZone);
    }
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
          if (SQLConf.isStaticConfigKey(confKey)) {
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

  protected Dataset<Row> jsonToDF(String schema, String... records) {
    Dataset<String> jsonDF = spark.createDataset(ImmutableList.copyOf(records), Encoders.STRING());
    return spark.read().schema(schema).json(jsonDF);
  }

  protected void append(String table, String... jsonRecords) {
    try {
      String schema = spark.table(table).schema().toDDL();
      Dataset<Row> df = jsonToDF(schema, jsonRecords);
      df.coalesce(1).writeTo(table).append();
    } catch (NoSuchTableException e) {
      throw new RuntimeException("Failed to write data", e);
    }
  }

  protected String tablePropsAsString(Map<String, String> tableProps) {
    StringBuilder stringBuilder = new StringBuilder();

    for (Map.Entry<String, String> property : tableProps.entrySet()) {
      if (stringBuilder.length() > 0) {
        stringBuilder.append(", ");
      }
      stringBuilder.append(String.format("'%s' '%s'", property.getKey(), property.getValue()));
    }

    return stringBuilder.toString();
  }

  protected SparkPlan executeAndKeepPlan(String query, Object... args) {
    return executeAndKeepPlan(() -> sql(query, args));
  }

  protected SparkPlan executeAndKeepPlan(Action action) {
    AtomicReference<SparkPlan> executedPlanRef = new AtomicReference<>();

    QueryExecutionListener listener =
        new QueryExecutionListener() {
          @Override
          public void onSuccess(String funcName, QueryExecution qe, long durationNs) {
            executedPlanRef.set(qe.executedPlan());
          }

          @Override
          public void onFailure(String funcName, QueryExecution qe, Exception exception) {}
        };

    spark.listenerManager().register(listener);

    action.invoke();

    try {
      spark.sparkContext().listenerBus().waitUntilEmpty();
    } catch (TimeoutException e) {
      throw new RuntimeException("Timeout while waiting for processing events", e);
    }

    SparkPlan executedPlan = executedPlanRef.get();
    if (executedPlan instanceof AdaptiveSparkPlanExec) {
      return ((AdaptiveSparkPlanExec) executedPlan).executedPlan();
    } else {
      return executedPlan;
    }
  }

  @FunctionalInterface
  protected interface Action {
    void invoke();
  }
}
