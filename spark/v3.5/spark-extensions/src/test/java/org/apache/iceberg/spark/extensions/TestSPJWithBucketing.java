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

import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSPJWithBucketing extends ExtensionsTestBase {

  private String sourceTableName() {
    return tablePrefix() + ".source";
  }

  private String targetTableName() {
    return tablePrefix() + ".target";
  }

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties(),
      },
    };
  }

  @BeforeEach
  public void before() {
    super.before();
    sql("USE %s", catalogName);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", targetTableName());
    sql("DROP TABLE IF EXISTS %s", sourceTableName());
  }

  @TestTemplate
  public void testMergeSPJwithCondition() {
    testWithCondition(
        "  AND ("
            + "(t.year_month='202306' AND t.day='01' AND testhive.system.bucket(4, t.id) = 0) OR\n"
            + "(t.year_month='202306' AND t.day='01' AND testhive.system.bucket(4, t.id) = 1) OR\n"
            + "(t.year_month='202306' AND t.day='02' AND testhive.system.bucket(4, t.id) = 0) OR\n"
            + "(t.year_month='202307' AND t.day='01' AND testhive.system.bucket(4, t.id) = 3)\n"
            + ")");
  }

  @TestTemplate
  public void testMergeSPJwithoutCondition() {
    testWithCondition("");
  }

  private void testWithCondition(String condition) {
    createPartitionedTable(targetTableName());
    insertRecords(targetTableName());
    createPartitionedTable(sourceTableName());
    insertRecordsToUpdate(sourceTableName());
    int tasks =
        executeAndCountTasks(
            spark,
            (s) ->
                withSQLConf(
                    ENABLED_SPJ_SQL_CONF,
                    () ->
                        // id STRING, year_month STRING, day STRING, data STRING
                        sql(
                            s,
                            "MERGE INTO %s t USING (SELECT * FROM %s) s \n"
                                + "ON t.id = s.id AND t.year_month = s.year_month AND t.day = s.day\n"
                                + "%s\n"
                                + "WHEN MATCHED THEN UPDATE SET\n"
                                + "  t.data = s.data\n"
                                + "WHEN NOT MATCHED THEN INSERT *",
                            targetTableName(),
                            sourceTableName(),
                            condition)));
    long affectedPartitions =
            sql(
                    spark,
                    "SELECT DISTINCT(t.year_month, t.day, testhive.system.bucket(4, t.id)) FROM %s t WHERE 1=1 %s",
                    targetTableName(),
                    condition)
                    .count();
    int shufflePartitions = Integer.parseInt(spark.conf().get("spark.sql.shuffle.partitions"));
    Assertions.assertThat(tasks).isEqualTo(affectedPartitions * 2 + shufflePartitions);
  }

  private static Dataset<Row> sql(SparkSession sparkSession, String sqlFormat, Object... args) {
    return sparkSession.sql(String.format(sqlFormat, args));
  }

  private static void createTable(String tName, String partitionCol) {
    sql(
        spark,
        "CREATE TABLE %s (id STRING, year_month STRING, day STRING, data STRING) USING iceberg PARTITIONED BY (%s)",
        tName,
        partitionCol);
    sql(spark, "ALTER TABLE %s SET TBLPROPERTIES ('write.merge.distribution-mode'='none')", tName);
  }

  private static void insertRecords(String tName) {
    sql(
        spark,
        "INSERT INTO TABLE %s VALUES %s",
        tName,
        String.join(
            ", ",
            "('3', '202306', '01', 'data-0')", // 202306/01/0
            "('9', '202306', '01', 'data-0')", // 202306/01/1
            "('11', '202306', '01', 'data-0')", // 202306/01/2
            "('0', '202306', '01', 'data-0')", // 202306/01/3
            "('3', '202306', '02', 'data-0')", // 202306/02/0
            "('9', '202306', '02', 'data-0')", // 202306/02/1
            "('0', '202307', '01', 'data-0')" // 202307/01/3
            ));
  }

  private static void insertRecordsToUpdate(String tName) {
    sql(
        spark,
        "INSERT INTO TABLE %s VALUES %s",
        tName,
        String.join(
            ", ",
            "('3', '202306', '01', 'data-1')", // 202306/01/0
            "('9', '202306', '01', 'data-1')", // 202306/01/1
            "('3', '202306', '02', 'data-1')", // 202306/02/0
            "('0', '202307', '01', 'data-1')" // 202307/01/3
            ));
  }

  private static void createPartitionedTable(String tName) {
    createTable(tName, "year_month, day, bucket(4, id)");
  }

  private String tablePrefix() {
    return (catalogName.equals("spark_catalog") ? "" : catalogName + ".") + "default";
  }

  private static final Map<String, String> ENABLED_SPJ_SQL_CONF =
      ImmutableMap.of(
          SQLConf.V2_BUCKETING_ENABLED().key(),
          "true",
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED().key(),
          "true",
          SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION().key(),
          "false",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(),
          "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD().key(),
          "-1",
          SparkSQLProperties.PRESERVE_DATA_GROUPING,
          "true");

  public static int executeAndCountTasks(SparkSession spark, Consumer<SparkSession> f) {

    CountTaskListener listener = new CountTaskListener();
    spark.sparkContext().addSparkListener(listener);

    f.accept(spark);

    try {
      spark.sparkContext().listenerBus().waitUntilEmpty();
    } catch (TimeoutException e) {
      throw new RuntimeException("Timeout while waiting for processing events", e);
    }

    return listener.getTaskCount();
  }

  private static class CountTaskListener extends SparkListener {
    private final AtomicInteger tasks = new AtomicInteger(0);

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
      tasks.incrementAndGet();
    }

    public int getTaskCount() {
      return tasks.get();
    }
  }
}
