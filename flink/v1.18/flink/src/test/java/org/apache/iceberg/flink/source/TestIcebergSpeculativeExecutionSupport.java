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
package org.apache.iceberg.flink.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SlowTaskDetectorOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.TestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TestIcebergSpeculativeExecutionSupport extends TestBase {
  private static final int NUM_TASK_MANAGERS = 1;
  private static final int NUM_TASK_SLOTS = 3;

  @RegisterExtension
  public static MiniClusterExtension miniClusterResource =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(NUM_TASK_MANAGERS)
              .setNumberSlotsPerTaskManager(NUM_TASK_SLOTS)
              .setConfiguration(configure())
              .build());

  private StreamTableEnvironment tEnv;
  private static final String CATALOG_NAME = "test_catalog";
  private static final String DATABASE_NAME = "test_db";
  private static final String INPUT_TABLE_NAME = "test_table";
  private static final String OUTPUT_TABLE_NAME = "sink_table";

  @Override
  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment(configure());
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        tEnv = StreamTableEnvironment.create(env);
      }
    }

    return tEnv;
  }

  @BeforeEach
  public void before() throws IOException {
    String warehouse =
        String.format("file:%s", Files.createTempDirectory(temporaryDirectory, "junit").toString());
    sql(
        "CREATE CATALOG %s WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')",
        CATALOG_NAME, warehouse);
    sql("USE CATALOG %s", CATALOG_NAME);
    sql("CREATE DATABASE %s", DATABASE_NAME);
    sql("USE %s", DATABASE_NAME);

    sql("CREATE TABLE %s (i INT, j INT)", INPUT_TABLE_NAME);
    sql("INSERT INTO %s VALUES (1, -1),(2, -1),(3, -1)", INPUT_TABLE_NAME);
    sql("CREATE TABLE %s (i INT, j INT, subTask INT, attempt INT)", OUTPUT_TABLE_NAME);
  }

  @AfterEach
  public void after() {
    sql("DROP TABLE IF EXISTS %s.%s", DATABASE_NAME, INPUT_TABLE_NAME);
    sql("DROP TABLE IF EXISTS %s.%s", DATABASE_NAME, OUTPUT_TABLE_NAME);
    dropDatabase(DATABASE_NAME, true);
    dropCatalog(CATALOG_NAME, true);
  }

  @Test
  public void testSpeculativeExecution() throws Exception {
    Table table =
        tEnv.sqlQuery(String.format("SELECT * FROM %s.%s", DATABASE_NAME, INPUT_TABLE_NAME));
    DataStream<Row> slowStream =
        tEnv.toDataStream(table, Row.class)
            .map(new TestingMap())
            .name("test_map")
            .returns(
                Types.ROW_NAMED(
                    new String[] {"i", "j", "subTask", "attempt"},
                    Types.INT,
                    Types.INT,
                    Types.INT,
                    Types.INT))
            .setParallelism(NUM_TASK_SLOTS);

    tEnv.fromDataStream(slowStream)
        .executeInsert(String.format("%s.%s", DATABASE_NAME, OUTPUT_TABLE_NAME))
        .await();

    List<Row> output = sql(String.format("SELECT * FROM %s.%s", DATABASE_NAME, OUTPUT_TABLE_NAME));

    // Ensure that all subTasks has attemptNum > 0
    assertThat(output.stream().map(x -> x.getField(3)).collect(Collectors.toSet())).contains(1);

    // Ensure the test_table rows are returned exactly the same after the slow map task from the
    // sink_table
    assertSameElements(
        output.stream().map(x -> Row.of(x.getField(0), x.getField(1))).collect(Collectors.toList()),
        Arrays.asList(Row.of(1, -1), Row.of(2, -1), Row.of(3, -1)));
  }

  /** A testing map function that simulates the slow task. */
  private static class TestingMap extends RichMapFunction<Row, Row> {
    @Override
    public Row map(Row row) throws Exception {
      // Put the even subtask indices with the first attempt to sleep to trigger speculative
      // execution
      if (getRuntimeContext().getAttemptNumber() <= 0) {
        Thread.sleep(Integer.MAX_VALUE);
      }

      Row output =
          Row.of(
              row.getField(0),
              row.getField(1),
              getRuntimeContext().getIndexOfThisSubtask(),
              getRuntimeContext().getAttemptNumber());

      return output;
    }
  }

  private static Configuration configure() {
    Configuration configuration = new Configuration();
    configuration.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);
    configuration.set(RestOptions.BIND_PORT, "0");
    configuration.set(JobManagerOptions.SLOT_REQUEST_TIMEOUT, 5000L);

    // Use FLIP-27 source
    configuration.set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_FLIP27_SOURCE, true);

    // for speculative execution
    configuration.set(BatchExecutionOptions.SPECULATIVE_ENABLED, true);

    configuration.set(SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_MULTIPLIER, 1.0);
    configuration.set(SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_RATIO, 0.2);
    configuration.set(
        SlowTaskDetectorOptions.EXECUTION_TIME_BASELINE_LOWER_BOUND, Duration.ofMillis(0));
    configuration.set(BatchExecutionOptions.BLOCK_SLOW_NODE_DURATION, Duration.ofMillis(0));

    return configuration;
  }
}
