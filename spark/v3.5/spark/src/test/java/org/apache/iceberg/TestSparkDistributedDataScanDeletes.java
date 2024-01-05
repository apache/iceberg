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
package org.apache.iceberg;

import static org.apache.iceberg.PlanningMode.DISTRIBUTED;
import static org.apache.iceberg.PlanningMode.LOCAL;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkDistributedDataScanDeletes
    extends DeleteFileIndexTestBase<BatchScan, ScanTask, ScanTaskGroup<ScanTask>> {

  @Parameters(
      name =
          "formatVersion = {0}, V1Assert = {1}, V2Assert = {2}, dataMode = {3}, deleteMode = {4}")
  public static Object[][] parameters() {
    return new Object[][] {
      {2, new TableAssertions(1, 2), new TableAssertions(2, 2), LOCAL, LOCAL},
      {2, new TableAssertions(1, 2), new TableAssertions(2, 2), LOCAL, DISTRIBUTED},
      {2, new TableAssertions(1, 2), new TableAssertions(2, 2), DISTRIBUTED, LOCAL},
      {2, new TableAssertions(1, 2), new TableAssertions(2, 2), LOCAL, DISTRIBUTED}
    };
  }

  private static SparkSession spark = null;

  @Parameter(index = 3)
  private PlanningMode dataMode;

  @Parameter(index = 4)
  private PlanningMode deleteMode;

  @BeforeEach
  public void configurePlanningModes() {
    table
        .updateProperties()
        .set(TableProperties.DATA_PLANNING_MODE, dataMode.modeName())
        .set(TableProperties.DELETE_PLANNING_MODE, deleteMode.modeName())
        .commit();
  }

  @BeforeAll
  public static void startSpark() {
    TestSparkDistributedDataScanDeletes.spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config(SQLConf.SHUFFLE_PARTITIONS().key(), "4")
            .getOrCreate();
  }

  @AfterAll
  public static void stopSpark() {
    SparkSession currentSpark = TestSparkDistributedDataScanDeletes.spark;
    TestSparkDistributedDataScanDeletes.spark = null;
    currentSpark.stop();
  }

  @Override
  protected BatchScan newScan(Table table) {
    SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
    return new SparkDistributedDataScan(spark, table, readConf);
  }
}
