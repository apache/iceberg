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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSparkDistributedDataScanReporting
    extends ScanPlanningAndReportingTestBase<BatchScan, ScanTask, ScanTaskGroup<ScanTask>> {

  @Parameterized.Parameters(name = "dataMode = {0}, deleteMode = {1}")
  public static Object[] parameters() {
    return new Object[][] {
      new Object[] {LOCAL, LOCAL},
      new Object[] {LOCAL, DISTRIBUTED},
      new Object[] {DISTRIBUTED, LOCAL},
      new Object[] {DISTRIBUTED, DISTRIBUTED}
    };
  }

  private static SparkSession spark = null;

  private final PlanningMode dataMode;
  private final PlanningMode deleteMode;

  public TestSparkDistributedDataScanReporting(
      PlanningMode dataPlanningMode, PlanningMode deletePlanningMode) {
    this.dataMode = dataPlanningMode;
    this.deleteMode = deletePlanningMode;
  }

  @BeforeClass
  public static void startSpark() {
    TestSparkDistributedDataScanReporting.spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config(SQLConf.SHUFFLE_PARTITIONS().key(), "4")
            .getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestSparkDistributedDataScanReporting.spark;
    TestSparkDistributedDataScanReporting.spark = null;
    currentSpark.stop();
  }

  @Override
  protected BatchScan newScan(Table table) {
    table
        .updateProperties()
        .set(TableProperties.DATA_PLANNING_MODE, dataMode.modeName())
        .set(TableProperties.DELETE_PLANNING_MODE, deleteMode.modeName())
        .commit();
    SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
    return new SparkDistributedDataScan(spark, table, readConf);
  }
}
