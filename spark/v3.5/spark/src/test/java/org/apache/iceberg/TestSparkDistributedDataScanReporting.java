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

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkDistributedDataScanReporting
    extends ScanPlanningAndReportingTestBase<BatchScan, ScanTask, ScanTaskGroup<ScanTask>> {

  @Parameters(name = "formatVersion = {0}, dataMode = {1}, deleteMode = {2}")
  public static List<Object> parameters() {
    return Arrays.asList(
        new Object[] {2, LOCAL, LOCAL},
        new Object[] {2, LOCAL, DISTRIBUTED},
        new Object[] {2, DISTRIBUTED, LOCAL},
        new Object[] {2, DISTRIBUTED, DISTRIBUTED});
  }

  private static SparkSession spark = null;

  @Parameter(index = 1)
  private PlanningMode dataMode;

  @Parameter(index = 2)
  private PlanningMode deleteMode;

  @BeforeAll
  public static void startSpark() {
    TestSparkDistributedDataScanReporting.spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config(SQLConf.SHUFFLE_PARTITIONS().key(), "4")
            .getOrCreate();
  }

  @AfterAll
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
