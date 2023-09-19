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
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public abstract class SparkDistributedDataScanTestBase
    extends DataTableScanTestBase<BatchScan, ScanTask, ScanTaskGroup<ScanTask>> {

  @Parameters(name = "formatVersion = {0}, dataMode = {1}, deleteMode = {2}")
  public static Object[] parameters() {
    return new Object[][] {
      new Object[] {1, LOCAL, LOCAL},
      new Object[] {1, LOCAL, DISTRIBUTED},
      new Object[] {1, DISTRIBUTED, LOCAL},
      new Object[] {1, DISTRIBUTED, DISTRIBUTED},
      new Object[] {2, LOCAL, LOCAL},
      new Object[] {2, LOCAL, DISTRIBUTED},
      new Object[] {2, DISTRIBUTED, LOCAL},
      new Object[] {2, DISTRIBUTED, DISTRIBUTED}
    };
  }

  protected static SparkSession spark = null;

  private final PlanningMode dataMode;
  private final PlanningMode deleteMode;

  public SparkDistributedDataScanTestBase(
      int formatVersion, PlanningMode dataPlanningMode, PlanningMode deletePlanningMode) {
    super(formatVersion);
    this.dataMode = dataPlanningMode;
    this.deleteMode = deletePlanningMode;
  }

  @Before
  public void configurePlanningModes() {
    table
        .updateProperties()
        .set(TableProperties.DATA_PLANNING_MODE, dataMode.modeName())
        .set(TableProperties.DELETE_PLANNING_MODE, deleteMode.modeName())
        .commit();
  }

  @Override
  protected BatchScan useRef(BatchScan scan, String ref) {
    return scan.useRef(ref);
  }

  @Override
  protected BatchScan useSnapshot(BatchScan scan, long snapshotId) {
    return scan.useSnapshot(snapshotId);
  }

  @Override
  protected BatchScan asOfTime(BatchScan scan, long timestampMillis) {
    return scan.asOfTime(timestampMillis);
  }

  @Override
  protected BatchScan newScan() {
    SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
    return new SparkDistributedDataScan(spark, table, readConf);
  }

  protected static SparkSession initSpark(String serializer) {
    return SparkSession.builder()
        .master("local[2]")
        .config("spark.serializer", serializer)
        .config(SQLConf.SHUFFLE_PARTITIONS().key(), "4")
        .getOrCreate();
  }
}
