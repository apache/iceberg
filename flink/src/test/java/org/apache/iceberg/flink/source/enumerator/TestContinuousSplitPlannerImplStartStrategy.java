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

package org.apache.iceberg.flink.source.enumerator;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestContinuousSplitPlannerImplStartStrategy {

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final FileFormat fileFormat = FileFormat.PARQUET;
  private static final ScanContext scanContext = ScanContext.builder()
      .project(TestFixtures.SCHEMA)
      .build();

  private static String warehouse;
  private static HadoopCatalog catalog;
  private static Table table;
  private static GenericAppenderHelper dataAppender;

  private static Snapshot snapshot1;
  private static Snapshot snapshot2;
  private static Snapshot snapshot3;

  @BeforeClass
  public static void beforeClass() throws IOException {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    // before variables
    warehouse = "file:" + warehouseFile;
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    catalog = new HadoopCatalog(hadoopConf, warehouse);
    table = catalog.createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);
    dataAppender = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);

    // snapshot1
    final List<Record> batch1 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(batch1);
    snapshot1 = table.currentSnapshot();
    // snapshot2
    final List<Record> batch2 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(batch2);
    snapshot2 = table.currentSnapshot();
    // snapshot3
    final List<Record> batch3 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, 0L);
    dataAppender.appendToTable(batch3);
    snapshot3 = table.currentSnapshot();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    catalog.dropTable(TestFixtures.TABLE_IDENTIFIER);
    catalog.close();
  }

  @Test
  public void testStartSnapshotIdForTableScanThenIncrementalStrategy() {
    final IcebergEnumeratorConfig config = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMinutes(5L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
        .build();
    final ContinuousSplitPlannerImpl splitPlanner = new ContinuousSplitPlannerImpl(
        table, config, scanContext);
    Assert.assertEquals(snapshot3.snapshotId(),
        splitPlanner.getStartSnapshot(table, config).snapshotId());
  }

  @Test
  public void testStartSnapshotIdForLatestSnapshotStrategy() {
    final IcebergEnumeratorConfig config = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMinutes(5L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.LATEST_SNAPSHOT)
        .build();
    final ContinuousSplitPlannerImpl splitPlanner = new ContinuousSplitPlannerImpl(
        table, config, scanContext);
    Assert.assertEquals(snapshot3.snapshotId(),
        splitPlanner.getStartSnapshot(table, config).snapshotId());
  }

  @Test
  public void testStartSnapshotIdForEarliestSnapshotStrategy() {
    final IcebergEnumeratorConfig config = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMinutes(5L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.EARLIEST_SNAPSHOT)
        .build();
    final ContinuousSplitPlannerImpl splitPlanner = new ContinuousSplitPlannerImpl(
        table, config, scanContext);
    Assert.assertEquals(snapshot1.snapshotId(),
        splitPlanner.getStartSnapshot(table, config).snapshotId());
  }

  @Test
  public void testStartSnapshotIdForSpecificSnapshotIdStrategy() {
    final IcebergEnumeratorConfig config = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMinutes(5L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.SPECIFIC_START_SNAPSHOT_ID)
        .startSnapshotId(snapshot2.snapshotId())
        .build();
    final ContinuousSplitPlannerImpl splitPlanner = new ContinuousSplitPlannerImpl(
        table, config, scanContext);
    Assert.assertEquals(snapshot2.snapshotId(),
        splitPlanner.getStartSnapshot(table, config).snapshotId());
  }

  @Test
  public void testStartSnapshotIdForSpecificSnapshotTimestampStrategySnapshot2() {
    final IcebergEnumeratorConfig config = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMinutes(5L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.SPECIFIC_START_SNAPSHOT_TIMESTAMP)
        .startSnapshotTimeMs(snapshot2.timestampMillis())
        .build();
    final ContinuousSplitPlannerImpl splitPlanner = new ContinuousSplitPlannerImpl(
        table, config, scanContext);
    Assert.assertEquals(snapshot2.snapshotId(),
        splitPlanner.getStartSnapshot(table, config).snapshotId());
  }

  @Test
  public void testStartSnapshotIdForSpecificSnapshotTimestampStrategySnapshot2Minus1() {
    final IcebergEnumeratorConfig config = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMinutes(5L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.SPECIFIC_START_SNAPSHOT_TIMESTAMP)
        .startSnapshotTimeMs(snapshot2.timestampMillis() - 1L)
        .build();
    final ContinuousSplitPlannerImpl splitPlanner = new ContinuousSplitPlannerImpl(
        table, config, scanContext);
    Assert.assertEquals(snapshot1.snapshotId(),
        splitPlanner.getStartSnapshot(table, config).snapshotId());
  }

}
