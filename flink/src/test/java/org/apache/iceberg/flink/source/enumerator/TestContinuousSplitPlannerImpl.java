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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestContinuousSplitPlannerImpl {
  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final FileFormat fileFormat = FileFormat.PARQUET;
  private static final ScanContext scanContext = ScanContext.builder()
      .project(TestFixtures.SCHEMA)
      .build();
  private static final AtomicLong randomSeed = new AtomicLong();

  private String warehouse;
  private HadoopCatalog catalog;
  private Table table;
  private GenericAppenderHelper dataAppender;
  private DataFile dataFile1;
  private Snapshot snapshot1;

  @Before
  public void before() throws IOException {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    // before variables
    warehouse = "file:" + warehouseFile;
    org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
    catalog = new HadoopCatalog(hadoopConf, warehouse);
    table = catalog.createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);
    dataAppender = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);

    final List<Record> batch1 = RandomGenericData.generate(TestFixtures.SCHEMA, 2, randomSeed.incrementAndGet());
    dataFile1 = dataAppender.writeFile(null, batch1);
    dataAppender.appendToTable(dataFile1);
    snapshot1 = table.currentSnapshot();
  }

  @After
  public void after() throws IOException {
    catalog.dropTable(TestFixtures.TABLE_IDENTIFIER);
    catalog.close();
  }

  @Test
  public void testContinuousEnumerator() throws Exception {
    final IcebergEnumeratorConfig config = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMinutes(5L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
        .build();
    final ContinuousSplitPlannerImpl splitPlanner = new ContinuousSplitPlannerImpl(
        table, config, scanContext);

    SplitPlanningResult result1 = splitPlanner.planSplits(Optional.empty());
    Assert.assertEquals(snapshot1.snapshotId(), result1.lastEnumeratedSnapshotId());
    Assert.assertEquals(1, result1.splits().size());
    final IcebergSourceSplit split1 = result1.splits().iterator().next();
    Assert.assertEquals(1, split1.task().files().size());
    Assert.assertEquals(dataFile1.path().toString(), split1.task().files().iterator().next().file().path().toString());

    long lastEnumeratedSnapshotId = result1.lastEnumeratedSnapshotId();
    for (int i = 0; i < 3; ++i) {
      lastEnumeratedSnapshotId = verifyOneCycle(splitPlanner, lastEnumeratedSnapshotId);
    }
  }

  /**
   * @return the last enumerated snapshot id
   */
  private long verifyOneCycle(ContinuousSplitPlannerImpl splitPlanner, long lastEnumeratedSnapshotId) throws Exception {
    final List<Record> batch = RandomGenericData.generate(TestFixtures.SCHEMA, 2, randomSeed.incrementAndGet());
    final DataFile dataFile = dataAppender.writeFile(null, batch);
    dataAppender.appendToTable(dataFile);
    final Snapshot snapshot = table.currentSnapshot();

    SplitPlanningResult result = splitPlanner.planSplits(Optional.of(lastEnumeratedSnapshotId));
    Assert.assertEquals(snapshot.snapshotId(), result.lastEnumeratedSnapshotId());
    Assert.assertEquals(1, result.splits().size());
    final IcebergSourceSplit split = result.splits().iterator().next();
    Assert.assertEquals(1, split.task().files().size());
    Assert.assertEquals(dataFile.path().toString(), split.task().files().iterator().next().file().path().toString());
    return result.lastEnumeratedSnapshotId();
  }
}
