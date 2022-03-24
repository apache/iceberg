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

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.HadoopTableResource;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class TestContinuousSplitPlannerImpl {
  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final FileFormat fileFormat = FileFormat.PARQUET;
  private static final AtomicLong randomSeed = new AtomicLong();

  @Rule
  public final HadoopTableResource tableResource = new HadoopTableResource(TEMPORARY_FOLDER,
      TestFixtures.DATABASE, TestFixtures.TABLE, TestFixtures.SCHEMA);

  @Rule
  public TestName testName = new TestName();

  private GenericAppenderHelper dataAppender;

  @Before
  public void before() throws IOException {
    dataAppender = new GenericAppenderHelper(tableResource.table(), fileFormat, TEMPORARY_FOLDER);
  }

  @Test
  public void testStartWithEmptyTable() throws Exception {
    ScanContext scanContext = ScanContext.builder()
        .streaming(true)
        .monitorInterval(Duration.ofMillis(10L))
        .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
        .build();
    ContinuousSplitPlannerImpl splitPlanner = new ContinuousSplitPlannerImpl(
        tableResource.table(), scanContext, testName.getMethodName());

    // empty table
    ContinuousEnumerationResult emptyTableResult = splitPlanner.planSplits(null);
    Assert.assertNull(emptyTableResult.position());
    Assert.assertTrue(emptyTableResult.splits().isEmpty());

    // next 3 snapshots
    IcebergEnumeratorPosition lastPosition = emptyTableResult.position();
    for (int i = 0; i < 3; ++i) {
      lastPosition = verifyOneCycle(splitPlanner, lastPosition);
    }
  }

  @Test
  public void testStartWithSomeData() throws Exception {
    List<Record> batch = RandomGenericData.generate(TestFixtures.SCHEMA, 2, randomSeed.incrementAndGet());
    DataFile dataFile = dataAppender.writeFile(null, batch);
    dataAppender.appendToTable(dataFile);
    Snapshot snapshot = tableResource.table().currentSnapshot();

    ScanContext scanContext = ScanContext.builder()
        .streaming(true)
        .monitorInterval(Duration.ofMillis(10L))
        .startingStrategy(StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
        .build();
    ContinuousSplitPlannerImpl splitPlanner = new ContinuousSplitPlannerImpl(
        tableResource.table(), scanContext, testName.getMethodName());

    ContinuousEnumerationResult initialResult = splitPlanner.planSplits(null);
    Assert.assertEquals(snapshot.snapshotId(), initialResult.position().endSnapshotId());
    Assert.assertEquals(1, initialResult.splits().size());
    IcebergSourceSplit split1 = initialResult.splits().iterator().next();
    Assert.assertEquals(1, split1.task().files().size());
    Assert.assertEquals(dataFile.path().toString(), split1.task().files().iterator().next().file().path().toString());

    IcebergEnumeratorPosition lastPosition = initialResult.position();
    for (int i = 0; i < 3; ++i) {
      lastPosition = verifyOneCycle(splitPlanner, lastPosition);
    }
  }

  /**
   * @return the last enumerated snapshot id
   */
  private IcebergEnumeratorPosition verifyOneCycle(
      ContinuousSplitPlannerImpl splitPlanner, IcebergEnumeratorPosition lastPosition) throws Exception {
    List<Record> batch = RandomGenericData.generate(TestFixtures.SCHEMA, 2, randomSeed.incrementAndGet());
    DataFile dataFile = dataAppender.writeFile(null, batch);
    dataAppender.appendToTable(dataFile);
    Snapshot snapshot = tableResource.table().currentSnapshot();

    ContinuousEnumerationResult result = splitPlanner.planSplits(lastPosition);
    Assert.assertEquals(snapshot.snapshotId(), result.position().endSnapshotId());
    Assert.assertEquals(1, result.splits().size());
    IcebergSourceSplit split = result.splits().iterator().next();
    Assert.assertEquals(1, split.task().files().size());
    Assert.assertEquals(dataFile.path().toString(), split.task().files().iterator().next().file().path().toString());
    return result.position();
  }
}
