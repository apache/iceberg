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

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSetStatistics extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  public TestSetStatistics(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testEmptyUpdateStatistics() {
    Assert.assertEquals("Table should be on version 0", 0, (int) version());
    TableMetadata base = readMetadata();

    table.updateStatistics().commit();

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, table.ops().current());
    Assert.assertEquals("Table should be on version 1", 1, (int) version());
  }

  @Test
  public void testEmptyTransactionalUpdateStatistics() {
    Assert.assertEquals("Table should be on version 0", 0, (int) version());
    TableMetadata base = readMetadata();

    Transaction transaction = table.newTransaction();
    transaction.updateStatistics().commit();
    transaction.commitTransaction();

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, table.ops().current());
    Assert.assertEquals("Table should be on version 0", 0, (int) version());
  }

  @Test
  public void testUpdateStatistics() {
    // Create a snapshot
    table.newFastAppend().commit();
    Assert.assertEquals("Table should be on version 1", 1, (int) version());

    TableMetadata base = readMetadata();
    long snapshotId = base.currentSnapshot().snapshotId();
    GenericStatisticsFile statisticsFile =
        new GenericStatisticsFile(
            snapshotId,
            "/some/statistics/file.puffin",
            100,
            42,
            ImmutableList.of(
                new GenericBlobMetadata(
                    "stats-type",
                    snapshotId,
                    base.lastSequenceNumber(),
                    ImmutableList.of(1, 2),
                    ImmutableMap.of("a-property", "some-property-value"))));

    table.updateStatistics().setStatistics(snapshotId, statisticsFile).commit();

    TableMetadata metadata = readMetadata();
    Assert.assertEquals("Table should be on version 2", 2, (int) version());
    Assert.assertEquals(
        "Table snapshot should be the same after setting statistics file",
        snapshotId,
        metadata.currentSnapshot().snapshotId());
    Assert.assertEquals(
        "Table metadata should have statistics files",
        ImmutableList.of(statisticsFile),
        metadata.statisticsFiles());
  }

  @Test
  public void testRemoveStatistics() {
    // Create a snapshot
    table.newFastAppend().commit();
    Assert.assertEquals("Table should be on version 1", 1, (int) version());

    TableMetadata base = readMetadata();
    long snapshotId = base.currentSnapshot().snapshotId();
    GenericStatisticsFile statisticsFile =
        new GenericStatisticsFile(
            snapshotId, "/some/statistics/file.puffin", 100, 42, ImmutableList.of());

    table.updateStatistics().setStatistics(snapshotId, statisticsFile).commit();

    TableMetadata metadata = readMetadata();
    Assert.assertEquals("Table should be on version 2", 2, (int) version());
    Assert.assertEquals(
        "Table metadata should have statistics files",
        ImmutableList.of(statisticsFile),
        metadata.statisticsFiles());

    table.updateStatistics().removeStatistics(snapshotId).commit();

    metadata = readMetadata();
    Assert.assertEquals("Table should be on version 3", 3, (int) version());
    Assert.assertEquals(
        "Table metadata should have no statistics files",
        ImmutableList.of(),
        metadata.statisticsFiles());
  }
}
