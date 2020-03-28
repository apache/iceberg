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

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestEntriesMetadataTable extends TableTestBase {

  @Test
  public void testEntriesTable() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table entriesTable = new ManifestEntriesTable(table.ops(), table);

    Schema expectedSchema = ManifestEntry.getSchema(table.spec().partitionType());

    assertEquals("A tableScan.select() should prune the schema",
        expectedSchema.asStruct(),
        entriesTable.schema().asStruct());
  }

  @Test
  public void testEntriesTableScan() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table entriesTable = new ManifestEntriesTable(table.ops(), table);
    TableScan scan = entriesTable.newScan();

    Schema expectedSchema = ManifestEntry.getSchema(table.spec().partitionType());

    assertEquals("A tableScan.select() should prune the schema",
        expectedSchema.asStruct(),
        scan.schema().asStruct());

    FileScanTask file = Iterables.getOnlyElement(scan.planFiles());
    Assert.assertEquals("Data file should be the table's manifest",
        Iterables.getOnlyElement(table.currentSnapshot().manifests()).path(), file.file().path());
    Assert.assertEquals("Should contain 2 data file records", 2, file.file().recordCount());
  }

  @Test
  public void testSplitPlanningWithSplitSizeOption() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    int splitSize = 2 * 1024; // 2 KB split size

    table.updateProperties()
        .set(TableProperties.METADATA_SPLIT_SIZE, String.valueOf(2 * splitSize))
        .commit();

    Table entriesTable = new ManifestEntriesTable(table.ops(), table);
    Assert.assertEquals(1, entriesTable.currentSnapshot().manifests().size());

    int expectedSplits =
        ((int) entriesTable.currentSnapshot().manifests().get(0).length() + splitSize - 1) / splitSize;

    TableScan scan = entriesTable.newScan()
        .option(TableProperties.SPLIT_SIZE, String.valueOf(splitSize));

    Assert.assertEquals(expectedSplits, Iterables.size(scan.planTasks()));
  }

  @Test
  public void testSplitPlanningWithMetadataSplitSizeProperty() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    int splitSize = 2 * 1024; // 2 KB split size

    table.updateProperties()
        .set(TableProperties.METADATA_SPLIT_SIZE, String.valueOf(splitSize))
        .commit();

    Table entriesTable = new ManifestEntriesTable(table.ops(), table);
    Assert.assertEquals(1, entriesTable.currentSnapshot().manifests().size());

    int expectedSplits =
        ((int) entriesTable.currentSnapshot().manifests().get(0).length() + splitSize - 1) / splitSize;

    TableScan scan = entriesTable.newScan();

    Assert.assertEquals(expectedSplits, Iterables.size(scan.planTasks()));
  }

  @Test
  public void testSplitPlanningWithDefaultMetadataSplitSize() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    int splitSize = (int) TableProperties.METADATA_SPLIT_SIZE_DEFAULT; // default split size is 32 MB

    Table entriesTable = new ManifestEntriesTable(table.ops(), table);
    Assert.assertEquals(1, entriesTable.currentSnapshot().manifests().size());

    int expectedSplits =
        ((int) entriesTable.currentSnapshot().manifests().get(0).length() + splitSize - 1) / splitSize;

    TableScan scan = entriesTable.newScan();

    Assert.assertEquals(expectedSplits, Iterables.size(scan.planTasks()));
  }

}
