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

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.spark.broadcastvar.expressions.RangeInTestUtils;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Assert;
import org.junit.Test;

public class TestMetadataTableScansWithRangeIn extends MetadataTableScanTestBase {
  public TestMetadataTableScansWithRangeIn(int formatVersion) {
    super(formatVersion);
  }

  private void preparePartitionedTable() {
    preparePartitionedTableData();

    if (formatVersion == 2) {
      table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
      table.newRowDelta().addDeletes(FILE_B_DELETES).commit();
      table.newRowDelta().addDeletes(FILE_C2_DELETES).commit();
      table.newRowDelta().addDeletes(FILE_D2_DELETES).commit();
    }
  }

  private void preparePartitionedTableData() {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_C).commit();
    table.newFastAppend().appendFile(FILE_D).commit();
    table.newFastAppend().appendFile(FILE_B).commit();
  }

  @Test
  public void testPartitionsTableScanInFilter() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table);

    Expression set = RangeInTestUtils.createPredicate("partition.data_bucket", new Object[] {2, 3});
    TableScan scanSet = partitionsTable.newScan().filter(set);
    CloseableIterable<ManifestEntry<?>> entries =
        PartitionsTable.planEntries((StaticTableScan) scanSet);
    if (formatVersion == 2) {
      Assert.assertEquals(4, Iterators.size(entries.iterator()));
    } else {
      Assert.assertEquals(2, Iterators.size(entries.iterator()));
    }

    validateSingleFieldPartition(entries, 2);
    validateSingleFieldPartition(entries, 3);
  }

  @Test
  public void testAllManifestsTableSnapshotIn() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table);

    TableScan manifestsTableScan =
        manifestsTable.newScan().filter(RangeInTestUtils.<Long>createPredicate(
            "reference_snapshot_id", new Object[] {1L, 3L}, DataTypes.LongType));
    Assert.assertEquals(
        "Expected snapshots do not match",
        expectedManifestListPaths(table.snapshots(), 1L, 3L),
        actualManifestListPaths(manifestsTableScan));
  }
}
