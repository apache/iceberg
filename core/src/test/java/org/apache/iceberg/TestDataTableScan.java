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

import java.util.List;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class TestDataTableScan extends ScanTestBase<TableScan, FileScanTask, CombinedScanTask> {
  public TestDataTableScan(int formatVersion) {
    super(formatVersion);
  }

  @Override
  protected TableScan newScan() {
    return table.newScan();
  }

  @Test
  public void testTaskRowCounts() {
    Assume.assumeTrue(formatVersion == 2);

    DataFile dataFile1 = newDataFile("data_bucket=0");
    table.newFastAppend().appendFile(dataFile1).commit();

    DataFile dataFile2 = newDataFile("data_bucket=1");
    table.newFastAppend().appendFile(dataFile2).commit();

    DeleteFile deleteFile1 = newDeleteFile("data_bucket=0");
    table.newRowDelta().addDeletes(deleteFile1).commit();

    DeleteFile deleteFile2 = newDeleteFile("data_bucket=1");
    table.newRowDelta().addDeletes(deleteFile2).commit();

    TableScan scan = table.newScan().option(TableProperties.SPLIT_SIZE, String.valueOf(50));

    List<FileScanTask> fileScanTasks = Lists.newArrayList(scan.planFiles());
    Assert.assertEquals("Must have 2 FileScanTasks", 2, fileScanTasks.size());
    for (FileScanTask task : fileScanTasks) {
      Assert.assertEquals("Rows count must match", 10, task.rowsCount());
    }

    List<CombinedScanTask> combinedScanTasks = Lists.newArrayList(scan.planTasks());
    Assert.assertEquals("Must have 4 CombinedScanTask", 4, combinedScanTasks.size());
    for (CombinedScanTask task : combinedScanTasks) {
      Assert.assertEquals("Rows count must match", 5, task.rowsCount());
    }
  }

  protected DataFile newDataFile(String partitionPath) {
    return DataFiles.builder(table.spec())
        .withPath("/path/to/data-" + UUID.randomUUID() + ".parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(100)
        .withPartitionPath(partitionPath)
        .withRecordCount(10)
        .build();
  }

  protected DeleteFile newDeleteFile(String partitionPath) {
    return FileMetadata.deleteFileBuilder(table.spec())
        .ofPositionDeletes()
        .withPath("/path/to/delete-" + UUID.randomUUID() + ".parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(100)
        .withPartitionPath(partitionPath)
        .withRecordCount(10)
        .build();
  }
}
