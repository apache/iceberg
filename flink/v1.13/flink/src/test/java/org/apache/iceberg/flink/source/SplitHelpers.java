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

package org.apache.iceberg.flink.source;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

public class SplitHelpers {

  private static final AtomicLong splitLengthIncrement = new AtomicLong();

  private SplitHelpers() {
  }

  /**
   * This create a list of IcebergSourceSplit from real files
   * <li>Create a new Hadoop table under the {@code temporaryFolder}
   * <li>write {@code fileCount} number of files to the new Iceberg table
   * <li>Discover the splits from the table and partition the splits by the {@code filePerSplit} limit
   * <li>Delete the Hadoop table
   *
   * Since the table and data files are deleted before this method return,
   * caller shouldn't attempt to read the data files.
   */
  public static List<IcebergSourceSplit> createSplitsFromTransientHadoopTable(
      TemporaryFolder temporaryFolder, int fileCount, int filesPerSplit) throws Exception {
    File warehouseFile = temporaryFolder.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    String warehouse = "file:" + warehouseFile;
    Configuration hadoopConf = new Configuration();
    HadoopCatalog catalog = new HadoopCatalog(hadoopConf, warehouse);
    try {
      Table table = catalog.createTable(TestFixtures.TABLE_IDENTIFIER, TestFixtures.SCHEMA);
      GenericAppenderHelper dataAppender = new GenericAppenderHelper(
          table, FileFormat.PARQUET, temporaryFolder);
      for (int i = 0; i < fileCount; ++i) {
        List<Record> records = RandomGenericData.generate(TestFixtures.SCHEMA, 2, i);
        dataAppender.appendToTable(records);
      }

      ScanContext scanContext = ScanContext.builder().build();
      List<IcebergSourceSplit> splits = FlinkSplitPlanner.planIcebergSourceSplits(table, scanContext);
      return splits.stream()
          .flatMap(split -> {
            List<List<FileScanTask>> filesList = Lists.partition(
                Lists.newArrayList(split.task().files()), filesPerSplit);
            return filesList.stream()
                .map(files ->  new BaseCombinedScanTask(files))
                .map(combinedScanTask -> IcebergSourceSplit.fromCombinedScanTask(combinedScanTask));
          })
          .collect(Collectors.toList());
    } finally {
      catalog.dropTable(TestFixtures.TABLE_IDENTIFIER);
      catalog.close();
    }
  }

  /**
   * Split planning doesn't guarantee the order is the same as appended. To make it simpler
   * to assert read and written records, this util sort the {@code FileScanTask} collection
   * in IcebergSourceSplit to match the order files are written.
   */
  public static IcebergSourceSplit sortFilesAsAppendOrder(IcebergSourceSplit split, List<DataFile> dataFiles) {
    Collection<FileScanTask> files = split.task().files();
    Assert.assertEquals(files.size(), dataFiles.size());
    FileScanTask[] sortedFileArray = new FileScanTask[files.size()];
    for (FileScanTask fileScanTask : files) {
      for (int i = 0; i < dataFiles.size(); ++i) {
        if (fileScanTask.file().path().toString().equals(dataFiles.get(i).path().toString())) {
          sortedFileArray[i] = fileScanTask;
        }
      }
    }
    List<FileScanTask> sortedFileList = Lists.newArrayList(sortedFileArray);
    MatcherAssert.assertThat(sortedFileList, CoreMatchers.everyItem(CoreMatchers.notNullValue(FileScanTask.class)));
    CombinedScanTask rearrangedCombinedTask = new BaseCombinedScanTask(sortedFileList);
    return IcebergSourceSplit.fromCombinedScanTask(rearrangedCombinedTask);
  }
}
