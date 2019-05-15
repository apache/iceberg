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
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestSplitPlanning {

  private static final Configuration CONF = new Configuration();
  private static final HadoopCatalog HADOOP_CATALOG = new HadoopCatalog(CONF);
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private Table table = null;

  @Before
  public void setupTable() throws IOException {
    File tableDir = temp.newFolder();
    String tableLocation = tableDir.toURI().toString();
    table = HADOOP_CATALOG.createTable(new TableIdentifier(Namespace.empty(), tableLocation), SCHEMA,
        PartitionSpec.unpartitioned(), null);
    table.updateProperties()
        .set(TableProperties.SPLIT_SIZE, String.valueOf(128 * 1024 * 1024))
        .set(TableProperties.SPLIT_OPEN_FILE_COST, String.valueOf(4 * 1024 * 1024))
        .set(TableProperties.SPLIT_LOOKBACK, String.valueOf(Integer.MAX_VALUE))
        .commit();
  }

  @Test
  public void testBasicSplitPlanning() {
    List<DataFile> files128Mb = newFiles(4, 128 * 1024 * 1024);
    appendFiles(files128Mb);
    // we expect 4 bins since split size is 128MB and we have 4 files 128MB each
    Assert.assertEquals(4, Iterables.size(table.newScan().planTasks()));
    List<DataFile> files32Mb = newFiles(16, 32 * 1024 * 1024);
    appendFiles(files32Mb);
    // we expect 8 bins after we add 16 files 32MB each as they will form additional 4 bins
    Assert.assertEquals(8, Iterables.size(table.newScan().planTasks()));
  }

  @Test
  public void testSplitPlanningWithSmallFiles() {
    List<DataFile> files60Mb = newFiles(50, 60 * 1024 * 1024);
    List<DataFile> files5Kb = newFiles(370, 5 * 1024);
    Iterable<DataFile> files = Iterables.concat(files60Mb, files5Kb);
    appendFiles(files);
    // 50 files of size 60MB will form 25 bins as split size is 128MB
    // each of those bins will have 8MB left and all 370 files of size 5KB would end up
    // in one of them without "read.split.open-file-cost"
    // as "read.split.open-file-cost" is 4MB, each of the original 25 bins will get at most 2 files
    // so 50 of 370 files will be packed into the existing 25 bins and the remaining 320 files
    // will form additional 10 bins, resulting in 35 bins in total
    Assert.assertEquals(35, Iterables.size(table.newScan().planTasks()));
  }

  @Test
  public void testSplitPlanningWithNoMinWeight() {
    table.updateProperties()
        .set(TableProperties.SPLIT_OPEN_FILE_COST, "0")
        .commit();
    List<DataFile> files60Mb = newFiles(2, 60 * 1024 * 1024);
    List<DataFile> files5Kb = newFiles(100, 5 * 1024);
    Iterable<DataFile> files = Iterables.concat(files60Mb, files5Kb);
    appendFiles(files);
    // all small files will be packed into one bin as "read.split.open-file-cost" is set to 0
    Assert.assertEquals(1, Iterables.size(table.newScan().planTasks()));
  }

  private void appendFiles(Iterable<DataFile> files) {
    AppendFiles appendFiles = table.newAppend();
    files.forEach(appendFiles::appendFile);
    appendFiles.commit();
  }

  private List<DataFile> newFiles(int numFiles, long sizeInBytes) {
    List<DataFile> files = Lists.newArrayList();
    for (int fileNum = 0; fileNum < numFiles; fileNum++) {
      files.add(newFile(sizeInBytes));
    }
    return files;
  }

  private DataFile newFile(long sizeInBytes) {
    String fileName = UUID.randomUUID().toString();
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(FileFormat.PARQUET.addExtension(fileName))
        .withFileSizeInBytes(sizeInBytes)
        .withRecordCount(2)
        .build();
  }
}
