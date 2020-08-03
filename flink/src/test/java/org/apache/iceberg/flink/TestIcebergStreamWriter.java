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

package org.apache.iceberg.flink;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestIcebergStreamWriter {
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private String tablePath;
  private Table table;

  private final FileFormat format;
  private final boolean partitioned;

  // TODO add ORC unit test once the readers and writers are ready.
  @Parameterized.Parameters(name = "format = {0}, partitioned = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] {"parquet", true},
        new Object[] {"parquet", false},
        new Object[] {"avro", true},
        new Object[] {"avro", false},
    };
  }

  public TestIcebergStreamWriter(String format, boolean partitioned) {
    this.format = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));
    this.partitioned = partitioned;
  }

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    tablePath = folder.getAbsolutePath();

    // Construct the iceberg table.
    Map<String, String> props = ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name());
    table = SimpleDataUtil.createTable(tablePath, props, partitioned);
  }

  @Test
  public void testWritingTable() throws Exception {
    long checkpointId = 1L;
    try (OneInputStreamOperatorTestHarness<Row, DataFile> testHarness = createIcebergStreamWriter()) {
      testHarness.setup();
      testHarness.open();

      // The first checkpoint
      testHarness.processElement(Row.of(1, "hello"), 1);
      testHarness.processElement(Row.of(2, "world"), 1);
      testHarness.processElement(Row.of(3, "hello"), 1);

      testHarness.prepareSnapshotPreBarrier(checkpointId);
      long expectedDataFiles = partitioned ? 2 : 1;
      Assert.assertEquals(expectedDataFiles, testHarness.extractOutputValues().size());

      checkpointId = checkpointId + 1;

      // The second checkpoint
      testHarness.processElement(Row.of(4, "foo"), 1);
      testHarness.processElement(Row.of(5, "bar"), 2);

      testHarness.prepareSnapshotPreBarrier(checkpointId);
      expectedDataFiles = partitioned ? 4 : 2;
      Assert.assertEquals(expectedDataFiles, testHarness.extractOutputValues().size());

      // Commit the iceberg transaction.
      AppendFiles appendFiles = table.newAppend();
      testHarness.extractOutputValues().forEach(appendFiles::appendFile);
      appendFiles.commit();

      // Assert the table records.
      SimpleDataUtil.assertTableRecords(tablePath, Lists.newArrayList(
          SimpleDataUtil.createRecord(1, "hello"),
          SimpleDataUtil.createRecord(2, "world"),
          SimpleDataUtil.createRecord(3, "hello"),
          SimpleDataUtil.createRecord(4, "foo"),
          SimpleDataUtil.createRecord(5, "bar")
      ));
    }
  }

  @Test
  public void testSnapshotTwice() throws Exception {
    long checkpointId = 1;
    long timestamp = 1;
    try (OneInputStreamOperatorTestHarness<Row, DataFile> testHarness = createIcebergStreamWriter()) {
      testHarness.setup();
      testHarness.open();

      testHarness.processElement(Row.of(1, "hello"), timestamp++);
      testHarness.processElement(Row.of(2, "world"), timestamp);

      testHarness.prepareSnapshotPreBarrier(checkpointId++);
      long expectedDataFiles = partitioned ? 2 : 1;
      Assert.assertEquals(expectedDataFiles, testHarness.extractOutputValues().size());

      // snapshot again immediately.
      for (int i = 0; i < 5; i++) {
        testHarness.prepareSnapshotPreBarrier(checkpointId++);
        Assert.assertEquals(expectedDataFiles, testHarness.extractOutputValues().size());
      }
    }
  }

  @Test
  public void testTableWithoutSnapshot() throws Exception {
    try (OneInputStreamOperatorTestHarness<Row, DataFile> testHarness = createIcebergStreamWriter()) {
      testHarness.setup();
      testHarness.open();

      Assert.assertEquals(0, testHarness.extractOutputValues().size());
    }
    // Even if we closed the iceberg stream writer, there's no orphan data file.
    Assert.assertEquals(0, scanDataFiles().size());

    try (OneInputStreamOperatorTestHarness<Row, DataFile> testHarness = createIcebergStreamWriter()) {
      testHarness.setup();
      testHarness.open();

      testHarness.processElement(Row.of(1, "hello"), 1);
      // Still not emit the data file yet, because there is no checkpoint.
      Assert.assertEquals(0, testHarness.extractOutputValues().size());
    }
    // Once we closed the iceberg stream writer, there will left an orphan data file.
    Assert.assertEquals(1, scanDataFiles().size());
  }

  private Set<String> scanDataFiles() throws IOException {
    Path dataDir = new Path(tablePath, "data");
    FileSystem fs = FileSystem.get(new Configuration());
    if (!fs.exists(dataDir)) {
      return ImmutableSet.of();
    } else {
      Set<String> paths = Sets.newHashSet();
      RemoteIterator<LocatedFileStatus> iterators = fs.listFiles(dataDir, true);
      while (iterators.hasNext()) {
        LocatedFileStatus status = iterators.next();
        if (status.isFile()) {
          Path path = status.getPath();
          if (path.getName().endsWith("." + format.toString().toLowerCase())) {
            paths.add(path.toString());
          }
        }
      }
      return paths;
    }
  }

  @Test
  public void testBoundedStreamCloseWithEmittingDataFiles() throws Exception {
    try (OneInputStreamOperatorTestHarness<Row, DataFile> testHarness = createIcebergStreamWriter()) {
      testHarness.setup();
      testHarness.open();

      testHarness.processElement(Row.of(1, "hello"), 1);
      testHarness.processElement(Row.of(2, "world"), 2);

      Assert.assertTrue(testHarness.getOneInputOperator() instanceof BoundedOneInput);
      ((BoundedOneInput) testHarness.getOneInputOperator()).endInput();

      long expectedDataFiles = partitioned ? 2 : 1;
      Assert.assertEquals(expectedDataFiles, testHarness.extractOutputValues().size());

      // invoke endInput again.
      ((BoundedOneInput) testHarness.getOneInputOperator()).endInput();
      Assert.assertEquals(expectedDataFiles * 2, testHarness.extractOutputValues().size());
    }
  }

  @Test
  public void testTableWithTargetFileSize() throws Exception {
    // Adjust the target-file-size in table properties.
    table.updateProperties()
        .set(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, "4") // ~4 bytes; low enough to trigger
        .commit();

    List<Row> rows = Lists.newArrayListWithCapacity(8000);
    List<Record> records = Lists.newArrayListWithCapacity(8000);
    for (int i = 0; i < 2000; i++) {
      for (String data : new String[] {"a", "b", "c", "d"}) {
        rows.add(Row.of(i, data));
        records.add(SimpleDataUtil.createRecord(i, data));
      }
    }

    try (OneInputStreamOperatorTestHarness<Row, DataFile> testHarness = createIcebergStreamWriter()) {
      testHarness.setup();
      testHarness.open();

      for (Row row : rows) {
        testHarness.processElement(row, 1);
      }

      // snapshot the operator.
      testHarness.prepareSnapshotPreBarrier(1);
      Assert.assertEquals(8, testHarness.extractOutputValues().size());

      // Assert that the data file have the expected records.
      for (DataFile serDataFile : testHarness.extractOutputValues()) {
        Assert.assertEquals(1000, serDataFile.recordCount());
      }

      // Commit the iceberg transaction.
      AppendFiles appendFiles = table.newAppend();
      testHarness.extractOutputValues().forEach(appendFiles::appendFile);
      appendFiles.commit();
    }

    // Assert the table records.
    SimpleDataUtil.assertTableRecords(tablePath, records);
  }

  private OneInputStreamOperatorTestHarness<Row, DataFile> createIcebergStreamWriter() throws Exception {
    IcebergStreamWriter<Row> streamWriter = IcebergSinkUtil.createStreamWriter(table, SimpleDataUtil.FLINK_SCHEMA);
    return new OneInputStreamOperatorTestHarness<>(streamWriter, 1, 1, 0);
  }

  private static Set<String> toAbsolutePathSet(List<DataFile> dataFiles) {
    Set<String> pathSet = Sets.newHashSet();
    for (DataFile dataFile : dataFiles) {
      pathSet.add(dataFile.path().toString());
    }
    return pathSet;
  }
}
