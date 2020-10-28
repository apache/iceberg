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

package org.apache.iceberg.flink.sink;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.flink.sink.ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION;

public class TestFlinkManifest {
  private static final Configuration CONF = new Configuration();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private String tablePath;
  private Table table;
  private final AtomicInteger dataFileCount = new AtomicInteger(0);

  @Before
  public void before() throws IOException {
    File folder = tempFolder.newFolder();
    String warehouse = folder.getAbsolutePath();

    tablePath = warehouse.concat("/test");
    Assert.assertTrue("Should create the table directory correctly.", new File(tablePath).mkdir());

    // Construct the iceberg table.
    table = SimpleDataUtil.createTable(tablePath, ImmutableMap.of(), false);
  }


  @Test
  public void testIO() throws IOException {
    String flinkJobId = newFlinkJobId();
    for (long checkpointId = 1; checkpointId <= 3; checkpointId++) {
      ManifestOutputFileFactory factory =
          FlinkManifestUtil.createOutputFileFactory(table, flinkJobId, 1, 1);
      OutputFile manifestOutputFile = factory.create(checkpointId);

      List<DataFile> expectedDataFiles = generateDataFiles(10);
      ManifestFile manifestFile = FlinkManifestUtil.writeDataFiles(manifestOutputFile, table.spec(), expectedDataFiles);

      List<DataFile> actualDataFiles = FlinkManifestUtil.readDataFiles(manifestFile, table.io());

      Assert.assertEquals("Size of data file list are not equal.", expectedDataFiles.size(), actualDataFiles.size());
      for (int i = 0; i < expectedDataFiles.size(); i++) {
        checkDataFile(expectedDataFiles.get(i), actualDataFiles.get(i));
      }
    }
  }

  @Test
  public void testUserProvidedManifestLocation() throws IOException {
    long checkpointId = 1;
    String flinkJobId = newFlinkJobId();
    File userProvidedFolder = tempFolder.newFolder();
    Map<String, String> props = ImmutableMap.of(FLINK_MANIFEST_LOCATION, userProvidedFolder.getAbsolutePath() + "///");
    ManifestOutputFileFactory factory = new ManifestOutputFileFactory(
        ((HasTableOperations) table).operations(), table.io(), props,
        flinkJobId, 1, 1);

    OutputFile outputFile = factory.create(checkpointId);
    List<DataFile> expectedDataFiles = generateDataFiles(5);
    ManifestFile manifestFile = FlinkManifestUtil.writeDataFiles(outputFile, table.spec(), expectedDataFiles);

    Assert.assertEquals("The newly created manifest file should be located under the user provided directory",
        userProvidedFolder.toPath(), Paths.get(manifestFile.path()).getParent());

    List<DataFile> actualDataFiles = FlinkManifestUtil.readDataFiles(manifestFile, table.io());

    Assert.assertEquals("Size of data file list are not equal.", expectedDataFiles.size(), actualDataFiles.size());
    for (int i = 0; i < expectedDataFiles.size(); i++) {
      checkDataFile(expectedDataFiles.get(i), actualDataFiles.get(i));
    }
  }

  @Test
  public void testVersionedSerializer() throws IOException {
    long checkpointId = 1;
    String flinkJobId = newFlinkJobId();
    ManifestOutputFileFactory factory = FlinkManifestUtil.createOutputFileFactory(table, flinkJobId, 1, 1);
    OutputFile outputFile = factory.create(checkpointId);

    List<DataFile> expectedDataFiles = generateDataFiles(10);
    ManifestFile expected = FlinkManifestUtil.writeDataFiles(outputFile, table.spec(), expectedDataFiles);

    byte[] versionedSerializeData =
        SimpleVersionedSerialization.writeVersionAndSerialize(FlinkManifestSerializer.INSTANCE, expected);
    ManifestFile actual = SimpleVersionedSerialization
        .readVersionAndDeSerialize(FlinkManifestSerializer.INSTANCE, versionedSerializeData);
    checkManifestFile(expected, actual);

    byte[] versionedSerializeData2 =
        SimpleVersionedSerialization.writeVersionAndSerialize(FlinkManifestSerializer.INSTANCE, actual);
    Assert.assertArrayEquals(versionedSerializeData, versionedSerializeData2);
  }

  private DataFile writeDataFile(String filename, List<RowData> rows) throws IOException {
    return SimpleDataUtil.writeFile(table.schema(), table.spec(), CONF,
        tablePath, FileFormat.PARQUET.addExtension(filename), rows);
  }

  private List<DataFile> generateDataFiles(int fileNum) throws IOException {
    List<RowData> rowDataList = Lists.newArrayList();
    List<DataFile> dataFiles = Lists.newArrayList();
    for (int i = 0; i < fileNum; i++) {
      rowDataList.add(SimpleDataUtil.createRowData(i, "a" + i));
      dataFiles.add(writeDataFile("data-file-" + dataFileCount.incrementAndGet(), rowDataList));
    }
    return dataFiles;
  }

  private static String newFlinkJobId() {
    return UUID.randomUUID().toString();
  }

  private static void checkManifestFile(ManifestFile expected, ManifestFile actual) {
    Assert.assertEquals("Path must match", expected.path(), actual.path());
    Assert.assertEquals("Length must match", expected.length(), actual.length());
    Assert.assertEquals("Spec id must match", expected.partitionSpecId(), actual.partitionSpecId());
    Assert.assertEquals("ManifestContent must match", expected.content(), actual.content());
    Assert.assertEquals("SequenceNumber must match", expected.sequenceNumber(), actual.sequenceNumber());
    Assert.assertEquals("MinSequenceNumber must match", expected.minSequenceNumber(), actual.minSequenceNumber());
    Assert.assertEquals("Snapshot id must match", expected.snapshotId(), actual.snapshotId());
    Assert.assertEquals("Added files flag must match", expected.hasAddedFiles(), actual.hasAddedFiles());
    Assert.assertEquals("Added files count must match", expected.addedFilesCount(), actual.addedFilesCount());
    Assert.assertEquals("Added rows count must match", expected.addedRowsCount(), actual.addedRowsCount());
    Assert.assertEquals("Existing files flag must match", expected.hasExistingFiles(), actual.hasExistingFiles());
    Assert.assertEquals("Existing files count must match", expected.existingFilesCount(), actual.existingFilesCount());
    Assert.assertEquals("Existing rows count must match", expected.existingRowsCount(), actual.existingRowsCount());
    Assert.assertEquals("Deleted files flag must match", expected.hasDeletedFiles(), actual.hasDeletedFiles());
    Assert.assertEquals("Deleted files count must match", expected.deletedFilesCount(), actual.deletedFilesCount());
    Assert.assertEquals("Deleted rows count must match", expected.deletedRowsCount(), actual.deletedRowsCount());
    Assert.assertEquals("PartitionFieldSummary must match", expected.partitions(), actual.partitions());
  }

  static void checkDataFile(DataFile expected, DataFile actual) {
    if (expected == actual) {
      return;
    }
    Assert.assertTrue("Shouldn't have null DataFile.", expected != null && actual != null);
    Assert.assertEquals("SpecId", expected.specId(), actual.specId());
    Assert.assertEquals("Content", expected.content(), actual.content());
    Assert.assertEquals("Path", expected.path(), actual.path());
    Assert.assertEquals("Format", expected.format(), actual.format());
    Assert.assertEquals("Partition", expected.partition(), actual.partition());
    Assert.assertEquals("Record count", expected.recordCount(), actual.recordCount());
    Assert.assertEquals("File size in bytes", expected.fileSizeInBytes(), actual.fileSizeInBytes());
    Assert.assertEquals("Column sizes", expected.columnSizes(), actual.columnSizes());
    Assert.assertEquals("Value counts", expected.valueCounts(), actual.valueCounts());
    Assert.assertEquals("Null value counts", expected.nullValueCounts(), actual.nullValueCounts());
    Assert.assertEquals("Lower bounds", expected.lowerBounds(), actual.lowerBounds());
    Assert.assertEquals("Upper bounds", expected.upperBounds(), actual.upperBounds());
    Assert.assertEquals("Key metadata", expected.keyMetadata(), actual.keyMetadata());
    Assert.assertEquals("Split offsets", expected.splitOffsets(), actual.splitOffsets());
    Assert.assertNull(actual.equalityFieldIds());
    Assert.assertNull(expected.equalityFieldIds());
  }
}
