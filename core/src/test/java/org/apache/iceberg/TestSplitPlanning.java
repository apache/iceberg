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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFiles.Builder;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSplitPlanning extends TestBase {

  private static final Configuration CONF = new Configuration();
  private static final HadoopTables TABLES = new HadoopTables(CONF);
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));

  @TempDir private Path temp;

  private Table table = null;

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2, 3);
  }

  @Override
  @BeforeEach
  public void setupTable() throws IOException {
    File tableDir = Files.createTempDirectory(temp, "junit").toFile();
    String tableLocation = tableDir.toURI().toString();
    table = TABLES.create(SCHEMA, tableLocation);
    table
        .updateProperties()
        .set(TableProperties.SPLIT_SIZE, String.valueOf(128 * 1024 * 1024))
        .set(TableProperties.SPLIT_OPEN_FILE_COST, String.valueOf(4 * 1024 * 1024))
        .set(TableProperties.SPLIT_LOOKBACK, String.valueOf(Integer.MAX_VALUE))
        .commit();
  }

  @TestTemplate
  public void testBasicSplitPlanning() {
    List<DataFile> files128Mb = newFiles(4, 128 * 1024 * 1024);
    appendFiles(files128Mb);
    // we expect 4 bins since split size is 128MB and we have 4 files 128MB each
    assertThat(table.newScan().planTasks()).hasSize(4);
    List<DataFile> files32Mb = newFiles(16, 32 * 1024 * 1024);
    appendFiles(files32Mb);
    // we expect 8 bins after we add 16 files 32MB each as they will form additional 4 bins
    assertThat(table.newScan().planTasks()).hasSize(8);
  }

  @TestTemplate
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
    assertThat(table.newScan().planTasks()).hasSize(35);
  }

  @TestTemplate
  public void testSplitPlanningWithNoMinWeight() {
    table.updateProperties().set(TableProperties.SPLIT_OPEN_FILE_COST, "0").commit();
    List<DataFile> files60Mb = newFiles(2, 60 * 1024 * 1024);
    List<DataFile> files5Kb = newFiles(100, 5 * 1024);
    Iterable<DataFile> files = Iterables.concat(files60Mb, files5Kb);
    appendFiles(files);
    // all small files will be packed into one bin as "read.split.open-file-cost" is set to 0
    assertThat(table.newScan().planTasks()).hasSize(1);
  }

  @TestTemplate
  public void testSplitPlanningWithOverridenSize() {
    List<DataFile> files128Mb = newFiles(4, 128 * 1024 * 1024);
    appendFiles(files128Mb);
    // we expect 2 bins since we are overriding split size in scan with 256MB
    TableScan scan =
        table.newScan().option(TableProperties.SPLIT_SIZE, String.valueOf(256L * 1024 * 1024));
    assertThat(scan.planTasks()).hasSize(2);
  }

  @TestTemplate
  public void testSplitPlanningWithOverriddenSizeForMetadataJsonFile() {
    List<DataFile> files8Mb = newFiles(32, 8 * 1024 * 1024, FileFormat.METADATA);
    appendFiles(files8Mb);
    // we expect 16 bins since we are overriding split size in scan with 16MB
    TableScan scan =
        table.newScan().option(TableProperties.SPLIT_SIZE, String.valueOf(16L * 1024 * 1024));
    assertThat(scan.planTasks()).hasSize(16);
  }

  @TestTemplate
  public void testSplitPlanningWithOverriddenSizeForLargeMetadataJsonFile() {
    List<DataFile> files128Mb = newFiles(4, 128 * 1024 * 1024, FileFormat.METADATA);
    appendFiles(files128Mb);
    // although overriding split size in scan with 8MB, we expect 4 bins since metadata file is not
    // splittable
    TableScan scan =
        table.newScan().option(TableProperties.SPLIT_SIZE, String.valueOf(8L * 1024 * 1024));
    assertThat(scan.planTasks()).hasSize(4);
  }

  @TestTemplate
  public void testSplitPlanningWithOverridenLookback() {
    List<DataFile> files120Mb = newFiles(1, 120 * 1024 * 1024);
    List<DataFile> file128Mb = newFiles(1, 128 * 1024 * 1024);
    Iterable<DataFile> files = Iterables.concat(files120Mb, file128Mb);
    appendFiles(files);
    // we expect 2 bins from non-overriden table properties
    TableScan scan = table.newScan().option(TableProperties.SPLIT_LOOKBACK, "1");
    CloseableIterable<CombinedScanTask> tasks = scan.planTasks();
    assertThat(tasks).hasSize(2);

    // since lookback was overridden to 1, we expect the first bin to be the largest of the two.
    CombinedScanTask combinedScanTask = tasks.iterator().next();
    FileScanTask task = combinedScanTask.files().iterator().next();
    assertThat(task.length()).isEqualTo(128 * 1024 * 1024);
  }

  @TestTemplate
  public void testSplitPlanningWithOverridenOpenCostSize() {
    List<DataFile> files16Mb = newFiles(16, 16 * 1024 * 1024);
    appendFiles(files16Mb);
    // we expect 4 bins since we are overriding open file cost in scan with a cost of 32MB
    // we can fit at most 128Mb/32Mb = 4 files per bin
    TableScan scan =
        table
            .newScan()
            .option(TableProperties.SPLIT_OPEN_FILE_COST, String.valueOf(32L * 1024 * 1024));
    assertThat(scan.planTasks()).hasSize(4);
  }

  @TestTemplate
  public void testSplitPlanningWithNegativeValues() {
    assertThatThrownBy(
            () ->
                table.newScan().option(TableProperties.SPLIT_SIZE, String.valueOf(-10)).planTasks())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Split size must be > 0: -10");

    assertThatThrownBy(
            () ->
                table
                    .newScan()
                    .option(TableProperties.SPLIT_LOOKBACK, String.valueOf(-10))
                    .planTasks())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Split planning lookback must be > 0: -10");

    assertThatThrownBy(
            () ->
                table
                    .newScan()
                    .option(TableProperties.SPLIT_OPEN_FILE_COST, String.valueOf(-10))
                    .planTasks())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("File open cost must be >= 0: -10");
  }

  @TestTemplate
  public void testSplitPlanningWithOffsets() {
    List<DataFile> files16Mb = newFiles(16, 16 * 1024 * 1024, 2);
    appendFiles(files16Mb);

    // Split Size is slightly larger than rowGroup Size, but we should still end up with
    // 1 split per row group
    TableScan scan =
        table.newScan().option(TableProperties.SPLIT_SIZE, String.valueOf(10L * 1024 * 1024));
    assertThat(scan.planTasks()).hasSize(32);
  }

  @TestTemplate
  public void testSplitPlanningWithOffsetsUnableToSplit() {
    List<DataFile> files16Mb = newFiles(16, 16 * 1024 * 1024, 2);
    appendFiles(files16Mb);

    // Split Size does not match up with offsets, so even though we want 4 cuts per file we still
    // only get 2
    TableScan scan =
        table
            .newScan()
            .option(TableProperties.SPLIT_OPEN_FILE_COST, String.valueOf(0))
            .option(TableProperties.SPLIT_SIZE, String.valueOf(4L * 1024 * 1024));
    assertThat(scan.planTasks()).hasSize(32);
  }

  @TestTemplate
  public void testBasicSplitPlanningDeleteFiles() {
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();
    List<DeleteFile> files128Mb = newDeleteFiles(4, 128 * 1024 * 1024);
    appendDeleteFiles(files128Mb);

    PositionDeletesTable posDeletesTable = new PositionDeletesTable(table);
    // we expect 4 bins since split size is 128MB and we have 4 files 128MB each
    assertThat(posDeletesTable.newBatchScan().planTasks()).hasSize(4);
    List<DeleteFile> files32Mb = newDeleteFiles(16, 32 * 1024 * 1024);
    appendDeleteFiles(files32Mb);
    // we expect 8 bins after we add 16 files 32MB each as they will form additional 4 bins
    assertThat(posDeletesTable.newBatchScan().planTasks()).hasSize(8);
  }

  @TestTemplate
  public void testBasicSplitPlanningDeleteFilesWithSplitOffsets() {
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "2").commit();
    List<DeleteFile> files128Mb = newDeleteFiles(4, 128 * 1024 * 1024, 8);
    appendDeleteFiles(files128Mb);

    PositionDeletesTable posDeletesTable = new PositionDeletesTable(table);

    try (CloseableIterable<ScanTaskGroup<ScanTask>> groups =
        posDeletesTable
            .newBatchScan()
            .option(TableProperties.SPLIT_SIZE, String.valueOf(64L * 1024 * 1024))
            .planTasks()) {
      int totalTaskGroups = 0;
      for (ScanTaskGroup<ScanTask> group : groups) {
        int tasksPerGroup = 0;
        long previousOffset = -1;
        for (ScanTask task : group.tasks()) {
          tasksPerGroup++;
          assertThat(task).isInstanceOf(SplitPositionDeletesScanTask.class);
          SplitPositionDeletesScanTask splitPosDelTask = (SplitPositionDeletesScanTask) task;
          if (previousOffset != -1) {
            assertThat(previousOffset).isEqualTo(splitPosDelTask.start());
          }
          previousOffset = splitPosDelTask.start() + splitPosDelTask.length();
        }

        assertThat(tasksPerGroup).isEqualTo(1);
        totalTaskGroups++;
      }
      // we expect 8 bins since split size is 64MB
      assertThat(totalTaskGroups).isEqualTo(8);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void appendFiles(Iterable<DataFile> files) {
    AppendFiles appendFiles = table.newAppend();
    files.forEach(appendFiles::appendFile);
    appendFiles.commit();
  }

  private List<DataFile> newFiles(int numFiles, long sizeInBytes) {
    return newFiles(numFiles, sizeInBytes, FileFormat.PARQUET, 1);
  }

  private List<DataFile> newFiles(int numFiles, long sizeInBytes, int numOffset) {
    return newFiles(numFiles, sizeInBytes, FileFormat.PARQUET, numOffset);
  }

  private List<DataFile> newFiles(int numFiles, long sizeInBytes, FileFormat fileFormat) {
    return newFiles(numFiles, sizeInBytes, fileFormat, 1);
  }

  private List<DataFile> newFiles(
      int numFiles, long sizeInBytes, FileFormat fileFormat, int numOffset) {
    List<DataFile> files = Lists.newArrayList();
    for (int fileNum = 0; fileNum < numFiles; fileNum++) {
      files.add(newFile(sizeInBytes, fileFormat, numOffset));
    }
    return files;
  }

  private DataFile newFile(long sizeInBytes, FileFormat fileFormat, int numOffsets) {
    String fileName = UUID.randomUUID().toString();
    Builder builder =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(fileFormat.addExtension(fileName))
            .withFileSizeInBytes(sizeInBytes)
            .withRecordCount(2);

    if (numOffsets > 1) {
      long stepSize = sizeInBytes / numOffsets;
      List<Long> offsets =
          LongStream.range(0, numOffsets)
              .map(i -> i * stepSize)
              .boxed()
              .collect(Collectors.toList());
      builder.withSplitOffsets(offsets);
    }

    return builder.build();
  }

  private void appendDeleteFiles(List<DeleteFile> files) {
    RowDelta rowDelta = table.newRowDelta();
    files.forEach(rowDelta::addDeletes);
    rowDelta.commit();
  }

  private List<DeleteFile> newDeleteFiles(int numFiles, long sizeInBytes) {
    return newDeleteFiles(numFiles, sizeInBytes, FileFormat.PARQUET, 1);
  }

  private List<DeleteFile> newDeleteFiles(int numFiles, long sizeInBytes, long numOffsets) {
    return newDeleteFiles(numFiles, sizeInBytes, FileFormat.PARQUET, numOffsets);
  }

  private List<DeleteFile> newDeleteFiles(
      int numFiles, long sizeInBytes, FileFormat fileFormat, long numOffsets) {
    List<DeleteFile> files = Lists.newArrayList();
    for (int fileNum = 0; fileNum < numFiles; fileNum++) {
      files.add(newDeleteFile(sizeInBytes, fileFormat, numOffsets));
    }
    return files;
  }

  private DeleteFile newDeleteFile(long sizeInBytes, FileFormat fileFormat, long numOffsets) {
    String fileName = UUID.randomUUID().toString();
    FileMetadata.Builder builder =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withPath(fileFormat.addExtension(fileName))
            .withFileSizeInBytes(sizeInBytes)
            .withRecordCount(2);

    if (numOffsets > 1) {
      long stepSize = sizeInBytes / numOffsets;
      List<Long> offsets =
          LongStream.range(0, numOffsets)
              .map(i -> i * stepSize)
              .boxed()
              .collect(Collectors.toList());
      builder.withSplitOffsets(offsets);
    }

    return builder.build();
  }
}
