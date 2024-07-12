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
package org.apache.iceberg.hadoop;

import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.LockManager;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Tasks;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

public class TestHadoopCommits extends HadoopTableTestBase {

  @Test
  public void testCreateTable() throws Exception {
    PartitionSpec expectedSpec = PartitionSpec.builderFor(TABLE_SCHEMA).bucket("data", 16).build();

    assertThat(table.schema().asStruct())
        .as("Table schema should match schema with reassigned ids")
        .isEqualTo(TABLE_SCHEMA.asStruct());
    assertThat(table.spec())
        .as("Table partition spec should match with reassigned ids")
        .isEqualTo(expectedSpec);

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    assertThat(tasks).as("Should not create any scan tasks").isEmpty();
    assertThat(tableDir).as("Table location should exist").exists();
    assertThat(metadataDir).as("Should create metadata folder").exists().isDirectory();
    assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
    assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();
    assertThat(versionHintFile).as("Should create version hint file").exists();
    assertThat(readVersionHint())
        .as("Should write the current version to the hint file")
        .isEqualTo(1);
    List<File> manifests = listManifestFiles();
    assertThat(manifests).as("Should contain 0 Avro manifest files").isEmpty();
  }

  @Test
  public void testSchemaUpdate() throws Exception {
    assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
    assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();

    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
    assertThat(readVersionHint())
        .as("Should write the current version to the hint file")
        .isEqualTo(2);
    assertThat(table.schema().asStruct())
        .as("Table schema should match schema with reassigned ids")
        .isEqualTo(UPDATED_SCHEMA.asStruct());

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    assertThat(tasks).as("Should not create any scan tasks").isEmpty();

    List<File> manifests = listManifestFiles();
    assertThat(manifests).as("Should contain 0 Avro manifest files").isEmpty();
  }

  @Test
  public void testSchemaUpdateComplexType() throws Exception {
    assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
    assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();

    Types.StructType complexColumn =
        Types.StructType.of(
            required(0, "w", Types.IntegerType.get()),
            required(1, "x", Types.StringType.get()),
            required(2, "y", Types.BooleanType.get()),
            optional(
                3,
                "z",
                Types.MapType.ofOptional(0, 1, Types.IntegerType.get(), Types.StringType.get())));
    Schema updatedSchema =
        new Schema(
            required(1, "id", Types.IntegerType.get(), "unique ID"),
            required(2, "data", Types.StringType.get()),
            optional(
                3,
                "complex",
                Types.StructType.of(
                    required(4, "w", Types.IntegerType.get()),
                    required(5, "x", Types.StringType.get()),
                    required(6, "y", Types.BooleanType.get()),
                    optional(
                        7,
                        "z",
                        Types.MapType.ofOptional(
                            8, 9, Types.IntegerType.get(), Types.StringType.get())))));

    table.updateSchema().addColumn("complex", complexColumn).commit();

    assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
    assertThat(readVersionHint())
        .as("Should write the current version to the hint file")
        .isEqualTo(2);
    assertThat(table.schema().asStruct())
        .as("Table schema should match schema with reassigned ids")
        .isEqualTo(updatedSchema.asStruct());

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    assertThat(tasks).as("Should not create any scan tasks").isEmpty();

    List<File> manifests = listManifestFiles();
    assertThat(manifests).as("Should contain 0 Avro manifest files").isEmpty();
  }

  @Test
  public void testSchemaUpdateIdentifierFields() throws Exception {
    assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
    assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();

    Schema updatedSchema =
        new Schema(
            Lists.newArrayList(
                required(1, "id", Types.IntegerType.get(), "unique ID"),
                required(2, "data", Types.StringType.get())),
            Sets.newHashSet(1));

    table.updateSchema().setIdentifierFields("id").commit();

    assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
    assertThat(readVersionHint())
        .as("Should write the current version to the hint file")
        .isEqualTo(2);
    assertThat(table.schema().asStruct())
        .as("Table schema should match schema with reassigned ids")
        .isEqualTo(updatedSchema.asStruct());
    assertThat(table.schema().identifierFieldIds())
        .as("Identifier fields should match schema with reassigned ids")
        .isEqualTo(updatedSchema.identifierFieldIds());
  }

  @Test
  public void testFailedCommit() throws Exception {
    // apply the change to metadata without committing
    UpdateSchema update = table.updateSchema().addColumn("n", Types.IntegerType.get());
    update.apply();

    assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
    assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();

    version(2).createNewFile();

    assertThatThrownBy(update::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessageStartingWith("Version 2 already exists");

    List<File> manifests = listManifestFiles();
    assertThat(manifests).as("Should contain 0 Avro manifest files").isEmpty();
  }

  @Test
  public void testStaleMetadata() throws Exception {
    Table tableCopy = TABLES.load(tableLocation);

    assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
    assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();

    // prepare changes on the copy without committing
    UpdateSchema updateCopy = tableCopy.updateSchema().addColumn("m", Types.IntegerType.get());
    updateCopy.apply();

    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
    assertThat(table.schema().asStruct())
        .as("Unmodified copy should be out of date after update")
        .isNotEqualTo(tableCopy.schema().asStruct());

    // update the table
    tableCopy.refresh();

    assertThat(table.schema().asStruct())
        .as("Copy should be back in sync")
        .isEqualTo(tableCopy.schema().asStruct());

    assertThatThrownBy(updateCopy::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Cannot commit changes based on stale table metadata");

    List<File> manifests = listManifestFiles();
    assertThat(manifests).as("Should contain 0 Avro manifest files").isEmpty();
  }

  @Test
  public void testStaleVersionHint() throws Exception {
    Table stale = TABLES.load(tableLocation);

    assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
    assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();

    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
    assertThat(readVersionHint())
        .as("Should write the current version to the hint file")
        .isEqualTo(2);
    assertThat(stale.schema().asStruct())
        .as("Stable table schema should not match")
        .isNotEqualTo(UPDATED_SCHEMA.asStruct());

    // roll the version hint back to 1
    replaceVersionHint(1);

    Table reloaded = TABLES.load(tableLocation);
    assertThat(reloaded.schema().asStruct())
        .as("Updated schema for newly loaded table should match")
        .isEqualTo(UPDATED_SCHEMA.asStruct());

    stale.refresh();
    assertThat(reloaded.schema().asStruct())
        .as("Refreshed schema for stale table should match")
        .isEqualTo(UPDATED_SCHEMA.asStruct());
  }

  @Test
  public void testFastAppend() throws Exception {
    // first append
    table.newFastAppend().appendFile(FILE_A).commit();

    assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
    assertThat(readVersionHint())
        .as("Should write the current version to the hint file")
        .isEqualTo(2);

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    assertThat(tasks).as("Should scan 1 file").hasSize(1);

    List<File> manifests = listManifestFiles();
    assertThat(manifests).as("Should contain only one Avro manifest file").hasSize(1);

    // second append
    table.newFastAppend().appendFile(FILE_B).commit();

    assertThat(version(3)).as("Should create v3 for the update").exists().isFile();
    assertThat(readVersionHint())
        .as("Should write the current version to the hint file")
        .isEqualTo(3);

    tasks = Lists.newArrayList(table.newScan().planFiles());
    assertThat(tasks).as("Should scan 2 files").hasSize(2);
    assertThat(listManifestFiles()).as("Should contain 2 Avro manifest files").hasSize(2);

    TableMetadata metadata = readMetadataVersion(3);
    assertThat(metadata.currentSnapshot().allManifests(table.io()))
        .as("Current snapshot should contain 2 manifests")
        .hasSize(2);
  }

  @Test
  public void testMergeAppend() throws Exception {
    testFastAppend(); // create 2 compatible manifest files that will be merged

    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    // third append
    table.newAppend().appendFile(FILE_C).commit();

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    assertThat(tasks).as("Should scan 3 files").hasSize(3);

    assertThat(listManifestFiles()).as("Should contain 3 Avro manifest files").hasSize(3);

    TableMetadata metadata = readMetadataVersion(5);
    assertThat(metadata.currentSnapshot().allManifests(table.io()))
        .as("Current snapshot should contain 1 merged manifest")
        .hasSize(1);
  }

  @Test
  public void testRenameReturnFalse() throws Exception {
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.exists(any())).thenReturn(true, false);
    when(mockFs.rename(any(), any())).thenReturn(false);
    testRenameWithFileSystem(mockFs);
  }

  @Test
  public void testRenameThrow() throws Exception {
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.exists(any())).thenReturn(true, false);
    when(mockFs.rename(any(), any())).thenThrow(new IOException("test injected"));
    testRenameWithFileSystem(mockFs);
  }

  /**
   * Test rename during {@link HadoopTableOperations#commit(TableMetadata, TableMetadata)} with the
   * provided {@link FileSystem} object. The provided FileSystem will be injected for commit call.
   */
  private void testRenameWithFileSystem(FileSystem mockFs) throws Exception {
    assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
    assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();
    assertThat(table).isInstanceOf(BaseTable.class);
    BaseTable baseTable = (BaseTable) table;
    // use v1 metafile as the test rename destination.
    TableMetadata meta1 = baseTable.operations().current();

    // create v2 metafile as base. This is solely for the convenience of rename testing later
    // (so that we have 2 valid and different metadata files, which will reach the rename part
    // during commit)
    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();
    assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
    assertThat(readVersionHint())
        .as("Should write the current version to the hint file")
        .isEqualTo(2);

    // mock / spy the classes for testing
    TableOperations tops = baseTable.operations();
    assertThat(tops).isInstanceOf(HadoopTableOperations.class);
    HadoopTableOperations spyOps = Mockito.spy((HadoopTableOperations) tops);

    // inject the mockFS into the TableOperations
    doReturn(mockFs).when(spyOps).getFileSystem(any(), any());
    assertThatThrownBy(() -> spyOps.commit(tops.current(), meta1))
        .isInstanceOf(CommitFailedException.class);

    // Verifies that there is no temporary metadata.json files left on rename failures.
    Set<String> actual =
        listMetadataJsonFiles().stream().map(File::getName).collect(Collectors.toSet());
    Set<String> expected = Sets.newHashSet("v1.metadata.json", "v2.metadata.json");
    assertThat(actual).as("only v1 and v2 metadata.json should exist.").isEqualTo(expected);
  }

  @Test
  public void testCanReadOldCompressedManifestFiles() throws Exception {
    assertThat(version(1)).as("Should create v1 metadata").exists().isFile();

    // do a file append
    table.newAppend().appendFile(FILE_A).commit();

    // since we don't generate old file extensions anymore, let's convert existing metadata to old
    // .metadata.json.gz
    // to test backwards compatibility
    rewriteMetadataAsGzipWithOldExtension();

    List<File> metadataFiles = listMetadataJsonFiles();

    assertThat(metadataFiles).as("Should have two versions").hasSize(2);
    assertThat(metadataFiles.stream().map(File::getName))
        .as("Metadata should be compressed with old format.")
        .allMatch(f -> f.endsWith(".metadata.json.gz"));

    Table reloaded = TABLES.load(tableLocation);

    List<FileScanTask> tasks = Lists.newArrayList(reloaded.newScan().planFiles());
    assertThat(tasks).as("Should scan 1 files").hasSize(1);
  }

  @Test
  public void testConcurrentFastAppends(@TempDir File dir) throws Exception {
    assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
    int threadsCount = 5;
    int numberOfCommitedFilesPerThread = 10;
    Table tableWithHighRetries =
        TABLES.create(
            SCHEMA,
            SPEC,
            ImmutableMap.of(COMMIT_NUM_RETRIES, String.valueOf(threadsCount)),
            dir.toURI().toString());

    String fileName = UUID.randomUUID().toString();
    DataFile file =
        DataFiles.builder(tableWithHighRetries.spec())
            .withPath(FileFormat.PARQUET.addExtension(fileName))
            .withRecordCount(2)
            .withFileSizeInBytes(0)
            .build();
    ExecutorService executorService = Executors.newFixedThreadPool(threadsCount);

    AtomicInteger barrier = new AtomicInteger(0);
    Tasks.range(threadsCount)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(executorService)
        .run(
            index -> {
              for (int numCommittedFiles = 0;
                  numCommittedFiles < numberOfCommitedFilesPerThread;
                  numCommittedFiles++) {
                final int currentFilesCount = numCommittedFiles;
                Awaitility.await()
                    .pollInterval(Duration.ofMillis(10))
                    .atMost(Duration.ofSeconds(10))
                    .until(() -> barrier.get() >= currentFilesCount * threadsCount);
                tableWithHighRetries.newFastAppend().appendFile(file).commit();
                barrier.incrementAndGet();
              }
            });

    tableWithHighRetries.refresh();
    assertThat(Lists.newArrayList(tableWithHighRetries.snapshots()))
        .hasSize(threadsCount * numberOfCommitedFilesPerThread);
  }

  @Test
  public void testCommitFailedToAcquireLock() {
    table.newFastAppend().appendFile(FILE_A).commit();
    Configuration conf = new Configuration();
    LockManager lockManager = new NoLockManager();
    HadoopTableOperations tableOperations =
        new HadoopTableOperations(
            new Path(table.location()), new HadoopFileIO(conf), conf, lockManager);
    tableOperations.refresh();
    BaseTable baseTable = (BaseTable) table;
    TableMetadata meta2 = baseTable.operations().current();
    assertThatThrownBy(() -> tableOperations.commit(tableOperations.current(), meta2))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageStartingWith("Failed to acquire lock on file");
  }

  @Test
  public void testCommitFailedBeforeChangeVersionHint() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    BaseTable baseTable = (BaseTable) table;
    HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();

    HadoopTableOperations spyOps2 = spy(tableOperations);
    doReturn(10000).when(spyOps2).findVersionWithOutVersionHint(any());
    TableMetadata metadataV1 = spyOps2.current();
    SortOrder dataSort = SortOrder.builderFor(baseTable.schema()).asc("data").build();
    TableMetadata metadataV2 = metadataV1.replaceSortOrder(dataSort);
    assertThatThrownBy(() -> spyOps2.commit(metadataV1, metadataV2))
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("Are there other clients running in parallel with the current task?");

    HadoopTableOperations spyOps3 = spy(tableOperations);
    doReturn(false).when(spyOps3).nextVersionIsLatest(anyInt(), anyInt());
    assertCommitNotChangeVersion(
        baseTable,
        spyOps3,
        CommitFailedException.class,
        "Are there other clients running in parallel with the current task?");

    HadoopTableOperations spyOps4 = spy(tableOperations);
    doThrow(new RuntimeException("FileSystem crash!"))
        .when(spyOps4)
        .renameMetaDataFileAndCheck(any(), any(), any(), anyBoolean());
    assertCommitNotChangeVersion(
        baseTable, spyOps4, CommitFailedException.class, "FileSystem crash!");
  }

  @Test
  public void testCommitFailedAndCheckFailed() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    BaseTable baseTable = (BaseTable) table;
    HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
    HadoopTableOperations spyOps = spy(tableOperations);
    doThrow(new IOException("FileSystem crash!"))
        .when(spyOps)
        .renameMetaDataFile(any(), any(), any());
    doThrow(new IOException("Can not check new Metadata!"))
        .when(spyOps)
        .checkMetaDataFileRenameSuccess(any(), any(), any(), anyBoolean());
    assertCommitNotChangeVersion(
        baseTable, spyOps, CommitStateUnknownException.class, "FileSystem crash!");

    HadoopTableOperations spyOps2 = spy(tableOperations);
    doThrow(new OutOfMemoryError("Java heap space"))
        .when(spyOps2)
        .renameMetaDataFile(any(), any(), any());
    assertCommitFail(baseTable, spyOps2, OutOfMemoryError.class, "Java heap space");

    HadoopTableOperations spyOps3 = spy(tableOperations);
    doThrow(new RuntimeException("UNKNOWN ERROR"))
        .when(spyOps3)
        .renameMetaDataFile(any(), any(), any());
    assertCommitNotChangeVersion(
        baseTable, spyOps3, CommitStateUnknownException.class, "UNKNOWN ERROR");
  }

  @Test
  public void testCommitFailedAndRenameNotSuccess() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    BaseTable baseTable = (BaseTable) table;
    HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
    HadoopTableOperations spyOps = spy(tableOperations);
    doThrow(new IOException("FileSystem crash!"))
        .when(spyOps)
        .renameMetaDataFile(any(), any(), any());
    assertCommitNotChangeVersion(
        baseTable,
        spyOps,
        CommitFailedException.class,
        "Are there other clients running in parallel with the current task?");
  }

  @Test
  public void testCommitFailedButActualSuccess() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    BaseTable baseTable = (BaseTable) table;
    HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
    HadoopTableOperations spyOps = spy(tableOperations);
    doThrow(new IOException("FileSystem crash!"))
        .when(spyOps)
        .renameMetaDataFile(any(), any(), any());
    doReturn(true).when(spyOps).checkMetaDataFileRenameSuccess(any(), any(), any(), anyBoolean());
    int versionBefore = spyOps.findVersion();
    TableMetadata metadataV1 = spyOps.current();
    SortOrder dataSort = SortOrder.builderFor(baseTable.schema()).asc("data").build();
    TableMetadata metadataV2 = metadataV1.replaceSortOrder(dataSort);
    spyOps.commit(metadataV1, metadataV2);
    int versionAfter = spyOps.findVersion();
    assertThat(versionAfter).isEqualTo(versionBefore + 1);
  }

  private void assertCommitNotChangeVersion(
      BaseTable baseTable,
      HadoopTableOperations spyOps,
      Class<? extends Throwable> exceptionClass,
      String msg) {
    int versionBefore = spyOps.findVersion();
    assertCommitFail(baseTable, spyOps, exceptionClass, msg);
    int versionAfter = spyOps.findVersion();
    assertThat(versionBefore).isEqualTo(versionAfter);
  }

  private void assertCommitFail(
      BaseTable baseTable,
      HadoopTableOperations spyOps,
      Class<? extends Throwable> exceptionClass,
      String msg) {
    TableMetadata metadataV1 = spyOps.current();
    SortOrder dataSort = SortOrder.builderFor(baseTable.schema()).asc("data").build();
    TableMetadata metadataV2 = metadataV1.replaceSortOrder(dataSort);
    assertThatThrownBy(() -> spyOps.commit(metadataV1, metadataV2))
        .isInstanceOf(exceptionClass)
        .hasMessageContaining(msg);
  }

  @Test
  public void testCommitFailedAfterChangeVersionHintRepeatCommit() {
    // Submit a new file to test the functionality.
    table.newFastAppend().appendFile(FILE_A).commit();
    BaseTable baseTable = (BaseTable) table;
    HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
    HadoopTableOperations spyOps = spy(tableOperations);
    doThrow(new RuntimeException("FileSystem crash!"))
        .when(spyOps)
        .deleteRemovedMetadataFiles(any(), any());
    int versionBefore = spyOps.findVersion();
    TableMetadata metadataV1 = spyOps.current();

    // first commit
    SortOrder dataSort = SortOrder.builderFor(baseTable.schema()).asc("data").build();
    TableMetadata metadataV2 = metadataV1.replaceSortOrder(dataSort);

    spyOps.commit(metadataV1, metadataV2);

    // The commit should actually success
    int versionAfterSecond = spyOps.findVersion();
    assertThat(versionAfterSecond).isEqualTo(versionBefore + 1);

    // second commit
    SortOrder idSort = SortOrder.builderFor(baseTable.schema()).desc("id").build();
    TableMetadata currentMeta = spyOps.current();
    TableMetadata metadataV3 = metadataV2.replaceSortOrder(idSort);
    spyOps.commit(currentMeta, metadataV3);

    // The commit should actually success
    int versionAfterThird = spyOps.findVersion();
    assertThat(versionAfterThird).isEqualTo(versionBefore + 2);
  }

  @Test
  public void testTwoClientCommitSameVersion() throws InterruptedException {
    // In the linux environment, the JDK FileSystem interface implementation class is
    // java.io.UnixFileSystem.
    // Its behavior follows the posix protocol, which causes rename operations to overwrite the
    // target file (because linux is compatible with some of the unix interfaces).
    // However, linux also supports renaming without overwriting the target file. In addition, some
    // other file systems such as Windows, HDFS, etc. also support renaming without overwriting the
    // target file.
    // We use the `mv -n` command to simulate the behavior of such filesystems.
    table.newFastAppend().appendFile(FILE_A).commit();
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    AtomicReference<Throwable> unexpectedException = new AtomicReference<>(null);
    AtomicReference<Throwable> expectedException = new AtomicReference<>(null);
    CountDownLatch countDownLatch = new CountDownLatch(2);
    BaseTable baseTable = (BaseTable) table;
    assertThat(((HadoopTableOperations) baseTable.operations()).findVersion()).isEqualTo(2);
    executorService.execute(
        () -> {
          try {
            HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
            HadoopTableOperations spyOps = spy(tableOperations);
            doNothing().when(spyOps).tryLock(any(), any());
            doAnswer(
                    x -> {
                      countDownLatch.countDown();
                      countDownLatch.await();
                      return x.callRealMethod();
                    })
                .when(spyOps)
                .renameMetaDataFileAndCheck(any(), any(), any(), anyBoolean());
            doAnswer(
                    x -> {
                      Path srcPath = x.getArgument(1);
                      Path dstPath = x.getArgument(2);
                      File src = new File(srcPath.toUri());
                      File dst = new File(dstPath.toUri());
                      String srcPathStr = src.getAbsolutePath();
                      String dstPathStr = dst.getAbsolutePath();
                      String cmd = String.format("mv -n %s  %s", srcPathStr, dstPathStr);
                      Process process = Runtime.getRuntime().exec(cmd);
                      assertThat(process.waitFor()).isEqualTo(0);
                      return dst.exists() && !src.exists();
                    })
                .when(spyOps)
                .renameMetaDataFile(any(), any(), any());
            TableMetadata metadataV1 = spyOps.current();
            SortOrder dataSort = SortOrder.builderFor(baseTable.schema()).asc("data").build();
            TableMetadata metadataV2 = metadataV1.replaceSortOrder(dataSort);
            spyOps.commit(metadataV1, metadataV2);
          } catch (CommitFailedException e) {
            expectedException.set(e);
          } catch (Throwable e) {
            unexpectedException.set(e);
          }
        });

    executorService.execute(
        () -> {
          try {
            HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
            HadoopTableOperations spyOps = spy(tableOperations);
            doNothing().when(spyOps).tryLock(any(), any());
            doAnswer(
                    x -> {
                      countDownLatch.countDown();
                      countDownLatch.await();
                      return x.callRealMethod();
                    })
                .when(spyOps)
                .renameMetaDataFileAndCheck(any(), any(), any(), anyBoolean());
            doAnswer(
                    x -> {
                      Path srcPath = x.getArgument(1);
                      Path dstPath = x.getArgument(2);
                      File src = new File(srcPath.toUri());
                      File dst = new File(dstPath.toUri());
                      String srcPathStr = src.getAbsolutePath();
                      String dstPathStr = dst.getAbsolutePath();
                      String cmd = String.format("mv -n %s  %s", srcPathStr, dstPathStr);
                      Process process = Runtime.getRuntime().exec(cmd);
                      assertThat(process.waitFor()).isEqualTo(0);
                      return dst.exists() && !src.exists();
                    })
                .when(spyOps)
                .renameMetaDataFile(any(), any(), any());
            TableMetadata metadataV1 = spyOps.current();
            SortOrder dataSort = SortOrder.builderFor(baseTable.schema()).asc("data").build();
            TableMetadata metadataV2 = metadataV1.replaceSortOrder(dataSort);
            spyOps.commit(metadataV1, metadataV2);
          } catch (CommitFailedException e) {
            expectedException.set(e);
          } catch (Throwable e) {
            unexpectedException.set(e);
          }
        });
    executorService.shutdown();
    if (!executorService.awaitTermination(610, TimeUnit.SECONDS)) {
      executorService.shutdownNow();
    }
    assertThat(expectedException.get()).isNotNull();
    assertThat(unexpectedException.get()).isNull();
    assertThatThrownBy(
            () -> {
              throw expectedException.get();
            })
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining(
            "Can not commit newMetaData because version [3] has already been committed.");
    assertThat(((HadoopTableOperations) baseTable.operations()).findVersion()).isEqualTo(3);
  }

  @Test
  public void testConcurrentCommitAndRejectCommitAlreadyExistsVersion()
      throws InterruptedException {
    table.newFastAppend().appendFile(FILE_A).commit();
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    AtomicReference<Throwable> unexpectedException = new AtomicReference<>(null);
    AtomicInteger commitTimes = new AtomicInteger(0);
    int maxCommitTimes = 20;
    CountDownLatch countDownLatch = new CountDownLatch(5);
    CountDownLatch countDownLatch2 = new CountDownLatch(1);
    executorService.execute(
        () -> {
          try {
            BaseTable baseTable = (BaseTable) table;
            HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
            HadoopTableOperations spyOps = spy(tableOperations);
            doNothing().when(spyOps).tryLock(any(), any());
            doAnswer(
                    x -> {
                      countDownLatch2.countDown();
                      countDownLatch.await();
                      return x.callRealMethod();
                    })
                .when(spyOps)
                .commitNewVersion(any(), any(), any(), any(), anyBoolean());
            assertCommitFail(
                baseTable, spyOps, CommitFailedException.class, "Version 3 already exists");
          } catch (Throwable e) {
            unexpectedException.set(e);
            throw new RuntimeException(e);
          }
        });

    executorService.execute(
        () -> {
          try {
            countDownLatch2.await();
            for (; commitTimes.get() < maxCommitTimes; commitTimes.incrementAndGet()) {
              table.newFastAppend().appendFile(FILE_A).commit();
              TimeUnit.SECONDS.sleep(1);
              countDownLatch.countDown();
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    executorService.shutdown();
    if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
      executorService.shutdownNow();
    }
    assertThat(unexpectedException.get()).isNull();
    assertThat(commitTimes.get()).isEqualTo(maxCommitTimes);
  }

  @Test
  public void testRejectCommitAlreadyExistsVersionWithUsingObjectStore()
      throws InterruptedException {
    // Since java.io.UnixFileSystem.rename overwrites existing files and we currently share the same
    // memory locks. So we can use the local file system to simulate the use of object storage.
    table.updateProperties().set(TableProperties.OBJECT_STORE_ENABLED, "true").commit();
    table.newFastAppend().appendFile(FILE_A).commit();
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    AtomicReference<Throwable> unexpectedException = new AtomicReference<>(null);
    AtomicInteger commitTimes = new AtomicInteger(0);
    int maxCommitTimes = 20;
    CountDownLatch countDownLatch = new CountDownLatch(5);
    CountDownLatch countDownLatch2 = new CountDownLatch(1);
    executorService.execute(
        () -> {
          try {
            BaseTable baseTable = (BaseTable) table;
            HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
            HadoopTableOperations spyOps = spy(tableOperations);
            doAnswer(
                    x -> {
                      countDownLatch2.countDown();
                      countDownLatch.await();
                      return x.callRealMethod();
                    })
                .when(spyOps)
                .tryLock(any(), any());
            assertCommitFail(
                baseTable, spyOps, CommitFailedException.class, "Version 4 already exists");
          } catch (Throwable e) {
            unexpectedException.set(e);
            throw new RuntimeException(e);
          }
        });

    executorService.execute(
        () -> {
          try {
            countDownLatch2.await();
            for (; commitTimes.get() < maxCommitTimes; commitTimes.incrementAndGet()) {
              table.newFastAppend().appendFile(FILE_A).commit();
              TimeUnit.SECONDS.sleep(1);
              countDownLatch.countDown();
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    executorService.shutdown();
    if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
      executorService.shutdownNow();
    }
    assertThat(unexpectedException.get()).isNull();
    assertThat(commitTimes.get()).isEqualTo(maxCommitTimes);
  }

  @Test
  public void testConcurrentCommitAndRejectTooOldCommit() throws InterruptedException {
    // Too-old-commit: commitVersion < currentMaxVersion - METADATA_PREVIOUS_VERSIONS_MAX
    table.newFastAppend().appendFile(FILE_A).commit();
    table.updateProperties().set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "2").commit();
    table
        .updateProperties()
        .set(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true")
        .commit();
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    AtomicReference<Throwable> unexpectedException = new AtomicReference<>(null);
    AtomicInteger commitTimes = new AtomicInteger(0);
    int maxCommitTimes = 20;
    CountDownLatch countDownLatch = new CountDownLatch(5);
    CountDownLatch countDownLatch2 = new CountDownLatch(1);
    CountDownLatch countDownLatch3 = new CountDownLatch(1);
    executorService.execute(
        () -> {
          try {
            BaseTable baseTable = (BaseTable) table;
            HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
            HadoopTableOperations spyOps = spy(tableOperations);
            doNothing().when(spyOps).tryLock(any(), any());
            doAnswer(
                    x -> {
                      countDownLatch2.countDown();
                      countDownLatch.await();
                      return x.callRealMethod();
                    })
                .when(spyOps)
                .commitNewVersion(any(), any(), any(), any(), anyBoolean());
            assertCommitFail(
                baseTable,
                spyOps,
                CommitFailedException.class,
                "Cannot commit version [5] because it is smaller or much larger than the current latest version [9].");
            countDownLatch3.countDown();
          } catch (Throwable e) {
            unexpectedException.set(e);
            throw new RuntimeException(e);
          }
        });

    executorService.execute(
        () -> {
          try {
            countDownLatch2.await();
            for (; commitTimes.get() < maxCommitTimes; commitTimes.incrementAndGet()) {
              table.newFastAppend().appendFile(FILE_A).commit();
              countDownLatch.countDown();
              if (countDownLatch.getCount() == 0) {
                countDownLatch3.await();
              }
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    executorService.shutdown();
    if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
      executorService.shutdownNow();
    }
    assertThat(unexpectedException.get()).isNull();
    assertThat(commitTimes.get()).isEqualTo(maxCommitTimes);
  }

  @Test
  public void testRejectTooOldCommitWithUsingObjectStore() throws InterruptedException {
    // Since java.io.UnixFileSystem.rename overwrites existing files and we currently share the same
    // memory locks. So we can use the local file system to simulate the use of object storage.
    table.updateProperties().set(TableProperties.OBJECT_STORE_ENABLED, "true").commit();
    table.newFastAppend().appendFile(FILE_A).commit();
    table.updateProperties().set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "2").commit();
    table
        .updateProperties()
        .set(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true")
        .commit();
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    AtomicReference<Throwable> unexpectedException = new AtomicReference<>(null);
    AtomicInteger commitTimes = new AtomicInteger(0);
    int maxCommitTimes = 20;
    CountDownLatch countDownLatch = new CountDownLatch(5);
    CountDownLatch countDownLatch2 = new CountDownLatch(1);
    executorService.execute(
        () -> {
          try {
            BaseTable baseTable = (BaseTable) table;
            HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
            HadoopTableOperations spyOps = spy(tableOperations);
            doNothing().when(spyOps).tryLock(any(), any());
            doAnswer(
                    x -> {
                      countDownLatch2.countDown();
                      countDownLatch.await();
                      return x.callRealMethod();
                    })
                .when(spyOps)
                .tryLock(any(), any());
            assertCommitFail(
                baseTable,
                spyOps,
                CommitFailedException.class,
                "Cannot commit version [6] because it is smaller or much larger than the current latest version [10].");
          } catch (Throwable e) {
            unexpectedException.set(e);
            throw new RuntimeException(e);
          }
        });

    executorService.execute(
        () -> {
          try {
            countDownLatch2.await();
            for (; commitTimes.get() < maxCommitTimes; commitTimes.incrementAndGet()) {
              table.newFastAppend().appendFile(FILE_A).commit();
              countDownLatch.countDown();
              TimeUnit.SECONDS.sleep(1);
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    executorService.shutdown();
    if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
      executorService.shutdownNow();
    }
    assertThat(unexpectedException.get()).isNull();
    assertThat(commitTimes.get()).isEqualTo(maxCommitTimes);
  }

  @Test
  public void testConcurrentCommitAndRejectDirtyCommit() throws InterruptedException {
    // At the end of the commit, if we realize that it is a dirty commit, we should fail the commit.
    table.newFastAppend().appendFile(FILE_A).commit();
    table.updateProperties().set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "2").commit();
    table
        .updateProperties()
        .set(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true")
        .commit();
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    AtomicReference<Throwable> unexpectedException = new AtomicReference<>(null);
    AtomicInteger commitTimes = new AtomicInteger(0);
    CountDownLatch countDownLatch = new CountDownLatch(5);
    CountDownLatch countDownLatch2 = new CountDownLatch(1);
    int maxCommitTimes = 20;
    executorService.execute(
        () -> {
          try {
            BaseTable baseTable = (BaseTable) table;
            HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
            HadoopTableOperations spyOps = spy(tableOperations);
            TableMetadata metadataV1 = spyOps.current();
            SortOrder dataSort = SortOrder.builderFor(baseTable.schema()).asc("data").build();
            TableMetadata metadataV2 = metadataV1.replaceSortOrder(dataSort);
            doNothing().when(spyOps).tryLock(any(), any());
            doAnswer(
                    invocation -> {
                      countDownLatch.await();
                      return invocation.callRealMethod();
                    })
                .when(spyOps)
                .renameMetaDataFileAndCheck(any(), any(), any(), anyBoolean());
            countDownLatch2.countDown();
            assertThatThrownBy(() -> spyOps.commit(metadataV1, metadataV2))
                .isInstanceOf(CommitStateUnknownException.class)
                .hasMessageContaining(
                    "Commit rejected by server!The current commit version [5] is much smaller than the latest version [9]");
          } catch (Throwable e) {
            unexpectedException.set(e);
            throw new RuntimeException(e);
          }
        });

    executorService.execute(
        () -> {
          try {
            countDownLatch2.await();
            for (; commitTimes.get() < maxCommitTimes; commitTimes.incrementAndGet()) {
              table.newFastAppend().appendFile(FILE_A).commit();
              TimeUnit.SECONDS.sleep(1);
              countDownLatch.countDown();
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    executorService.shutdown();
    if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
      executorService.shutdownNow();
    }
    assertThat(commitTimes.get()).isEqualTo(maxCommitTimes);
    assertThat(unexpectedException.get()).isNull();
  }

  @Test
  public void testCleanTooOldDirtyCommit() throws InterruptedException {
    // If there are dirty commits in the metadata for some reason, we need to clean them up.
    table.newFastAppend().appendFile(FILE_A).commit();
    table.updateProperties().set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "2").commit();
    table
        .updateProperties()
        .set(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true")
        .commit();
    ExecutorService executorService = Executors.newFixedThreadPool(8);
    AtomicReference<Exception> unexpectedException = new AtomicReference<>(null);
    AtomicInteger commitTimes = new AtomicInteger(0);
    int maxCommitTimes = 20;
    BaseTable baseTable = (BaseTable) table;
    HadoopTableOperations tableOperations = (HadoopTableOperations) baseTable.operations();
    HadoopTableOperations spyOps = spy(tableOperations);
    CountDownLatch countDownLatch = new CountDownLatch(5);
    CountDownLatch countDownLatch2 = new CountDownLatch(1);
    AtomicReference<File> dirtyCommitFile = new AtomicReference<>(null);
    executorService.execute(
        () -> {
          try {
            TableMetadata metadataV1 = spyOps.current();
            int currentVersion = spyOps.findVersion();
            int nextVersion = currentVersion + 1;
            SortOrder dataSort = SortOrder.builderFor(baseTable.schema()).asc("data").build();
            TableMetadata metadataV2 = metadataV1.replaceSortOrder(dataSort);
            doNothing().when(spyOps).tryLock(any(), any());
            doAnswer(
                    invocation -> {
                      countDownLatch2.countDown();
                      countDownLatch.await();
                      return invocation.callRealMethod();
                    })
                .when(spyOps)
                .renameMetaDataFileAndCheck(any(), any(), any(), anyBoolean());
            doNothing().when(spyOps).fastFailIfDirtyCommit(anyInt(), anyInt(), any(), any());
            spyOps.commit(metadataV1, metadataV2);
            Path metadataFile = spyOps.getMetadataFile(nextVersion);
            dirtyCommitFile.set(new File(metadataFile.toUri()));
          } catch (Exception e) {
            unexpectedException.set(e);
            throw new RuntimeException(e);
          }
        });

    executorService.execute(
        () -> {
          try {
            countDownLatch2.await();
            for (; commitTimes.get() < maxCommitTimes; commitTimes.incrementAndGet()) {
              table.newFastAppend().appendFile(FILE_A).commit();
              TimeUnit.SECONDS.sleep(1);
              countDownLatch.countDown();
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    executorService.shutdown();
    if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
      executorService.shutdownNow();
    }

    assertThat(unexpectedException.get()).isNull();
    assertThat(dirtyCommitFile.get()).isNotNull();
    assertThat(dirtyCommitFile.get().exists()).isEqualTo(true);
    long ttl = 30L * 3600 * 24 * 1000;
    assertThat(dirtyCommitFile.get().setLastModified(System.currentTimeMillis() - ttl)).isTrue();
    table.newFastAppend().appendFile(FILE_A).commit();
    assertThat(dirtyCommitFile.get().exists()).isEqualTo(false);
  }

  // Always returns false when trying to acquire
  static class NoLockManager implements LockManager {

    @Override
    public boolean acquire(String entityId, String ownerId) {
      return false;
    }

    @Override
    public boolean release(String entityId, String ownerId) {
      return false;
    }

    @Override
    public void close() throws Exception {}

    @Override
    public void initialize(Map<String, String> properties) {}
  }
}
