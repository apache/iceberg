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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Tasks;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

public class TestHadoopCommits extends HadoopTableTestBase {

  @Test
  public void testCreateTable() throws Exception {
    PartitionSpec expectedSpec = PartitionSpec.builderFor(TABLE_SCHEMA).bucket("data", 16).build();

    Assertions.assertThat(table.schema().asStruct())
        .as("Table schema should match schema with reassigned ids")
        .isEqualTo(TABLE_SCHEMA.asStruct());
    Assertions.assertThat(table.spec())
        .as("Table partition spec should match with reassigned ids")
        .isEqualTo(expectedSpec);

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    Assertions.assertThat(tasks).as("Should not create any scan tasks").isEmpty();
    Assertions.assertThat(tableDir).as("Table location should exist").exists();
    Assertions.assertThat(metadataDir).as("Should create metadata folder").exists().isDirectory();
    Assertions.assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
    Assertions.assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();
    Assertions.assertThat(versionHintFile).as("Should create version hint file").exists();
    Assertions.assertThat(readVersionHint())
        .as("Should write the current version to the hint file")
        .isEqualTo(1);
    List<File> manifests = listManifestFiles();
    Assertions.assertThat(manifests).as("Should contain 0 Avro manifest files").isEmpty();
  }

  @Test
  public void testSchemaUpdate() throws Exception {
    Assertions.assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
    Assertions.assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();

    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    Assertions.assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
    Assertions.assertThat(readVersionHint())
        .as("Should write the current version to the hint file")
        .isEqualTo(2);
    Assertions.assertThat(table.schema().asStruct())
        .as("Table schema should match schema with reassigned ids")
        .isEqualTo(UPDATED_SCHEMA.asStruct());

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    Assertions.assertThat(tasks).as("Should not create any scan tasks").isEmpty();

    List<File> manifests = listManifestFiles();
    Assertions.assertThat(manifests).as("Should contain 0 Avro manifest files").isEmpty();
  }

  @Test
  public void testSchemaUpdateComplexType() throws Exception {
    Assertions.assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
    Assertions.assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();

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

    Assertions.assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
    Assertions.assertThat(readVersionHint())
        .as("Should write the current version to the hint file")
        .isEqualTo(2);
    Assertions.assertThat(table.schema().asStruct())
        .as("Table schema should match schema with reassigned ids")
        .isEqualTo(updatedSchema.asStruct());

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    Assertions.assertThat(tasks).as("Should not create any scan tasks").isEmpty();

    List<File> manifests = listManifestFiles();
    Assertions.assertThat(manifests).as("Should contain 0 Avro manifest files").isEmpty();
  }

  @Test
  public void testSchemaUpdateIdentifierFields() throws Exception {
    Assertions.assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
    Assertions.assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();

    Schema updatedSchema =
        new Schema(
            Lists.newArrayList(
                required(1, "id", Types.IntegerType.get(), "unique ID"),
                required(2, "data", Types.StringType.get())),
            Sets.newHashSet(1));

    table.updateSchema().setIdentifierFields("id").commit();

    Assertions.assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
    Assertions.assertThat(readVersionHint())
        .as("Should write the current version to the hint file")
        .isEqualTo(2);
    Assertions.assertThat(table.schema().asStruct())
        .as("Table schema should match schema with reassigned ids")
        .isEqualTo(updatedSchema.asStruct());
    Assertions.assertThat(table.schema().identifierFieldIds())
        .as("Identifier fields should match schema with reassigned ids")
        .isEqualTo(updatedSchema.identifierFieldIds());
  }

  @Test
  public void testFailedCommit() throws Exception {
    // apply the change to metadata without committing
    UpdateSchema update = table.updateSchema().addColumn("n", Types.IntegerType.get());
    update.apply();

    Assertions.assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
    Assertions.assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();

    version(2).createNewFile();

    Assertions.assertThatThrownBy(update::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessageStartingWith("Version 2 already exists");

    List<File> manifests = listManifestFiles();
    Assertions.assertThat(manifests).as("Should contain 0 Avro manifest files").isEmpty();
  }

  @Test
  public void testStaleMetadata() throws Exception {
    Table tableCopy = TABLES.load(tableLocation);

    Assertions.assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
    Assertions.assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();

    // prepare changes on the copy without committing
    UpdateSchema updateCopy = tableCopy.updateSchema().addColumn("m", Types.IntegerType.get());
    updateCopy.apply();

    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    Assertions.assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
    Assertions.assertThat(table.schema().asStruct())
        .as("Unmodified copy should be out of date after update")
        .isNotEqualTo(tableCopy.schema().asStruct());

    // update the table
    tableCopy.refresh();

    Assertions.assertThat(table.schema().asStruct())
        .as("Copy should be back in sync")
        .isEqualTo(tableCopy.schema().asStruct());

    Assertions.assertThatThrownBy(updateCopy::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Cannot commit changes based on stale table metadata");

    List<File> manifests = listManifestFiles();
    Assertions.assertThat(manifests).as("Should contain 0 Avro manifest files").isEmpty();
  }

  @Test
  public void testStaleVersionHint() throws Exception {
    Table stale = TABLES.load(tableLocation);

    Assertions.assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
    Assertions.assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();

    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    Assertions.assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
    Assertions.assertThat(readVersionHint())
        .as("Should write the current version to the hint file")
        .isEqualTo(2);
    Assertions.assertThat(stale.schema().asStruct())
        .as("Stable table schema should not match")
        .isNotEqualTo(UPDATED_SCHEMA.asStruct());

    // roll the version hint back to 1
    replaceVersionHint(1);

    Table reloaded = TABLES.load(tableLocation);
    Assertions.assertThat(reloaded.schema().asStruct())
        .as("Updated schema for newly loaded table should match")
        .isEqualTo(UPDATED_SCHEMA.asStruct());

    stale.refresh();
    Assertions.assertThat(reloaded.schema().asStruct())
        .as("Refreshed schema for stale table should match")
        .isEqualTo(UPDATED_SCHEMA.asStruct());
  }

  @Test
  public void testFastAppend() throws Exception {
    // first append
    table.newFastAppend().appendFile(FILE_A).commit();

    Assertions.assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
    Assertions.assertThat(readVersionHint())
        .as("Should write the current version to the hint file")
        .isEqualTo(2);

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    Assertions.assertThat(tasks).as("Should scan 1 file").hasSize(1);

    List<File> manifests = listManifestFiles();
    Assertions.assertThat(manifests).as("Should contain only one Avro manifest file").hasSize(1);

    // second append
    table.newFastAppend().appendFile(FILE_B).commit();

    Assertions.assertThat(version(3)).as("Should create v3 for the update").exists().isFile();
    Assertions.assertThat(readVersionHint())
        .as("Should write the current version to the hint file")
        .isEqualTo(3);

    tasks = Lists.newArrayList(table.newScan().planFiles());
    Assertions.assertThat(tasks).as("Should scan 2 files").hasSize(2);
    Assertions.assertThat(listManifestFiles())
        .as("Should contain 2 Avro manifest files")
        .hasSize(2);

    TableMetadata metadata = readMetadataVersion(3);
    Assertions.assertThat(metadata.currentSnapshot().allManifests(table.io()))
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
    Assertions.assertThat(tasks).as("Should scan 3 files").hasSize(3);

    Assertions.assertThat(listManifestFiles())
        .as("Should contain 3 Avro manifest files")
        .hasSize(3);

    TableMetadata metadata = readMetadataVersion(5);
    Assertions.assertThat(metadata.currentSnapshot().allManifests(table.io()))
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
    Assertions.assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
    Assertions.assertThat(version(2)).as("Should not create v2 or newer versions").doesNotExist();
    Assertions.assertThat(table).isInstanceOf(BaseTable.class);
    BaseTable baseTable = (BaseTable) table;
    // use v1 metafile as the test rename destination.
    TableMetadata meta1 = baseTable.operations().current();

    // create v2 metafile as base. This is solely for the convenience of rename testing later
    // (so that we have 2 valid and different metadata files, which will reach the rename part
    // during commit)
    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();
    Assertions.assertThat(version(2)).as("Should create v2 for the update").exists().isFile();
    Assertions.assertThat(readVersionHint())
        .as("Should write the current version to the hint file")
        .isEqualTo(2);

    // mock / spy the classes for testing
    TableOperations tops = baseTable.operations();
    Assertions.assertThat(tops).isInstanceOf(HadoopTableOperations.class);
    HadoopTableOperations spyOps = Mockito.spy((HadoopTableOperations) tops);

    // inject the mockFS into the TableOperations
    doReturn(mockFs).when(spyOps).getFileSystem(any(), any());
    Assertions.assertThatThrownBy(() -> spyOps.commit(tops.current(), meta1))
        .isInstanceOf(CommitFailedException.class);

    // Verifies that there is no temporary metadata.json files left on rename failures.
    Set<String> actual =
        listMetadataJsonFiles().stream().map(File::getName).collect(Collectors.toSet());
    Set<String> expected = Sets.newHashSet("v1.metadata.json", "v2.metadata.json");
    Assertions.assertThat(actual)
        .as("only v1 and v2 metadata.json should exist.")
        .isEqualTo(expected);
  }

  @Test
  public void testCanReadOldCompressedManifestFiles() throws Exception {
    Assertions.assertThat(version(1)).as("Should create v1 metadata").exists().isFile();

    // do a file append
    table.newAppend().appendFile(FILE_A).commit();

    // since we don't generate old file extensions anymore, let's convert existing metadata to old
    // .metadata.json.gz
    // to test backwards compatibility
    rewriteMetadataAsGzipWithOldExtension();

    List<File> metadataFiles = listMetadataJsonFiles();

    Assertions.assertThat(metadataFiles).as("Should have two versions").hasSize(2);
    Assertions.assertThat(metadataFiles.stream().map(File::getName))
        .as("Metadata should be compressed with old format.")
        .allMatch(f -> f.endsWith(".metadata.json.gz"));

    Table reloaded = TABLES.load(tableLocation);

    List<FileScanTask> tasks = Lists.newArrayList(reloaded.newScan().planFiles());
    Assertions.assertThat(tasks).as("Should scan 1 files").hasSize(1);
  }

  @Test
  public void testConcurrentFastAppends(@TempDir File dir) throws Exception {
    Assertions.assertThat(version(1)).as("Should create v1 metadata").exists().isFile();
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
                while (barrier.get() < numCommittedFiles * threadsCount) {
                  try {
                    Thread.sleep(10);
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                }
                tableWithHighRetries.newFastAppend().appendFile(file).commit();
                barrier.incrementAndGet();
              }
            });

    tableWithHighRetries.refresh();
    Assertions.assertThat(Lists.newArrayList(tableWithHighRetries.snapshots()))
        .hasSize(threadsCount * numberOfCommitedFilesPerThread);
  }
}
