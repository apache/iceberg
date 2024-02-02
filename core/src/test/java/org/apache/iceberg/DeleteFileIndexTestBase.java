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

import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DeleteFileIndex.EqualityDeletes;
import org.apache.iceberg.DeleteFileIndex.PositionDeletes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.CharSequenceSet;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class DeleteFileIndexTestBase<
        ScanT extends Scan<ScanT, T, G>, T extends ScanTask, G extends ScanTaskGroup<T>>
    extends TestBase {

  @Parameters(name = "formatVersion = {0}")
  public static List<Object> parameters() {
    return Arrays.asList(2);
  }

  static final DeleteFile FILE_A_POS_1 =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofPositionDeletes()
          .withPath("/path/to/data-a-pos-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartition(FILE_A.partition())
          .withRecordCount(1)
          .build();

  static final DeleteFile FILE_A_EQ_1 =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofEqualityDeletes()
          .withPath("/path/to/data-a-eq-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartition(FILE_A.partition())
          .withRecordCount(1)
          .build();

  private static DataFile unpartitionedFile(PartitionSpec spec) {
    return DataFiles.builder(spec)
        .withPath("/path/to/data-unpartitioned.parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  private static DeleteFile unpartitionedPosDeletes(PartitionSpec spec) {
    return FileMetadata.deleteFileBuilder(spec)
        .ofPositionDeletes()
        .withPath(UUID.randomUUID() + "/path/to/data-unpartitioned-pos-deletes.parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  private static DeleteFile partitionedPosDeletes(PartitionSpec spec, StructLike partition) {
    return FileMetadata.deleteFileBuilder(spec)
        .ofPositionDeletes()
        .withPartition(partition)
        .withPath(UUID.randomUUID() + "/path/to/data-partitioned-pos-deletes.parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  private static DeleteFile unpartitionedEqDeletes(PartitionSpec spec) {
    return FileMetadata.deleteFileBuilder(spec)
        .ofEqualityDeletes()
        .withPath(UUID.randomUUID() + "/path/to/data-unpartitioned-eq-deletes.parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  private static DeleteFile partitionedEqDeletes(PartitionSpec spec, StructLike partition) {
    return FileMetadata.deleteFileBuilder(spec)
        .ofEqualityDeletes()
        .withPartition(partition)
        .withPath(UUID.randomUUID() + "/path/to/data-partitioned-eq-deletes.parquet")
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  @SuppressWarnings("unchecked")
  private static <F extends ContentFile<F>> F withDataSequenceNumber(long seq, F file) {
    BaseFile<F> baseFile = (BaseFile<F>) file;
    baseFile.setDataSequenceNumber(seq);
    return file;
  }

  protected abstract ScanT newScan(Table table);

  @TestTemplate
  public void testMinSequenceNumberFilteringForFiles() {
    PartitionSpec partSpec = PartitionSpec.unpartitioned();

    DeleteFile[] deleteFiles = {
      withDataSequenceNumber(4, unpartitionedEqDeletes(partSpec)),
      withDataSequenceNumber(6, unpartitionedEqDeletes(partSpec))
    };

    DeleteFileIndex index =
        DeleteFileIndex.builderFor(Arrays.asList(deleteFiles))
            .specsById(ImmutableMap.of(partSpec.specId(), partSpec, 1, SPEC))
            .afterSequenceNumber(4)
            .build();

    DataFile file = unpartitionedFile(partSpec);

    assertThat(index.forDataFile(0, file)).as("Only one delete file should apply").hasSize(1);
  }

  @TestTemplate
  public void testUnpartitionedDeletes() {
    PartitionSpec partSpec = PartitionSpec.unpartitioned();

    DeleteFile[] deleteFiles = {
      withDataSequenceNumber(4, unpartitionedEqDeletes(partSpec)),
      withDataSequenceNumber(6, unpartitionedEqDeletes(partSpec)),
      withDataSequenceNumber(5, unpartitionedPosDeletes(partSpec)),
      withDataSequenceNumber(6, unpartitionedPosDeletes(partSpec))
    };

    DeleteFileIndex index =
        DeleteFileIndex.builderFor(Arrays.asList(deleteFiles))
            .specsById(ImmutableMap.of(partSpec.specId(), partSpec, 1, SPEC))
            .build();

    DataFile unpartitionedFile = unpartitionedFile(partSpec);
    assertThat(index.forDataFile(0, unpartitionedFile))
        .as("All deletes should apply to seq 0")
        .isEqualTo(deleteFiles);

    assertThat(index.forDataFile(3, unpartitionedFile))
        .as("All deletes should apply to seq 3")
        .isEqualTo(deleteFiles);

    assertThat(index.forDataFile(4, unpartitionedFile))
        .as("All deletes should apply to seq 4")
        .isEqualTo(Arrays.copyOfRange(deleteFiles, 1, 4));

    assertThat(index.forDataFile(4, unpartitionedFile))
        .as("Last 3 deletes should apply to seq 4")
        .isEqualTo(Arrays.copyOfRange(deleteFiles, 1, 4));

    assertThat(index.forDataFile(5, unpartitionedFile))
        .as("Last 3 deletes should apply to seq 5")
        .isEqualTo(Arrays.copyOfRange(deleteFiles, 1, 4));

    assertThat(index.forDataFile(6, unpartitionedFile))
        .as("Last delete should apply to seq 6")
        .isEqualTo(Arrays.copyOfRange(deleteFiles, 3, 4));

    assertThat(index.forDataFile(7, unpartitionedFile))
        .as("No deletes should apply to seq 7")
        .isEqualTo(new DataFile[0]);

    assertThat(index.forDataFile(10, unpartitionedFile))
        .as("No deletes should apply to seq 10")
        .isEqualTo(new DataFile[0]);

    // copy file A with a different spec ID
    DataFile partitionedFileA = FILE_A.copy();
    ((BaseFile<?>) partitionedFileA).setSpecId(1);
    assertThat(index.forDataFile(0, partitionedFileA))
        .as("All global equality deletes should apply to a partitioned file")
        .isEqualTo(Arrays.copyOfRange(deleteFiles, 0, 2));
  }

  @TestTemplate
  public void testPartitionedDeleteIndex() {
    DeleteFile[] deleteFiles = {
      withDataSequenceNumber(4, partitionedEqDeletes(SPEC, FILE_A.partition())),
      withDataSequenceNumber(6, partitionedEqDeletes(SPEC, FILE_A.partition())),
      withDataSequenceNumber(5, partitionedPosDeletes(SPEC, FILE_A.partition())),
      withDataSequenceNumber(6, partitionedPosDeletes(SPEC, FILE_A.partition()))
    };

    DeleteFileIndex index =
        DeleteFileIndex.builderFor(Arrays.asList(deleteFiles))
            .specsById(ImmutableMap.of(SPEC.specId(), SPEC, 1, PartitionSpec.unpartitioned()))
            .build();

    assertThat(index.forDataFile(0, FILE_A))
        .as("All deletes should apply to seq 0")
        .isEqualTo(deleteFiles);

    assertThat(index.forDataFile(3, FILE_A))
        .as("All deletes should apply to seq 3")
        .isEqualTo(deleteFiles);

    assertThat(index.forDataFile(4, FILE_A))
        .as("Last 3 deletes should apply to seq 4")
        .isEqualTo(Arrays.copyOfRange(deleteFiles, 1, 4));

    assertThat(index.forDataFile(5, FILE_A))
        .as("Last 3 deletes should apply to seq 5")
        .isEqualTo(Arrays.copyOfRange(deleteFiles, 1, 4));

    assertThat(index.forDataFile(6, FILE_A))
        .as("Last delete should apply to seq 6")
        .isEqualTo(Arrays.copyOfRange(deleteFiles, 3, 4));

    assertThat(index.forDataFile(7, FILE_A))
        .as("No deletes should apply to seq 7")
        .isEqualTo(new DataFile[0]);

    assertThat(index.forDataFile(10, FILE_A))
        .as("No deletes should apply to seq 10")
        .isEqualTo(new DataFile[0]);

    assertThat(index.forDataFile(0, FILE_B))
        .as("No deletes should apply to FILE_B, partition not in index")
        .hasSize(0);

    assertThat(index.forDataFile(0, FILE_C))
        .as("No deletes should apply to FILE_C, no indexed delete files")
        .hasSize(0);

    DataFile unpartitionedFileA = FILE_A.copy();
    ((BaseFile<?>) unpartitionedFileA).setSpecId(1);
    assertThat(index.forDataFile(0, unpartitionedFileA))
        .as("No deletes should apply to FILE_A with a different specId")
        .hasSize(0);
  }

  @TestTemplate
  public void testUnpartitionedTableScan() throws IOException {
    File location = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(location.delete()).isTrue();

    Table unpartitioned =
        TestTables.create(location, "unpartitioned", SCHEMA, PartitionSpec.unpartitioned(), 2);

    DataFile unpartitionedFile = unpartitionedFile(unpartitioned.spec());
    unpartitioned.newAppend().appendFile(unpartitionedFile).commit();

    // add a delete file
    DeleteFile unpartitionedPosDeletes = unpartitionedPosDeletes(unpartitioned.spec());
    unpartitioned.newRowDelta().addDeletes(unpartitionedPosDeletes).commit();

    List<T> tasks = Lists.newArrayList(newScan(unpartitioned).planFiles().iterator());
    assertThat(tasks).as("Should have one task").hasSize(1);

    FileScanTask task = (FileScanTask) tasks.get(0);
    assertThat(task.file().path())
        .as("Should have the correct data file path")
        .isEqualTo(unpartitionedFile.path());
    assertThat(task.deletes()).as("Should have one associated delete file").hasSize(1);
    assertThat(task.deletes().get(0).path())
        .as("Should have expected delete file")
        .isEqualTo(unpartitionedPosDeletes.path());

    // add a second delete file
    DeleteFile unpartitionedEqDeletes = unpartitionedEqDeletes(unpartitioned.spec());
    unpartitioned.newRowDelta().addDeletes(unpartitionedEqDeletes).commit();

    tasks = Lists.newArrayList(newScan(unpartitioned).planFiles().iterator());
    task = (FileScanTask) tasks.get(0);
    assertThat(task.file().path())
        .as("Should have the correct data file path")
        .isEqualTo(unpartitionedFile.path());
    assertThat(task.deletes()).as("Should have two associated delete files").hasSize(2);
    assertThat(Sets.newHashSet(Iterables.transform(task.deletes(), ContentFile::path)))
        .as("Should have expected delete files")
        .isEqualTo(Sets.newHashSet(unpartitionedPosDeletes.path(), unpartitionedEqDeletes.path()));
  }

  @TestTemplate
  public void testPartitionedTableWithPartitionPosDeletes() {
    table.newAppend().appendFile(FILE_A).commit();

    table.newRowDelta().addDeletes(FILE_A_POS_1).commit();

    List<T> tasks = Lists.newArrayList(newScan(table).planFiles().iterator());
    assertThat(tasks).as("Should have one task").hasSize(1);

    FileScanTask task = (FileScanTask) tasks.get(0);
    assertThat(task.file().path())
        .as("Should have the correct data file path")
        .isEqualTo(FILE_A.path());
    assertThat(task.deletes()).as("Should have one associated delete file").hasSize(1);
    assertThat(task.deletes().get(0).path())
        .as("Should have only pos delete file")
        .isEqualTo(FILE_A_POS_1.path());
  }

  @TestTemplate
  public void testPartitionedTableWithPartitionEqDeletes() {
    table.newAppend().appendFile(FILE_A).commit();

    table.newRowDelta().addDeletes(FILE_A_EQ_1).commit();

    List<T> tasks = Lists.newArrayList(newScan(table).planFiles().iterator());
    assertThat(tasks).as("Should have one task").hasSize(1);

    FileScanTask task = (FileScanTask) tasks.get(0);
    assertThat(task.file().path())
        .as("Should have the correct data file path")
        .isEqualTo(FILE_A.path());
    assertThat(task.deletes()).as("Should have one associated delete file").hasSize(1);
    assertThat(task.deletes().get(0).path())
        .as("Should have only pos delete file")
        .isEqualTo(FILE_A_EQ_1.path());
  }

  @TestTemplate
  public void testPartitionedTableWithUnrelatedPartitionDeletes() {
    table.newAppend().appendFile(FILE_B).commit();

    table.newRowDelta().addDeletes(FILE_A_POS_1).addDeletes(FILE_A_EQ_1).commit();

    List<T> tasks = Lists.newArrayList(newScan(table).planFiles().iterator());
    assertThat(tasks).as("Should have one task").hasSize(1);

    FileScanTask task = (FileScanTask) tasks.get(0);
    assertThat(task.file().path())
        .as("Should have the correct data file path")
        .isEqualTo(FILE_B.path());
    assertThat(task.deletes()).as("Should have no delete files to apply").hasSize(0);
  }

  @TestTemplate
  public void testPartitionedTableWithOlderPartitionDeletes() {
    table.newRowDelta().addDeletes(FILE_A_POS_1).addDeletes(FILE_A_EQ_1).commit();

    table.newAppend().appendFile(FILE_A).commit();

    List<T> tasks = Lists.newArrayList(newScan(table).planFiles().iterator());
    assertThat(tasks).as("Should have one task").hasSize(1);

    FileScanTask task = (FileScanTask) tasks.get(0);
    assertThat(task.file().path())
        .as("Should have the correct data file path")
        .isEqualTo(FILE_A.path());
    assertThat(task.deletes()).as("Should have no delete files to apply").hasSize(0);
  }

  @TestTemplate
  public void testPartitionedTableScanWithGlobalDeletes() {
    table.newAppend().appendFile(FILE_A).commit();

    TableMetadata base = table.ops().current();
    table.ops().commit(base, base.updatePartitionSpec(PartitionSpec.unpartitioned()));

    // add unpartitioned equality and position deletes, but only equality deletes are global
    DeleteFile unpartitionedEqDeletes = unpartitionedEqDeletes(table.spec());
    table
        .newRowDelta()
        .addDeletes(unpartitionedPosDeletes(table.spec()))
        .addDeletes(unpartitionedEqDeletes)
        .commit();

    List<T> tasks = Lists.newArrayList(newScan(table).planFiles().iterator());
    assertThat(tasks).as("Should have one task").hasSize(1);

    FileScanTask task = (FileScanTask) tasks.get(0);
    assertThat(task.file().path())
        .as("Should have the correct data file path")
        .isEqualTo(FILE_A.path());
    assertThat(task.deletes()).as("Should have one associated delete file").hasSize(1);
    assertThat(task.deletes().get(0).path())
        .as("Should have expected delete file")
        .isEqualTo(unpartitionedEqDeletes.path());
  }

  @TestTemplate
  public void testPartitionedTableScanWithGlobalAndPartitionDeletes() {
    table.newAppend().appendFile(FILE_A).commit();

    table.newRowDelta().addDeletes(FILE_A_EQ_1).commit();

    TableMetadata base = table.ops().current();
    table.ops().commit(base, base.updatePartitionSpec(PartitionSpec.unpartitioned()));

    // add unpartitioned equality and position deletes, but only equality deletes are global
    DeleteFile unpartitionedEqDeletes = unpartitionedEqDeletes(table.spec());
    table
        .newRowDelta()
        .addDeletes(unpartitionedPosDeletes(table.spec()))
        .addDeletes(unpartitionedEqDeletes)
        .commit();

    List<T> tasks = Lists.newArrayList(newScan(table).planFiles().iterator());
    assertThat(tasks).as("Should have one task").hasSize(1);

    FileScanTask task = (FileScanTask) tasks.get(0);
    assertThat(task.file().path())
        .as("Should have the correct data file path")
        .isEqualTo(FILE_A.path());
    assertThat(task.deletes()).as("Should have two associated delete files").hasSize(2);
    assertThat(Sets.newHashSet(Iterables.transform(task.deletes(), ContentFile::path)))
        .as("Should have expected delete files")
        .isEqualTo(Sets.newHashSet(unpartitionedEqDeletes.path(), FILE_A_EQ_1.path()));
  }

  @TestTemplate
  public void testPartitionedTableSequenceNumbers() {
    table.newRowDelta().addRows(FILE_A).addDeletes(FILE_A_EQ_1).addDeletes(FILE_A_POS_1).commit();

    List<T> tasks = Lists.newArrayList(newScan(table).planFiles().iterator());
    assertThat(tasks).as("Should have one task").hasSize(1);

    FileScanTask task = (FileScanTask) tasks.get(0);
    assertThat(task.file().path())
        .as("Should have the correct data file path")
        .isEqualTo(FILE_A.path());
    assertThat(task.deletes()).as("Should have one associated delete file").hasSize(1);
    assertThat(task.deletes().get(0).path())
        .as("Should have only pos delete file")
        .isEqualTo(FILE_A_POS_1.path());
  }

  @TestTemplate
  public void testUnpartitionedTableSequenceNumbers() throws IOException {
    File location = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(location.delete()).isTrue();

    Table unpartitioned =
        TestTables.create(location, "unpartitioned", SCHEMA, PartitionSpec.unpartitioned(), 2);

    // add data, pos deletes, and eq deletes in the same sequence number
    // the position deletes will be applied to the data file, but the equality deletes will not
    DataFile unpartitionedFile = unpartitionedFile(unpartitioned.spec());
    DeleteFile unpartitionedPosDeleteFile = unpartitionedPosDeletes(unpartitioned.spec());
    unpartitioned
        .newRowDelta()
        .addRows(unpartitionedFile)
        .addDeletes(unpartitionedPosDeleteFile)
        .addDeletes(unpartitionedEqDeletes(unpartitioned.spec()))
        .commit();

    assertThat(
            unpartitioned
                .currentSnapshot()
                .deleteManifests(unpartitioned.io())
                .get(0)
                .addedFilesCount())
        .as("Table should contain 2 delete files")
        .isEqualTo(2);

    List<FileScanTask> tasks = Lists.newArrayList(unpartitioned.newScan().planFiles().iterator());
    assertThat(tasks).as("Should have one task").hasSize(1);

    FileScanTask task = tasks.get(0);
    assertThat(task.file().path())
        .as("Should have the correct data file path")
        .isEqualTo(unpartitionedFile.path());
    assertThat(task.deletes()).as("Should have one associated delete file").hasSize(1);
    assertThat(task.deletes().get(0).path())
        .as("Should have only pos delete file")
        .isEqualTo(unpartitionedPosDeleteFile.path());
  }

  @TestTemplate
  public void testPartitionedTableWithExistingDeleteFile() {
    table.updateProperties().set(TableProperties.MANIFEST_MERGE_ENABLED, "false").commit();

    table.newAppend().appendFile(FILE_A).commit();

    table.newRowDelta().addDeletes(FILE_A_EQ_1).commit();

    table.newRowDelta().addDeletes(FILE_A_POS_1).commit();

    table
        .updateProperties()
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1")
        .set(TableProperties.MANIFEST_MERGE_ENABLED, "true")
        .commit();

    assertThat(table.currentSnapshot().deleteManifests(table.io()))
        .as("Should have two delete manifests")
        .hasSize(2);

    // merge delete manifests
    table.newAppend().appendFile(FILE_B).commit();

    assertThat(table.currentSnapshot().deleteManifests(table.io()))
        .as("Should have one delete manifest")
        .hasSize(1);

    assertThat(
            table.currentSnapshot().deleteManifests(table.io()).get(0).addedFilesCount().intValue())
        .as("Should have zero added delete file")
        .isEqualTo(0);

    assertThat(
            table
                .currentSnapshot()
                .deleteManifests(table.io())
                .get(0)
                .deletedFilesCount()
                .intValue())
        .as("Should have zero deleted delete file")
        .isEqualTo(0);

    assertThat(
            table
                .currentSnapshot()
                .deleteManifests(table.io())
                .get(0)
                .existingFilesCount()
                .intValue())
        .as("Should have two existing delete files")
        .isEqualTo(2);

    List<T> tasks =
        Lists.newArrayList(
            newScan(table).filter(equal(bucket("data", BUCKETS_NUMBER), 0)).planFiles().iterator());
    assertThat(tasks).as("Should have one task").hasSize(1);

    FileScanTask task = (FileScanTask) tasks.get(0);
    assertThat(task.file().path())
        .as("Should have the correct data file path")
        .isEqualTo(FILE_A.path());
    assertThat(task.deletes()).as("Should have two associated delete files").hasSize(2);
    assertThat(Sets.newHashSet(Iterables.transform(task.deletes(), ContentFile::path)))
        .as("Should have expected delete files")
        .isEqualTo(Sets.newHashSet(FILE_A_EQ_1.path(), FILE_A_POS_1.path()));
  }

  @TestTemplate
  public void testPositionDeletesGroup() {
    DeleteFile file1 = withDataSequenceNumber(1, partitionedPosDeletes(SPEC, FILE_A.partition()));
    DeleteFile file2 = withDataSequenceNumber(2, partitionedPosDeletes(SPEC, FILE_A.partition()));
    DeleteFile file3 = withDataSequenceNumber(3, partitionedPosDeletes(SPEC, FILE_A.partition()));
    DeleteFile file4 = withDataSequenceNumber(4, partitionedPosDeletes(SPEC, FILE_A.partition()));

    PositionDeletes group = new PositionDeletes();
    group.add(file4);
    group.add(file2);
    group.add(file1);
    group.add(file3);

    // the group must not be empty
    assertThat(group.isEmpty()).isFalse();

    // all files must be reported as referenced
    CharSequenceSet paths =
        CharSequenceSet.of(Iterables.transform(group.referencedDeleteFiles(), ContentFile::path));
    assertThat(paths).contains(file1.path(), file2.path(), file3.path(), file4.path());

    // position deletes are indexed by their data sequence numbers
    // so that position deletes can apply to data files added in the same snapshot
    assertThat(group.filter(0)).isEqualTo(new DeleteFile[] {file1, file2, file3, file4});
    assertThat(group.filter(1)).isEqualTo(new DeleteFile[] {file1, file2, file3, file4});
    assertThat(group.filter(2)).isEqualTo(new DeleteFile[] {file2, file3, file4});
    assertThat(group.filter(3)).isEqualTo(new DeleteFile[] {file3, file4});
    assertThat(group.filter(4)).isEqualTo(new DeleteFile[] {file4});
    assertThat(group.filter(5)).isEqualTo(new DeleteFile[] {});

    // it should not be possible to add more elements upon indexing
    assertThatThrownBy(() -> group.add(file1)).isInstanceOf(IllegalStateException.class);
  }

  @TestTemplate
  public void testEqualityDeletesGroup() {
    DeleteFile file1 = withDataSequenceNumber(1, partitionedEqDeletes(SPEC, FILE_A.partition()));
    DeleteFile file2 = withDataSequenceNumber(2, partitionedEqDeletes(SPEC, FILE_A.partition()));
    DeleteFile file3 = withDataSequenceNumber(3, partitionedEqDeletes(SPEC, FILE_A.partition()));
    DeleteFile file4 = withDataSequenceNumber(4, partitionedEqDeletes(SPEC, FILE_A.partition()));

    EqualityDeletes group = new EqualityDeletes();
    group.add(SPEC, file4);
    group.add(SPEC, file2);
    group.add(SPEC, file1);
    group.add(SPEC, file3);

    // the group must not be empty
    assertThat(group.isEmpty()).isFalse();

    // all files must be reported as referenced
    CharSequenceSet paths =
        CharSequenceSet.of(Iterables.transform(group.referencedDeleteFiles(), ContentFile::path));
    assertThat(paths).contains(file1.path(), file2.path(), file3.path(), file4.path());

    // equality deletes are indexed by data sequence number - 1 to apply to next snapshots
    assertThat(group.filter(0, FILE_A)).isEqualTo(new DeleteFile[] {file1, file2, file3, file4});
    assertThat(group.filter(1, FILE_A)).isEqualTo(new DeleteFile[] {file2, file3, file4});
    assertThat(group.filter(2, FILE_A)).isEqualTo(new DeleteFile[] {file3, file4});
    assertThat(group.filter(3, FILE_A)).isEqualTo(new DeleteFile[] {file4});
    assertThat(group.filter(4, FILE_A)).isEqualTo(new DeleteFile[] {});

    // it should not be possible to add more elements upon indexing
    assertThatThrownBy(() -> group.add(SPEC, file1)).isInstanceOf(IllegalStateException.class);
  }
}
