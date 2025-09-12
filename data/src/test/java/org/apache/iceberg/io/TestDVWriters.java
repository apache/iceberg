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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class TestDVWriters<T> extends WriterTestBase<T> {

  @Parameters(name = "formatVersion = {0}")
  protected static List<Integer> parameters() {
    return TestHelpers.V2_AND_ABOVE;
  }

  private OutputFileFactory fileFactory = null;
  private OutputFileFactory parquetFileFactory = null;

  protected abstract StructLikeSet toSet(Iterable<T> records);

  protected FileFormat dataFormat() {
    return FileFormat.PARQUET;
  }

  @Override
  @BeforeEach
  public void setupTable() throws Exception {
    this.table = create(SCHEMA, PartitionSpec.unpartitioned());
    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
    this.parquetFileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PARQUET).build();
  }

  @TestTemplate
  public void testBasicDVs() throws IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);

    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());

    // add the first data file
    List<T> rows1 = ImmutableList.of(toRow(1, "aaa"), toRow(2, "aaa"), toRow(11, "aaa"));
    DataFile dataFile1 = writeData(writerFactory, fileFactory, rows1, table.spec(), null);
    table.newFastAppend().appendFile(dataFile1).commit();

    // add the second data file
    List<T> rows2 = ImmutableList.of(toRow(3, "aaa"), toRow(4, "aaa"), toRow(12, "aaa"));
    DataFile dataFile2 = writeData(writerFactory, fileFactory, rows2, table.spec(), null);
    table.newFastAppend().appendFile(dataFile2).commit();

    // init the DV writer
    DVFileWriter dvWriter =
        new BaseDVFileWriter(fileFactory, new PreviousDeleteLoader(table, ImmutableMap.of()));

    // write deletes for both data files (the order of records is mixed)
    dvWriter.delete(dataFile1.location(), 1L, table.spec(), null);
    dvWriter.delete(dataFile2.location(), 0L, table.spec(), null);
    dvWriter.delete(dataFile1.location(), 0L, table.spec(), null);
    dvWriter.delete(dataFile2.location(), 1L, table.spec(), null);
    dvWriter.close();

    // verify the writer result
    DeleteWriteResult result = dvWriter.result();
    assertThat(result.deleteFiles()).hasSize(2);
    assertThat(result.referencedDataFiles())
        .hasSize(2)
        .contains(dataFile1.location())
        .contains(dataFile2.location());
    assertThat(result.referencesDataFiles()).isTrue();

    // commit the deletes
    commit(result);

    // verify correctness
    assertRows(ImmutableList.of(toRow(11, "aaa"), toRow(12, "aaa")));
  }

  @TestTemplate
  public void testRewriteDVs() throws IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);

    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());

    // add a data file with 3 data records
    List<T> rows = ImmutableList.of(toRow(1, "aaa"), toRow(2, "aaa"), toRow(3, "aaa"));
    DataFile dataFile = writeData(writerFactory, parquetFileFactory, rows, table.spec(), null);
    table.newFastAppend().appendFile(dataFile).commit();

    // write the first DV
    DVFileWriter dvWriter1 =
        new BaseDVFileWriter(fileFactory, new PreviousDeleteLoader(table, ImmutableMap.of()));
    dvWriter1.delete(dataFile.location(), 1L, table.spec(), null);
    dvWriter1.close();

    // validate the writer result
    DeleteWriteResult result1 = dvWriter1.result();
    assertThat(result1.deleteFiles()).hasSize(1);
    assertThat(result1.referencedDataFiles()).containsOnly(dataFile.location());
    assertThat(result1.referencesDataFiles()).isTrue();
    assertThat(result1.rewrittenDeleteFiles()).isEmpty();

    // commit the first DV
    commit(result1);
    assertThat(table.currentSnapshot().addedDeleteFiles(table.io())).hasSize(1);
    assertThat(table.currentSnapshot().removedDeleteFiles(table.io())).isEmpty();

    // verify correctness after committing the first DV
    assertRows(ImmutableList.of(toRow(1, "aaa"), toRow(3, "aaa")));

    // write the second DV, merging with the first one
    DeleteFile dv1 = Iterables.getOnlyElement(result1.deleteFiles());
    DVFileWriter dvWriter2 =
        new BaseDVFileWriter(
            fileFactory,
            new PreviousDeleteLoader(table, ImmutableMap.of(dataFile.location(), dv1)));
    dvWriter2.delete(dataFile.location(), 2L, table.spec(), null);
    dvWriter2.close();

    // validate the writer result
    DeleteWriteResult result2 = dvWriter2.result();
    assertThat(result2.deleteFiles()).hasSize(1);
    assertThat(result2.referencedDataFiles()).containsOnly(dataFile.location());
    assertThat(result2.referencesDataFiles()).isTrue();
    assertThat(result2.rewrittenDeleteFiles()).hasSize(1);

    // replace DVs
    commit(result2);
    assertThat(table.currentSnapshot().addedDeleteFiles(table.io())).hasSize(1);
    assertThat(table.currentSnapshot().removedDeleteFiles(table.io())).hasSize(1);

    // verify correctness after replacing DVs
    assertRows(ImmutableList.of(toRow(1, "aaa")));
  }

  @TestTemplate
  public void testRewriteFileScopedPositionDeletes() throws IOException {
    assumeThat(formatVersion).isEqualTo(2);

    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());

    // add a data file with 3 records
    List<T> rows = ImmutableList.of(toRow(1, "aaa"), toRow(2, "aaa"), toRow(3, "aaa"));
    DataFile dataFile = writeData(writerFactory, parquetFileFactory, rows, table.spec(), null);
    table.newFastAppend().appendFile(dataFile).commit();

    // add a file-scoped position delete file
    DeleteFile deleteFile =
        writePositionDeletes(writerFactory, ImmutableList.of(Pair.of(dataFile.location(), 0L)));
    table.newRowDelta().addDeletes(deleteFile).commit();

    // verify correctness after adding the file-scoped position delete
    assertRows(ImmutableList.of(toRow(2, "aaa"), toRow(3, "aaa")));

    // upgrade the table to V3 to enable DVs
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();

    // write a DV, merging with the file-scoped position delete
    DVFileWriter dvWriter =
        new BaseDVFileWriter(
            fileFactory,
            new PreviousDeleteLoader(table, ImmutableMap.of(dataFile.location(), deleteFile)));
    dvWriter.delete(dataFile.location(), 1L, table.spec(), null);
    dvWriter.close();

    // validate the writer result
    DeleteWriteResult result = dvWriter.result();
    assertThat(result.deleteFiles()).hasSize(1);
    assertThat(result.referencedDataFiles()).containsOnly(dataFile.location());
    assertThat(result.referencesDataFiles()).isTrue();
    assertThat(result.rewrittenDeleteFiles()).hasSize(1);

    // replace the position delete file with the DV
    commit(result);
    assertThat(table.currentSnapshot().addedDeleteFiles(table.io())).hasSize(1);
    assertThat(table.currentSnapshot().removedDeleteFiles(table.io())).hasSize(1);

    // verify correctness
    assertRows(ImmutableList.of(toRow(3, "aaa")));
  }

  @TestTemplate
  public void testNoPuffinFileCreatedWhenNoDeletesWritten() throws IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);

    DVFileWriter dvWriter =
        new BaseDVFileWriter(fileFactory, new PreviousDeleteLoader(table, ImmutableMap.of()));

    // close without writing any deletes
    dvWriter.close();

    // verify the writer result has no delete files
    DeleteWriteResult result = dvWriter.result();
    assertThat(result.deleteFiles()).isEmpty();
    assertThat(result.referencedDataFiles()).isEmpty();
    assertThat(result.referencesDataFiles()).isFalse();
    assertThat(result.rewrittenDeleteFiles()).isEmpty();

    // verify that the data directory doesn't exist, implying no puffin files were created
    File dir = new File(table.location(), "data");
    assertThat(dir).doesNotExist();
  }

  @TestTemplate
  public void testApplyPartitionScopedPositionDeletes() throws IOException {
    assumeThat(formatVersion).isEqualTo(2);

    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());

    // add the first data file with 3 records
    List<T> rows1 = ImmutableList.of(toRow(1, "aaa"), toRow(2, "aaa"), toRow(3, "aaa"));
    DataFile dataFile1 = writeData(writerFactory, parquetFileFactory, rows1, table.spec(), null);
    table.newFastAppend().appendFile(dataFile1).commit();

    // add the second data file with 3 records
    List<T> rows2 = ImmutableList.of(toRow(4, "aaa"), toRow(5, "aaa"), toRow(6, "aaa"));
    DataFile dataFile2 = writeData(writerFactory, parquetFileFactory, rows2, table.spec(), null);
    table.newFastAppend().appendFile(dataFile2).commit();

    // add a position delete file with deletes for both data files
    DeleteFile deleteFile =
        writePositionDeletes(
            writerFactory,
            ImmutableList.of(
                Pair.of(dataFile1.location(), 0L),
                Pair.of(dataFile1.location(), 1L),
                Pair.of(dataFile2.location(), 0L)));
    table.newRowDelta().addDeletes(deleteFile).commit();

    // verify correctness with the position delete file
    assertRows(ImmutableList.of(toRow(3, "aaa"), toRow(5, "aaa"), toRow(6, "aaa")));

    // upgrade the table to V3 to enable DVs
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();

    // write a DV, applying old positions but keeping the position delete file in place
    DVFileWriter dvWriter =
        new BaseDVFileWriter(
            fileFactory,
            new PreviousDeleteLoader(table, ImmutableMap.of(dataFile2.location(), deleteFile)));
    dvWriter.delete(dataFile2.location(), 1L, table.spec(), null);
    dvWriter.close();

    // validate the writer result
    DeleteWriteResult result = dvWriter.result();
    assertThat(result.deleteFiles()).hasSize(1);
    assertThat(result.referencedDataFiles()).containsOnly(dataFile2.location());
    assertThat(result.referencesDataFiles()).isTrue();
    assertThat(result.rewrittenDeleteFiles()).isEmpty();
    DeleteFile dv = Iterables.getOnlyElement(result.deleteFiles());

    // commit the DV, ensuring the position delete file remains
    commit(result);
    assertThat(table.currentSnapshot().addedDeleteFiles(table.io())).hasSize(1);
    assertThat(table.currentSnapshot().removedDeleteFiles(table.io())).isEmpty();

    // verify correctness with DVs and position delete files
    assertRows(ImmutableList.of(toRow(3, "aaa"), toRow(6, "aaa")));

    // verify the position delete file applies only to the data file without the DV
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        DeleteFile taskDeleteFile = Iterables.getOnlyElement(task.deletes());
        if (task.file().location().equals(dataFile1.location())) {
          assertThat(taskDeleteFile.location()).isEqualTo(deleteFile.location());
        } else {
          assertThat(taskDeleteFile.location()).isEqualTo(dv.location());
        }
      }
    }
  }

  private void commit(DeleteWriteResult result) {
    Snapshot startSnapshot = table.currentSnapshot();
    RowDelta rowDelta = table.newRowDelta();
    result.rewrittenDeleteFiles().forEach(rowDelta::removeDeletes);
    result.deleteFiles().forEach(rowDelta::addDeletes);
    if (startSnapshot != null) {
      rowDelta.validateFromSnapshot(startSnapshot.snapshotId());
    }
    rowDelta.commit();
  }

  private void assertRows(Iterable<T> expectedRows) throws IOException {
    assertThat(actualRowSet("*")).isEqualTo(toSet(expectedRows));
  }

  private DeleteFile writePositionDeletes(
      FileWriterFactory<T> writerFactory, List<Pair<String, Long>> deletes) throws IOException {
    EncryptedOutputFile file = parquetFileFactory.newOutputFile(table.spec(), null);
    PositionDeleteWriter<T> writer =
        writerFactory.newPositionDeleteWriter(file, table.spec(), null);
    PositionDelete<T> posDelete = PositionDelete.create();

    try (PositionDeleteWriter<T> closableWriter = writer) {
      for (Pair<String, Long> delete : deletes) {
        closableWriter.write(posDelete.set(delete.first(), delete.second()));
      }
    }

    return writer.toDeleteFile();
  }

  private static class PreviousDeleteLoader implements Function<String, PositionDeleteIndex> {
    private final Map<String, DeleteFile> deleteFiles;
    private final DeleteLoader deleteLoader;

    PreviousDeleteLoader(Table table, Map<String, DeleteFile> deleteFiles) {
      this.deleteFiles = deleteFiles;
      this.deleteLoader = new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile));
    }

    @Override
    public PositionDeleteIndex apply(String path) {
      DeleteFile deleteFile = deleteFiles.get(path);
      if (deleteFile == null) {
        return null;
      }
      return deleteLoader.loadPositionDeletes(ImmutableList.of(deleteFile), path);
    }
  }
}
