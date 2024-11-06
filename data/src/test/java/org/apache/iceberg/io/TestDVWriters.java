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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class TestDVWriters<T> extends WriterTestBase<T> {

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(new Object[] {3});
  }

  private OutputFileFactory fileFactory = null;

  protected abstract StructLikeSet toSet(Iterable<T> records);

  protected FileFormat dataFormat() {
    return FileFormat.PARQUET;
  }

  @Override
  @BeforeEach
  public void setupTable() throws Exception {
    this.table = create(SCHEMA, PartitionSpec.unpartitioned());
    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
  }

  @TestTemplate
  public void testBasicDVs() throws IOException {
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
