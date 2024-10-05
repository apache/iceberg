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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestBaseTaskWriter extends TestBase {
  private static final int FORMAT_V2 = 2;

  private final GenericRecord gRecord = GenericRecord.create(SCHEMA);

  private OutputFileFactory fileFactory = null;
  private FileAppenderFactory<Record> appenderFactory = null;

  @Parameter(index = 1)
  protected FileFormat format;

  @Parameters(name = "formatVersion = {0}, FileFormat = {1}")
  protected static List<Object> parameters() {
    return Arrays.asList(
        new Object[] {FORMAT_V2, FileFormat.AVRO},
        new Object[] {FORMAT_V2, FileFormat.ORC},
        new Object[] {FORMAT_V2, FileFormat.PARQUET});
  }

  @Override
  @BeforeEach
  public void setupTable() throws IOException {
    this.tableDir = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableDir.delete()).isTrue(); // created by table create

    this.metadataDir = new File(tableDir, "metadata");

    this.table = create(SCHEMA, PartitionSpec.unpartitioned());
    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(format).build();

    int firstFieldId = table.schema().findField("id").fieldId();
    int secondFieldId = table.schema().findField("data").fieldId();
    this.appenderFactory =
        new GenericAppenderFactory(
            table.schema(),
            table.spec(),
            new int[] {firstFieldId, secondFieldId},
            table.schema(),
            null);

    table.updateProperties().defaultFormat(format).commit();
  }

  private Record createRecord(Integer id, String data) {
    return gRecord.copy("id", id, "data", data);
  }

  @TestTemplate
  public void testWriteZeroRecord() throws IOException {
    try (TestTaskWriter writer = createTaskWriter(128 * 1024 * 1024)) {
      writer.close();

      WriteResult result = writer.complete();
      assertThat(result.dataFiles()).hasSize(0);
      assertThat(result.deleteFiles()).hasSize(0);

      writer.close();
      result = writer.complete();
      assertThat(result.dataFiles()).hasSize(0);
      assertThat(result.deleteFiles()).hasSize(0);
    }
  }

  @TestTemplate
  public void testAbort() throws IOException {
    List<Record> records = Lists.newArrayList();
    for (int i = 0; i < 2000; i++) {
      records.add(createRecord(i, "aaa"));
    }

    List<Path> files;
    try (TestTaskWriter taskWriter = createTaskWriter(4)) {
      for (Record record : records) {
        taskWriter.write(record);
        taskWriter.delete(record);
      }

      // Close the current opened files.
      taskWriter.close();

      // Assert the current data file count.
      files =
          Files.list(Paths.get(tableDir.getPath(), "data"))
              .filter(p -> !p.toString().endsWith(".crc"))
              .collect(Collectors.toList());
      assertThat(files).as("Should have 4 files but the files are: " + files).hasSize(4);

      // Abort to clean all delete files and data files.
      taskWriter.abort();
    }

    for (Path path : files) {
      assertThat(path).doesNotExist();
    }
  }

  @TestTemplate
  public void testRollIfExceedTargetFileSize() throws IOException {
    List<Record> records = Lists.newArrayListWithCapacity(8000);
    for (int i = 0; i < 2000; i++) {
      records.add(createRecord(i, "aaa"));
      records.add(createRecord(i, "bbb"));
      records.add(createRecord(i, "ccc"));
      records.add(createRecord(i, "ddd"));
    }

    WriteResult result;
    try (TaskWriter<Record> taskWriter = createTaskWriter(4)) {
      for (Record record : records) {
        taskWriter.write(record);
      }

      result = taskWriter.complete();
      assertThat(result.dataFiles()).hasSize(8);
      assertThat(result.deleteFiles()).hasSize(0);
    }

    RowDelta rowDelta = table.newRowDelta();
    Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
    rowDelta.commit();

    List<Record> expected = Lists.newArrayList();
    try (TestTaskWriter taskWriter = createTaskWriter(3)) {
      for (Record record : records) {
        // ex: UPSERT <0, 'aaa'> to <0, 'AAA'>
        taskWriter.delete(record);

        int id = record.get(0, Integer.class);
        String data = record.get(1, String.class);
        Record newRecord = createRecord(id, data.toUpperCase(Locale.ROOT));
        expected.add(newRecord);
        taskWriter.write(newRecord);
      }

      result = taskWriter.complete();
      assertThat(result.dataFiles()).hasSize(8);
      assertThat(result.deleteFiles()).hasSize(8);
    }

    rowDelta = table.newRowDelta();
    Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
    Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
    rowDelta.commit();

    assertThat(actualRowSet("*"))
        .as("Should have expected records")
        .isEqualTo(expectedRowSet(expected));
  }

  private StructLikeSet expectedRowSet(Iterable<Record> records) {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    records.forEach(set::add);
    return set;
  }

  private StructLikeSet actualRowSet(String... columns) throws IOException {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    try (CloseableIterable<Record> reader = IcebergGenerics.read(table).select(columns).build()) {
      reader.forEach(set::add);
    }
    return set;
  }

  private TestTaskWriter createTaskWriter(long targetFileSize) {
    return new TestTaskWriter(
        table.spec(), format, appenderFactory, fileFactory, table.io(), targetFileSize);
  }

  private static class TestTaskWriter extends BaseTaskWriter<Record> {

    private final RollingFileWriter dataWriter;
    private final RollingEqDeleteWriter deleteWriter;

    private TestTaskWriter(
        PartitionSpec spec,
        FileFormat format,
        FileAppenderFactory<Record> appenderFactory,
        OutputFileFactory fileFactory,
        FileIO io,
        long targetFileSize) {
      super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
      this.dataWriter = new RollingFileWriter(null);
      this.deleteWriter = new RollingEqDeleteWriter(null);
    }

    @Override
    public void write(Record row) throws IOException {
      dataWriter.write(row);
    }

    void delete(Record row) throws IOException {
      deleteWriter.write(row);
    }

    @Override
    public void close() throws IOException {
      if (dataWriter != null) {
        dataWriter.close();
      }
      if (deleteWriter != null) {
        deleteWriter.close();
      }
    }
  }
}
