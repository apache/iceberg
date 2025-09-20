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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestBase;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestPartitionedWriterWithUnPartitionedData extends TestBase {
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
    this.metadataDir = new File(tableDir, "metadata");

    this.table = create(SCHEMA, PartitionSpec.unpartitioned());

    this.fileFactory =
        OutputFileFactory.builderFor(table, 1, 1)
            .ioSupplier(InMemoryFileIO::new)
            .format(format)
            .build();

    this.appenderFactory = new GenericAppenderFactory(table.schema(), table.spec()) {};

    table.updateProperties().defaultFormat(format).commit();
  }

  private Record createRecord(Integer id, String data) {
    return gRecord.copy("id", id, "data", data);
  }

  @TestTemplate
  public void testDataFilePathForUnPartitionedTable() throws IOException {
    List<Record> records = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      records.add(createRecord(i, "aaa"));
    }

    WriteResult result;
    try (TestGenericPartitionedWriter taskWriter = createGenericPartitionedWriter(10)) {
      for (Record record : records) {
        taskWriter.write(record);
      }
      result = taskWriter.complete();
      assertThat(result.dataFiles()).hasSize(1);
      Optional<DataFile> dataFile = Arrays.stream(result.dataFiles()).findFirst();
      assertThat(dataFile.isPresent()).isTrue();
      assertThat(dataFile.get().location().contains("/data//")).isFalse();
    }
  }

  private TestGenericPartitionedWriter createGenericPartitionedWriter(long targetFileSizeInBytes) {
    return new TestGenericPartitionedWriter(
        this.table,
        table.spec(),
        format,
        appenderFactory,
        fileFactory,
        table.io(),
        targetFileSizeInBytes);
  }

  private static class TestGenericPartitionedWriter extends PartitionedFanoutWriter<Record> {

    private final PartitionKey partitionKey;

    private TestGenericPartitionedWriter(
        Table table,
        PartitionSpec spec,
        FileFormat format,
        FileAppenderFactory<Record> appenderFactory,
        OutputFileFactory fileFactory,
        FileIO io,
        long targetFileSizeInBytes) {
      super(spec, format, appenderFactory, fileFactory, io, targetFileSizeInBytes);
      this.partitionKey = new PartitionKey(spec, table.schema());
    }

    @Override
    public PartitionKey partition(Record row) {
      this.partitionKey.partition(row);
      return this.partitionKey;
    }
  }
}
