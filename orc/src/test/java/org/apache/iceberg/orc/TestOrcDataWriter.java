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
package org.apache.iceberg.orc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.StripeInformation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestOrcDataWriter {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "binary", Types.BinaryType.get()));

  private List<Record> records;

  @TempDir private File temp;

  @BeforeEach
  public void createRecords() {
    GenericRecord record = GenericRecord.create(SCHEMA);

    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    builder.add(record.copy(ImmutableMap.of("id", 1L, "data", "a")));
    builder.add(record.copy(ImmutableMap.of("id", 2L, "data", "b")));
    builder.add(record.copy(ImmutableMap.of("id", 3L, "data", "c")));
    builder.add(record.copy(ImmutableMap.of("id", 4L, "data", "d")));
    builder.add(record.copy(ImmutableMap.of("id", 5L, "data", "e")));

    this.records = builder.build();
  }

  private List<Long> stripeOffsetsFromReader(DataFile dataFile) throws IOException {
    return stripeOffsetsFromReader(dataFile, OrcFile.readerOptions(new Configuration()));
  }

  private List<Long> stripeOffsetsFromReader(DataFile dataFile, OrcFile.ReaderOptions options)
      throws IOException {
    return OrcFile.createReader(new Path(dataFile.path().toString()), options).getStripes().stream()
        .map(StripeInformation::getOffset)
        .collect(Collectors.toList());
  }

  @Test
  public void testDataWriter() throws IOException {
    OutputFile file = Files.localOutput(temp);

    SortOrder sortOrder = SortOrder.builderFor(SCHEMA).withOrderId(10).asc("id").build();

    DataWriter<Record> dataWriter =
        ORC.writeData(file)
            .schema(SCHEMA)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .withSortOrder(sortOrder)
            .build();

    try {
      for (Record record : records) {
        dataWriter.write(record);
      }
    } finally {
      dataWriter.close();
    }

    DataFile dataFile = dataWriter.toDataFile();
    assertThat(dataFile.splitOffsets()).isEqualTo(stripeOffsetsFromReader(dataFile));
    assertThat(dataFile.format()).isEqualTo(FileFormat.ORC);
    assertThat(dataFile.content()).isEqualTo(FileContent.DATA);
    assertThat(dataFile.recordCount()).isEqualTo(records.size());
    assertThat(dataFile.partition().size()).isEqualTo(0);
    assertThat(dataFile.sortOrderId()).isEqualTo(sortOrder.orderId());
    assertThat(dataFile.keyMetadata()).isNull();
    List<Record> writtenRecords;
    try (CloseableIterable<Record> reader =
        ORC.read(file.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(SCHEMA, fileSchema))
            .build()) {
      writtenRecords = Lists.newArrayList(reader);
    }
    assertThat(writtenRecords).as("Written records should match").isEqualTo(records);
  }

  @Test
  public void testUsingFileIO() throws IOException {
    // When files other than HadoopInputFile and HadoopOutputFile are supplied the location
    // is used to determine the corresponding FileSystem class based on the scheme in case of
    // local files that would be the LocalFileSystem. To prevent this we use the Proxy classes to
    // use a scheme `dummy` that is not handled. Note that Hadoop 2.7.3 throws IOException
    // while latest Hadoop versions throw UnsupportedFileSystemException (extends IOException)
    ProxyOutputFile outFile = new ProxyOutputFile(Files.localOutput(temp));
    assertThatThrownBy(() -> new Path(outFile.location()).getFileSystem(new Configuration()))
        .isInstanceOf(IOException.class)
        .hasMessageStartingWith("No FileSystem for scheme");

    // Given that FileIO is now handled there is no determination of FileSystem based on scheme
    // but instead operations are handled by the InputFileSystem and OutputFileSystem that wrap
    // the InputFile and OutputFile correspondingly. Both write and read should be successful.
    DataWriter<Record> dataWriter =
        ORC.writeData(outFile)
            .schema(SCHEMA)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build();

    try {
      for (Record record : records) {
        dataWriter.write(record);
      }
    } finally {
      dataWriter.close();
    }

    DataFile dataFile = dataWriter.toDataFile();
    OrcFile.ReaderOptions options =
        OrcFile.readerOptions(new Configuration())
            .filesystem(new FileIOFSUtil.InputFileSystem(outFile.toInputFile()))
            .maxLength(outFile.toInputFile().getLength());
    assertThat(dataFile.splitOffsets()).isEqualTo(stripeOffsetsFromReader(dataFile, options));
    assertThat(dataFile.format()).isEqualTo(FileFormat.ORC);
    assertThat(dataFile.content()).isEqualTo(FileContent.DATA);
    assertThat(dataFile.recordCount()).isEqualTo(records.size());
    assertThat(dataFile.partition().size()).isEqualTo(0);
    assertThat(dataFile.keyMetadata()).isNull();
    List<Record> writtenRecords;
    try (CloseableIterable<Record> reader =
        ORC.read(outFile.toInputFile())
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(SCHEMA, fileSchema))
            .build()) {
      writtenRecords = Lists.newArrayList(reader);
    }
    assertThat(writtenRecords).as("Written records should match").isEqualTo(records);
  }

  private static class ProxyInputFile implements InputFile {
    private final InputFile inputFile;

    private ProxyInputFile(InputFile inputFile) {
      this.inputFile = inputFile;
    }

    @Override
    public long getLength() {
      return inputFile.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      return inputFile.newStream();
    }

    @Override
    public String location() {
      return "dummy://" + inputFile.location();
    }

    @Override
    public boolean exists() {
      return inputFile.exists();
    }
  }

  private static class ProxyOutputFile implements OutputFile {
    private final OutputFile outputFile;

    private ProxyOutputFile(OutputFile outputFile) {
      this.outputFile = outputFile;
    }

    @Override
    public PositionOutputStream create() {
      return outputFile.create();
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return outputFile.createOrOverwrite();
    }

    @Override
    public String location() {
      return "dummy://" + outputFile.location();
    }

    @Override
    public InputFile toInputFile() {
      return new ProxyInputFile(outputFile.toInputFile());
    }
  }
}
