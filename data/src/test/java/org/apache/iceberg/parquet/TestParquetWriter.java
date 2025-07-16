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
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.BaseFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FanoutDataWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.WriterTestBase;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestParquetWriter extends WriterTestBase<Record> {

  private static final long TARGET_FILE_SIZE = 128L * 1024 * 1024;
  private static final Logger LOG = LoggerFactory.getLogger(TestParquetWriter.class);

  private final FileFormat fileFormat = FileFormat.PARQUET;

  private OutputFileFactory fileFactory = null;
  private static volatile boolean fail = true;
  private static final List<File> PARQUET_FILE_LIST =
      Collections.synchronizedList(Lists.newArrayList());

  @BeforeEach
  void before() {
    // A lot is already done in TestBase.
    this.metadataDir = new File(tableDir, "metadata");
    this.fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).ioSupplier(() -> IO).build();
  }

  @Test
  void testParquetWriterWithFailingIO() throws IOException {
    table.updateSpec().addField(Expressions.ref("data")).commit();

    FileWriterFactory<Record> writerFactory = newWriterFactory(table.schema());
    FanoutDataWriter<Record> writer =
        new FanoutDataWriter<>(writerFactory, fileFactory, IO, TARGET_FILE_SIZE);

    PartitionSpec spec = table.spec();

    writer.write(toRow(1, "aaa"), spec, partitionKey(spec, "aaa"));
    writer.write(toRow(3, "bbb"), spec, partitionKey(spec, "bbb"));
    writer.write(toRow(2, "aaa"), spec, partitionKey(spec, "aaa"));
    writer.write(toRow(4, "bbb"), spec, partitionKey(spec, "bbb"));
    writer.write(toRow(5, "ccc"), spec, partitionKey(spec, "ccc"));

    try {
      writer.close();
    } catch (IOException | UncheckedIOException e) {
      LOG.warn("Error closing writer", e);
    }

    // The data of the first parquet-file was first written into a byte-buffer and on close should
    // have been written to file.
    // But that failed, so the file should not exist:
    assertThat(PARQUET_FILE_LIST.get(0)).doesNotExist();

    // Simulate that the network is up again:
    fail = false;
    // Try again:
    LOG.info("Trying to close {} again.", writer);
    try {
      writer.close();
    } catch (UncheckedIOException e) {
      // This error comes from the underlying ParquetFileWriter, which seems to have a similar
      // problem with setting the internal state too early.
      if (!"Failed to flush row group".equals(e.getMessage())) {
        throw e;
      }
      // Otherwise, we log and ignore it for now:
      LOG.warn("The underlying ParquetFileWriter is in an invalid state now:", e);
    }

    // The writer (or at least one underlying writer) should not be closed:
    assertThatThrownBy(writer::result)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot get result from unclosed writer");
  }

  @Override
  protected FileWriterFactory<Record> newWriterFactory(
      Schema dataSchema,
      List<Integer> equalityFieldIds,
      Schema equalityDeleteRowSchema,
      Schema positionDeleteRowSchema) {
    return new MyFileWriterFactory(table);
  }

  protected Record toRow(Integer id, String data) {
    GenericRecord result = GenericRecord.create(table.schema().asStruct());
    result.set(0, id);
    result.set(1, data);
    return result;
  }

  private static class MyFileWriterFactory extends BaseFileWriterFactory<Record> {
    protected MyFileWriterFactory(Table table) {
      super(
          table,
          FileFormat.PARQUET,
          table.schema(),
          table.sortOrder(),
          FileFormat.PARQUET,
          null,
          table.schema(),
          table.sortOrder(),
          table.schema());
    }

    @Override
    protected void configureDataWrite(Parquet.DataWriteBuilder builder) {
      builder.createWriterFunc(GenericParquetWriter::create);
    }

    @Override
    protected void configureDataWrite(Avro.DataWriteBuilder builder) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void configureEqualityDelete(Avro.DeleteWriteBuilder builder) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void configurePositionDelete(Avro.DeleteWriteBuilder builder) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void configureEqualityDelete(Parquet.DeleteWriteBuilder builder) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void configurePositionDelete(Parquet.DeleteWriteBuilder builder) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void configureDataWrite(ORC.DataWriteBuilder builder) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void configureEqualityDelete(ORC.DeleteWriteBuilder builder) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void configurePositionDelete(ORC.DeleteWriteBuilder builder) {
      throw new UnsupportedOperationException();
    }
  }

  static final FileIO IO = new FailingLocalFileIo();

  private static class FailingLocalFileIo extends TestTables.LocalFileIO {
    @Override
    public OutputFile newOutputFile(String path) {
      return new FailingLocalOutputFile(Paths.get(path).toAbsolutePath().toFile());
    }
  }

  private static class FailingLocalOutputFile implements OutputFile {
    private final File file;

    private FailingLocalOutputFile(File file) {
      this.file = file;
    }

    @Override
    public PositionOutputStream create() {
      if (file.exists()) {
        throw new AlreadyExistsException("File already exists: %s", file);
      }

      if (!file.getParentFile().isDirectory() && !file.getParentFile().mkdirs()) {
        throw new RuntimeIOException(
            "Failed to create the file's directory at %s.", file.getParentFile().getAbsolutePath());
      }

      PARQUET_FILE_LIST.add(file);

      return new PositionFileOutputStream(file);
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      if (file.exists() && !file.delete()) {
        throw new RuntimeIOException("Failed to delete: %s", file);
      }

      return create();
    }

    @Override
    public String location() {
      return file.toString();
    }

    @Override
    public InputFile toInputFile() {
      return Files.localInput(file);
    }

    @Override
    public String toString() {
      return location();
    }
  }

  /**
   * Keeps all data in memory, before writing it to file when calling close().
   *
   * <p>This mimics the behaviour of writing to S3 over network - files are only uploaded when
   * closing the output.
   */
  private static class PositionFileOutputStream extends PositionOutputStream {
    private final ByteArrayOutputStream contents = new ByteArrayOutputStream();
    private final File file;

    private PositionFileOutputStream(File file) {
      this.file = file;
    }

    @Override
    public long getPos() {
      return contents.size();
    }

    @Override
    public void write(byte[] b) throws IOException {
      contents.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
      contents.write(b, off, len);
    }

    @Override
    public void write(int b) {
      contents.write(b);
    }

    @Override
    public void close() throws IOException {
      if (fail) {
        LOG.warn("Failure while closing file {}.", file);
        throw new IOException("Boooom!");
      }
      final RandomAccessFile stream = new RandomAccessFile(file, "rw");
      // Eventually, we're flushing the byte-buffer into the file:
      stream.write(contents.toByteArray());
      stream.close();
    }
  }
}
