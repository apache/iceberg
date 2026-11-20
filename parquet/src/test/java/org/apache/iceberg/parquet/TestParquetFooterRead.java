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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RandomInternalData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.InternalReader;
import org.apache.iceberg.data.parquet.InternalWriter;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.junit.jupiter.api.Test;

public class TestParquetFooterRead {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

  @Test
  public void testFooterReadWithSingleTailRead() throws IOException {
    byte[] bytes = write(SCHEMA, RandomInternalData.generate(SCHEMA, 100, 1376L));
    CountingInputFile file = new CountingInputFile(bytes);

    try (ParquetFileReader reader =
        ParquetIO.open(ParquetIO.file(file), ParquetReadOptions.builder().build())) {
      assertThat(reader.getRecordCount()).isEqualTo(100);
    }

    assertThat(file.tailReads).containsExactly(bytes.length);
    assertThat(file.sequentialReads).as("footer should not be read with seek and read").isZero();
  }

  @Test
  public void testFooterLargerThanInitialTailRead() throws IOException {
    // enough columns for the footer to exceed the initial tail read
    Schema wideSchema =
        new Schema(
            IntStream.rangeClosed(1, 2000)
                .mapToObj(id -> optional(id, "col_" + id, Types.LongType.get()))
                .collect(Collectors.toList()));
    byte[] bytes = write(wideSchema, RandomInternalData.generate(wideSchema, 10, 1376L));
    int footerLength =
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt(bytes.length - 8);
    assertThat(footerLength + 8).isGreaterThan(ParquetIO.FOOTER_READ_SIZE);

    CountingInputFile file = new CountingInputFile(bytes);
    try (ParquetFileReader reader =
        ParquetIO.open(ParquetIO.file(file), ParquetReadOptions.builder().build())) {
      assertThat(reader.getRecordCount()).isEqualTo(10);
      assertThat(reader.getFileMetaData().getSchema().getFieldCount()).isEqualTo(2000);
    }

    assertThat(file.tailReads).containsExactly(ParquetIO.FOOTER_READ_SIZE, footerLength + 8);
    assertThat(file.sequentialReads).isZero();
  }

  @Test
  public void testReadAboveEagerFetchThreshold() throws IOException {
    // large enough to skip eager fetching, so the footer and row groups are read remotely
    List<Record> expected = RandomInternalData.generate(SCHEMA, 100_000, 1376L);
    byte[] bytes = write(SCHEMA, expected);
    assertThat(bytes.length).isGreaterThan(1024 * 1024);

    CountingInputFile file = new CountingInputFile(bytes);
    List<Record> actual;
    try (CloseableIterable<Record> reader =
        Parquet.read(file)
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> InternalReader.create(SCHEMA, fileSchema))
            .build()) {
      actual = Lists.newArrayList(reader);
    }

    assertThat(actual).hasSameSizeAs(expected);
    assertThat(actual.get(0).get(0)).isEqualTo(expected.get(0).get(0));
    assertThat(actual.get(actual.size() - 1).get(0))
        .isEqualTo(expected.get(expected.size() - 1).get(0));
    assertThat(file.tailReads).containsExactly(ParquetIO.FOOTER_READ_SIZE);
    assertThat(file.positionalReads).as("row groups should be read with range reads").isPositive();
    assertThat(file.sequentialReads).isZero();
  }

  @Test
  public void testStreamWithoutRangeReads() throws IOException {
    byte[] bytes = write(SCHEMA, RandomInternalData.generate(SCHEMA, 100, 1376L));

    try (ParquetFileReader reader =
        ParquetIO.open(
            ParquetIO.file(new InMemoryInputFile(bytes)), ParquetReadOptions.builder().build())) {
      assertThat(reader.getRecordCount()).isEqualTo(100);
    }
  }

  @Test
  public void testNotAParquetFile() {
    byte[] bytes = new byte[1024];
    CountingInputFile file = new CountingInputFile(bytes);

    assertThatThrownBy(
            () -> ParquetIO.open(ParquetIO.file(file), ParquetReadOptions.builder().build()))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("is not a Parquet file");
    assertThat(file.closed).isTrue();
  }

  private static byte[] write(Schema schema, List<Record> records) throws IOException {
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    try (DataWriter<StructLike> writer =
        Parquet.writeData(outputFile)
            .schema(schema)
            .createWriterFunc(InternalWriter::createWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build()) {
      for (Record record : records) {
        writer.write(record);
      }
    }

    return outputFile.toByteArray();
  }

  /** An in-memory file that supports range reads and counts how its streams are read. */
  private static class CountingInputFile implements InputFile {
    private final byte[] bytes;
    private final List<Integer> tailReads = Lists.newArrayList();
    private int positionalReads = 0;
    private int sequentialReads = 0;
    private boolean closed = false;

    private CountingInputFile(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public long getLength() {
      return bytes.length;
    }

    @Override
    public SeekableInputStream newStream() {
      return new CountingStream();
    }

    @Override
    public String location() {
      return "memory:/counting";
    }

    @Override
    public boolean exists() {
      return true;
    }

    private class CountingStream extends SeekableInputStream implements RangeReadable {
      private long pos = 0;

      @Override
      public long getPos() {
        return pos;
      }

      @Override
      public void seek(long newPos) {
        this.pos = newPos;
      }

      @Override
      public int read() {
        sequentialReads += 1;
        if (pos >= bytes.length) {
          return -1;
        }

        return bytes[(int) pos++] & 0xFF;
      }

      @Override
      public int read(byte[] buffer, int offset, int length) {
        sequentialReads += 1;
        if (pos >= bytes.length) {
          return -1;
        }

        int toRead = (int) Math.min(length, bytes.length - pos);
        System.arraycopy(bytes, (int) pos, buffer, offset, toRead);
        pos += toRead;
        return toRead;
      }

      @Override
      public void readFully(long position, byte[] buffer, int offset, int length) {
        positionalReads += 1;
        System.arraycopy(bytes, (int) position, buffer, offset, length);
      }

      @Override
      public int readTail(byte[] buffer, int offset, int length) {
        tailReads.add(length);
        int toRead = Math.min(length, bytes.length);
        System.arraycopy(bytes, bytes.length - toRead, buffer, offset, toRead);
        return toRead;
      }

      @Override
      public void close() {
        closed = true;
      }
    }
  }
}
