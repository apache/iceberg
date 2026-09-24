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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.Seekable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.data.parquet.InternalReader;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

public class TestParquetReadAllocationSize {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));

  // Iceberg eagerly reads whole files at or below its EAGER_FETCH_THRESHOLD_BYTES (1 MB) in a
  // single read, bypassing the allocation-size-bounded chunking path entirely. The written file
  // must clear that threshold for these tests to exercise the code this test actually targets.
  private static final int MIN_TOTAL_BYTES = 2 * 1024 * 1024;

  // high-entropy, unique-per-row content so it can't collapse via dictionary encoding or
  // compression, guaranteeing the written column chunk is meaningfully larger than the small
  // allocation caps used below.
  private static List<Record> highEntropyRecords(int numRecords, int stringLength) {
    Random random = new Random(41103L);
    List<Record> records = Lists.newArrayListWithCapacity(numRecords);
    for (int i = 0; i < numRecords; i += 1) {
      byte[] bytes = new byte[stringLength];
      random.nextBytes(bytes);
      Record record = GenericRecord.create(SCHEMA);
      record.setField("id", (long) i);
      record.setField("data", Base64.getEncoder().encodeToString(bytes));
      records.add(record);
    }
    return records;
  }

  private static InputFile writeFile(List<Record> records) throws IOException {
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    try (DataWriter<Record> writer =
        Parquet.writeData(outputFile)
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build()) {
      for (Record record : records) {
        writer.write(record);
      }
    }

    InputFile inputFile = outputFile.toInputFile();
    assertThat(inputFile.getLength())
        .as("test file must clear Iceberg's eager-fetch threshold to exercise chunked reads")
        .isGreaterThan(MIN_TOTAL_BYTES);
    return inputFile;
  }

  /**
   * {@code Parquet.ReadBuilder} hands {@link HadoopInputFile}-backed reads off to a native
   * parquet-hadoop file/stream built directly from the Hadoop {@link Configuration} (see {@code
   * ParquetIO.file(InputFile)}), bypassing Iceberg's own {@code InputFile.newStream()} entirely.
   * Spying on the Iceberg {@link InputFile} therefore can't observe these reads; this records every
   * {@code read(byte[], int, int)} length at the Hadoop {@link FileSystem} level instead, which
   * both the write path and parquet-hadoop's native read path actually go through.
   */
  public static class RecordingLocalFileSystem extends RawLocalFileSystem {
    private static final ThreadLocal<List<Integer>> RECORDED_READ_LENGTHS = new ThreadLocal<>();

    static void record(List<Integer> requestedLengths) {
      RECORDED_READ_LENGTHS.set(requestedLengths);
    }

    static void stopRecording() {
      RECORDED_READ_LENGTHS.remove();
    }

    @Override
    public FSDataInputStream open(org.apache.hadoop.fs.Path f, int bufferSize) throws IOException {
      FSDataInputStream delegate = super.open(f, bufferSize);
      List<Integer> requestedLengths = RECORDED_READ_LENGTHS.get();
      return requestedLengths == null
          ? delegate
          : new FSDataInputStream(new RecordingStream(delegate, requestedLengths));
    }

    private static class RecordingStream extends InputStream
        implements Seekable, PositionedReadable {
      private final FSDataInputStream delegate;
      private final List<Integer> requestedLengths;

      RecordingStream(FSDataInputStream delegate, List<Integer> requestedLengths) {
        this.delegate = delegate;
        this.requestedLengths = requestedLengths;
      }

      @Override
      public int read() throws IOException {
        return delegate.read();
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        requestedLengths.add(len);
        return delegate.read(b, off, len);
      }

      @Override
      public void seek(long pos) throws IOException {
        delegate.seek(pos);
      }

      @Override
      public long getPos() throws IOException {
        return delegate.getPos();
      }

      @Override
      public boolean seekToNewSource(long targetPos) throws IOException {
        return delegate.seekToNewSource(targetPos);
      }

      @Override
      public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        return delegate.read(position, buffer, offset, length);
      }

      @Override
      public void readFully(long position, byte[] buffer, int offset, int length)
          throws IOException {
        delegate.readFully(position, buffer, offset, length);
      }

      @Override
      public void readFully(long position, byte[] buffer) throws IOException {
        delegate.readFully(position, buffer);
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }
    }
  }

  private static Configuration newRecordingConfiguration() {
    Configuration conf = new Configuration();
    conf.setClass("fs.file.impl", RecordingLocalFileSystem.class, FileSystem.class);
    conf.setBoolean("fs.file.impl.disable.cache", true);
    return conf;
  }

  private static HadoopInputFile writeHadoopFile(
      List<Record> records, Path tempDir, Configuration conf) throws IOException {
    File file = new File(tempDir.toFile(), "ambient-config-test.parquet");
    org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.getAbsolutePath());
    OutputFile outputFile = HadoopOutputFile.fromPath(hadoopPath, conf);
    try (DataWriter<Record> writer =
        Parquet.writeData(outputFile)
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .build()) {
      for (Record record : records) {
        writer.write(record);
      }
    }

    HadoopInputFile inputFile = HadoopInputFile.fromPath(hadoopPath, conf);
    assertThat(inputFile.getLength())
        .as("test file must clear Iceberg's eager-fetch threshold to exercise chunked reads")
        .isGreaterThan(MIN_TOTAL_BYTES);
    return inputFile;
  }

  /**
   * Spies on the delegate's stream reads and records the length requested by every {@code
   * read(byte[], int, int)} call, so tests can assert on the actual buffer sizes Parquet reads
   * with, not just that the read completed successfully.
   */
  private static InputFile spyOnReadLengths(InputFile delegate, List<Integer> requestedLengths) {
    InputFile spy = Mockito.spy(delegate);
    Mockito.doAnswer(
            invocation -> {
              SeekableInputStream streamSpy =
                  Mockito.spy((SeekableInputStream) invocation.callRealMethod());
              Mockito.doAnswer(
                      (InvocationOnMock readInvocation) -> {
                        requestedLengths.add(readInvocation.getArgument(2));
                        return readInvocation.callRealMethod();
                      })
                  .when(streamSpy)
                  .read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt());
              return streamSpy;
            })
        .when(spy)
        .newStream();
    return spy;
  }

  /**
   * {@code Parquet.ReadBuilder} has no typed setter for this - callers that only hold the generic
   * {@link org.apache.iceberg.formats.ReadBuilder} (e.g. via {@code
   * FormatModelRegistry.readBuilder(...)}, as engines like Beam do) only ever have {@code set(key,
   * value)} available, and {@code ParquetFormatModel}'s wrapper forwards it verbatim into this
   * builder.
   */
  @Test
  public void testAllocationSizePropertyRoutesThroughGenericSet() throws IOException {
    List<Record> expected = highEntropyRecords(4000, 1024);
    InputFile file = writeFile(expected);

    int maxAllocationSizeInBytes = 4096;
    List<Integer> requestedLengths = Lists.newArrayList();
    InputFile spy = spyOnReadLengths(file, requestedLengths);

    try (CloseableIterable<Record> reader =
        Parquet.read(spy)
            .project(SCHEMA)
            .set("parquet.read.allocation.size", String.valueOf(maxAllocationSizeInBytes))
            .createReaderFunc(fileSchema -> InternalReader.create(SCHEMA, fileSchema))
            .build()) {
      assertThat(reader).as("all records should be read back").hasSameSizeAs(expected);
    }

    assertThat(requestedLengths).as("test should exercise at least one buffered read").isNotEmpty();
    assertThat(requestedLengths)
        .as("no single read should request more than the configured allocation size")
        .allSatisfy(len -> assertThat(len).isLessThanOrEqualTo(maxAllocationSizeInBytes));
  }

  /**
   * All tests above use a plain {@link InMemoryOutputFile}, which is not {@code HadoopConfigurable}
   * and so only exercises the non-Hadoop branch of {@code Parquet.ReadBuilder#build()}. This test
   * exercises the other branch (real {@link HadoopInputFile}), and does so purely through the
   * file's own ambient {@link Configuration} - simulating a cluster/session-level Hadoop setting
   * (e.g. Spark's {@code spark.hadoop.parquet.read.allocation.size}) set before the file ever
   * reaches {@code Parquet.read(...)}, with no {@code .set(...)} call on the builder itself. Reads
   * are observed at the Hadoop {@link FileSystem} level via {@link RecordingLocalFileSystem}, not
   * by spying on the Iceberg {@code InputFile}, since {@code ParquetIO.file(InputFile)} bypasses
   * the latter entirely for {@link HadoopInputFile}.
   */
  @Test
  public void testAmbientHadoopConfigurationAllocationSizeIsRespected(@TempDir Path tempDir)
      throws IOException {
    List<Record> expected = highEntropyRecords(4000, 1024);

    int maxAllocationSizeInBytes = 4096;
    Configuration conf = newRecordingConfiguration();
    conf.setInt("parquet.read.allocation.size", maxAllocationSizeInBytes);

    HadoopInputFile file = writeHadoopFile(expected, tempDir, conf);

    List<Integer> requestedLengths = Lists.newArrayList();
    RecordingLocalFileSystem.record(requestedLengths);
    try {
      try (CloseableIterable<Record> reader =
          Parquet.read(file)
              .project(SCHEMA)
              .createReaderFunc(fileSchema -> InternalReader.create(SCHEMA, fileSchema))
              .build()) {
        assertThat(reader).as("all records should be read back").hasSameSizeAs(expected);
      }
    } finally {
      RecordingLocalFileSystem.stopRecording();
    }

    assertThat(requestedLengths).as("test should exercise at least one buffered read").isNotEmpty();
    assertThat(requestedLengths)
        .as("no single read should request more than the ambient configuration's allocation size")
        .allSatisfy(len -> assertThat(len).isLessThanOrEqualTo(maxAllocationSizeInBytes));
  }

  @Test
  public void testDefaultAllocationSizeIsNotBoundedByASmallCap() throws IOException {
    List<Record> expected = highEntropyRecords(4000, 1024);
    InputFile file = writeFile(expected);

    int smallAllocationSizeInBytes = 4096;
    List<Integer> requestedLengths = Lists.newArrayList();
    InputFile spy = spyOnReadLengths(file, requestedLengths);

    // no set(...) call: exercises Parquet's default (8 MB) allocation size,
    // proving the small cap above is not met unless explicitly configured.
    try (CloseableIterable<Record> reader =
        Parquet.read(spy)
            .project(SCHEMA)
            .createReaderFunc(fileSchema -> InternalReader.create(SCHEMA, fileSchema))
            .build()) {
      assertThat(reader).as("all records should be read back").hasSameSizeAs(expected);
    }

    assertThat(requestedLengths)
        .as("without an explicit cap, at least one read exceeds the small allocation size")
        .anySatisfy(len -> assertThat(len).isGreaterThan(smallAllocationSizeInBytes));
  }
}
