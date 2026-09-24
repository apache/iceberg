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

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.data.parquet.InternalReader;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
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

  @Test
  public void testWithMaxAllocationInBytesBoundsReadBufferSize() throws IOException {
    List<Record> expected = highEntropyRecords(4000, 1024);
    InputFile file = writeFile(expected);

    int maxAllocationSizeInBytes = 4096;
    List<Integer> requestedLengths = Lists.newArrayList();
    InputFile spy = spyOnReadLengths(file, requestedLengths);

    try (CloseableIterable<Record> reader =
        Parquet.read(spy)
            .project(SCHEMA)
            .withMaxAllocationInBytes(maxAllocationSizeInBytes)
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
   * Callers that only hold the generic {@link org.apache.iceberg.formats.ReadBuilder} (e.g. via
   * {@code FormatModelRegistry.readBuilder(...)}, as engines like Beam do) can't call {@link
   * ReadBuilder#withMaxAllocationInBytes(int)} directly: {@code ParquetFormatModel}'s wrapper only
   * re-exposes the generic interface, which forwards {@code set(key, value)} verbatim into this
   * builder. This test exercises that exact path instead of the typed method above.
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

  @Test
  public void testDefaultAllocationSizeIsNotBoundedByASmallCap() throws IOException {
    List<Record> expected = highEntropyRecords(4000, 1024);
    InputFile file = writeFile(expected);

    int smallAllocationSizeInBytes = 4096;
    List<Integer> requestedLengths = Lists.newArrayList();
    InputFile spy = spyOnReadLengths(file, requestedLengths);

    // no withMaxAllocationInBytes(...) call: exercises Parquet's default (8 MB) allocation size,
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
