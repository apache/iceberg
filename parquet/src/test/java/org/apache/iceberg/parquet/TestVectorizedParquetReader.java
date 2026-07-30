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

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestVectorizedParquetReader {
  private static final Schema SCHEMA = new Schema(required(1, "id", Types.IntegerType.get()));

  @TempDir private Path temp;

  @Test
  void closeReleasesReaderWhenModelCloseThrows() throws IOException {
    File file = ParquetWritingTestUtils.createTempFile(temp);
    GenericData.Record record =
        new GenericData.Record(AvroSchemaUtil.convert(SCHEMA.asStruct(), "test"));
    record.put("id", 1);
    ParquetWritingTestUtils.write(
        file, SCHEMA, ImmutableMap.of(), ParquetAvroWriter::buildWriter, record);

    // track the number of open input streams so a leaked reader is observable
    AtomicInteger openStreams = new AtomicInteger(0);
    InputFile trackingFile = trackingInputFile(localInput(file), openStreams);

    Function<MessageType, VectorizedReader<?>> readerFunc = type -> new ThrowingCloseReader();

    VectorizedParquetReader<Object> reader =
        new VectorizedParquetReader<>(
            trackingFile,
            SCHEMA,
            ParquetReadOptions.builder().build(),
            readerFunc,
            null,
            Expressions.alwaysTrue(),
            false,
            true,
            1);

    CloseableIterator<Object> iterator = reader.iterator();
    assertThat(openStreams.get()).as("opening the reader should open a stream").isPositive();

    // model.close() throws, but the underlying file reader must still be closed
    assertThatThrownBy(iterator::close).isInstanceOf(RuntimeException.class).hasMessage("boom");
    assertThat(openStreams.get())
        .as("file reader must be closed even when model.close() throws")
        .isZero();
  }

  private static class ThrowingCloseReader implements VectorizedReader<Object> {
    @Override
    public Object read(Object reuse, int numRows) {
      return null;
    }

    @Override
    public void setBatchSize(int batchSize) {}

    @Override
    public void close() {
      throw new RuntimeException("boom");
    }
  }

  private static InputFile trackingInputFile(InputFile delegate, AtomicInteger openStreams) {
    return new InputFile() {
      @Override
      public long getLength() {
        return delegate.getLength();
      }

      @Override
      public SeekableInputStream newStream() {
        openStreams.incrementAndGet();
        SeekableInputStream stream = delegate.newStream();
        return new SeekableInputStream() {
          @Override
          public long getPos() throws IOException {
            return stream.getPos();
          }

          @Override
          public void seek(long newPos) throws IOException {
            stream.seek(newPos);
          }

          @Override
          public int read() throws IOException {
            return stream.read();
          }

          @Override
          public int read(byte[] bytes, int off, int len) throws IOException {
            return stream.read(bytes, off, len);
          }

          @Override
          public void close() throws IOException {
            openStreams.decrementAndGet();
            stream.close();
          }
        };
      }

      @Override
      public String location() {
        return delegate.location();
      }

      @Override
      public boolean exists() {
        return delegate.exists();
      }
    };
  }
}
