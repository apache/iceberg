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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestFailingParquetWriter {

  private static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get()), required(4, "data", Types.StringType.get()));
  private static final String EXCEPTION_MSG = "Surprise! Your network is down!";

  private OutputFile output;
  private PositionOutputStream outputStream;

  @BeforeEach
  void before() throws IOException {
    output = spy(new InMemoryOutputFile());
    outputStream = mock(PositionOutputStream.class);

    when(output.create()).thenReturn(outputStream);
    when(output.createOrOverwrite()).thenReturn(outputStream);
    // First, the output fails, then it works again:
    doThrow(new IOException(EXCEPTION_MSG)).doCallRealMethod().when(outputStream).close();
  }

  @Test
  void testFailingWriter() throws IOException {
    FileAppender<Record> appender =
        Parquet.write(output).schema(SCHEMA).createWriterFunc(GenericParquetWriter::create).build();
    appender.addAll(RandomGenericData.generate(SCHEMA, 5, 12345L));

    // This call fails, as the IO throws an exception:
    assertThatThrownBy(appender::close)
        .isInstanceOf(IOException.class)
        .hasMessage("java.io.IOException: " + EXCEPTION_MSG);

    // Network is up again, now it should work.
    // As long as https://github.com/apache/parquet-java/issues/3254 is not fixed, this
    // unfortunately still throws an exception:
    assertThatThrownBy(appender::close)
        .isInstanceOf(UncheckedIOException.class)
        .hasMessage("Failed to flush row group");
  }
}
