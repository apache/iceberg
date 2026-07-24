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
package org.apache.iceberg.avro;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class TestAvroIterable {
  @Test
  void streamClosedOnIOException() throws Exception {
    InputFile inputFile = mock(InputFile.class);
    SeekableInputStream seekableInputStream = mock(SeekableInputStream.class);
    SeekableInput seekableInput = mock(SeekableInput.class);
    DatumReader<?> datumReader = mock(DatumReader.class);

    when(inputFile.newStream()).thenReturn(seekableInputStream);
    when(inputFile.getLength()).thenReturn(100L);

    try (MockedStatic<AvroIO> avroIo = mockStatic(AvroIO.class);
        MockedStatic<DataFileReader> dataFileReaderMock = mockStatic(DataFileReader.class)) {
      avroIo.when(() -> AvroIO.stream(seekableInputStream, 100L)).thenReturn(seekableInput);
      dataFileReaderMock
          .when(() -> DataFileReader.openReader(seekableInput, datumReader))
          .thenThrow(new IOException());

      AvroIterable<?> iterable = new AvroIterable<>(inputFile, datumReader, null, null, false);

      Method method = iterable.getClass().getDeclaredMethod("newFileReader");
      method.setAccessible(true);
      assertThatThrownBy(() -> method.invoke(iterable))
          .hasCauseInstanceOf(RuntimeIOException.class);

      verify(seekableInput, atLeastOnce()).close();
    }
  }

  @Test
  void readerClosedWhenRangeSyncFails() throws Exception {
    InputFile inputFile = mock(InputFile.class);
    SeekableInputStream seekableInputStream = mock(SeekableInputStream.class);
    SeekableInput seekableInput = mock(SeekableInput.class);
    DatumReader<?> datumReader = mock(DatumReader.class);
    DataFileReader<?> fileReader = mock(DataFileReader.class);

    when(inputFile.newStream()).thenReturn(seekableInputStream);
    when(inputFile.getLength()).thenReturn(100L);
    when(fileReader.getMetaKeys()).thenReturn(Collections.emptyList());
    doThrow(new IOException()).when(fileReader).sync(5L);
    IOException closeException = new IOException("close failed");
    doThrow(closeException).when(fileReader).close();

    try (MockedStatic<AvroIO> avroIo = mockStatic(AvroIO.class);
        MockedStatic<DataFileReader> dataFileReaderMock = mockStatic(DataFileReader.class)) {
      avroIo.when(() -> AvroIO.stream(seekableInputStream, 100L)).thenReturn(seekableInput);
      dataFileReaderMock
          .when(() -> DataFileReader.openReader(seekableInput, datumReader))
          .thenReturn(fileReader);

      AvroIterable<?> iterable = new AvroIterable<>(inputFile, datumReader, 5L, 10L, false);

      assertThatThrownBy(iterable::iterator)
          .isInstanceOf(RuntimeIOException.class)
          .hasMessage("Failed to find sync past position 5")
          .extracting(e -> java.util.Arrays.asList(e.getSuppressed()))
          .asInstanceOf(LIST)
          .containsExactly(closeException);

      verify(fileReader, atLeastOnce()).close();
    }
  }
}
