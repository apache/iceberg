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

package org.apache.iceberg.io.inmemory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;

public class InMemoryFileIOTest {

  private InMemoryFileIO fileIO;
  private InMemoryFileStore store;

  @BeforeEach
  public void before() {
    fileIO = new InMemoryFileIO();
    store = fileIO.getStore();
  }

  @Test
  public void testGetStore() {
    Assertions.assertThat(store).isNotNull();
  }

  @Test
  public void testDeleteFile() {
    Assertions.assertThat(store.exists("file1")).isFalse();

    // Create the file
    store.put("file1", ByteBuffer.wrap(new byte[0]));
    Assertions.assertThat(store.exists("file1")).isTrue();

    // Delete the file
    fileIO.deleteFile("file1");
    // Verify that the file has been deleted
    Assertions.assertThat(store.exists("file1")).isFalse();

    Assertions.assertThatThrownBy(() -> fileIO.deleteFile("nonExisting"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("File at location 'nonExisting' does not exist");
  }

  @Test
  public void testNewInputFile() throws IOException {
    String fileName = "file1";

    store.put(fileName, ByteBuffer.wrap("data1".getBytes(UTF_8)));

    InputFile inputFile = fileIO.newInputFile(fileName);
    Assertions.assertThat(inputFile).isNotNull();

    Assertions.assertThat(inputFile.exists()).isTrue();
    Assertions.assertThat(inputFile.location()).isEqualTo(fileName);
    Assertions.assertThat(inputFile.getLength()).isEqualTo(5);

    try (SeekableInputStream inputStream = inputFile.newStream()) {
      Assertions.assertThat(inputStream).isNotNull();

      // Test read()
      Assertions.assertThat(inputStream.getPos()).isEqualTo(0);
      Assertions.assertThat(inputStream.read()).isEqualTo('d');

      // Test read(byte[], index, len)
      byte[] dataRead = new byte[3];
      Assertions.assertThat(inputStream.read(dataRead, 0, 2)).isEqualTo(2);
      Assertions.assertThat(new String(dataRead, 0, 2, UTF_8)).isEqualTo("at");
    }
  }

  @Test
  public void testSeek() throws IOException {
    String fileName = "file1";

    // Number of rows
    int numRowsInChunk = 10;

    // Emulate parquet file structure
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    // Add magic number
    buffer.put("par1".getBytes(UTF_8));
    // Add column chunk 1 for int32
    IntStream.range(0, numRowsInChunk).forEach(buffer::putInt);
    // Add column chunk 2 for float64
    IntStream.range(0, numRowsInChunk).forEach(buffer::putDouble);
    // Add metadata footer (index to the start of chunks)
    buffer.putInt(4);
    buffer.putInt(4 + numRowsInChunk * 4);
    // Add footer size
    buffer.putInt(8);
    // Add magic number
    buffer.put("par1".getBytes(UTF_8));

    // Put the data in the store
    buffer.flip();
    store.put(fileName, buffer);

    // magic number, chunk1, chunk2, footer, footer size, magic number
    int expectedFileSize = 4 + numRowsInChunk * 4 + numRowsInChunk * 8 + 8 + 4 + 4;

    InputFile inputFile = fileIO.newInputFile(fileName);
    Assertions.assertThat(inputFile).isNotNull();

    // Check file size
    Assertions.assertThat(inputFile.exists()).isTrue();
    Assertions.assertThat(inputFile.location()).isEqualTo(fileName);
    Assertions.assertThat(inputFile.getLength()).isEqualTo(expectedFileSize);

    try (SeekableInputStream inputStream = inputFile.newStream()) {
      Assertions.assertThat(inputStream).isNotNull();

      // Seek to footer length
      inputStream.seek(expectedFileSize - 8);

      // Read and check the footer size
      byte[] footerSizeBytes = new byte[4];
      Assertions.assertThat(inputStream.read(footerSizeBytes)).isEqualTo(footerSizeBytes.length);
      ByteBuffer footerSizeBuffer = ByteBuffer.wrap(footerSizeBytes);
      int footerSize = footerSizeBuffer.getInt();
      Assertions.assertThat(footerSize).isEqualTo(8);

      // Read and check the footer
      inputStream.seek(expectedFileSize - 8 - footerSize);
      byte[] footerBytes = new byte[footerSize];
      Assertions.assertThat(inputStream.read(footerBytes)).isEqualTo(footerBytes.length);
      ByteBuffer footerBuffer = ByteBuffer.wrap(footerBytes);
      int chunk1Offset = footerBuffer.getInt();
      int chunk2Offset = footerBuffer.getInt();
      Assertions.assertThat(chunk1Offset).isEqualTo(4);
      Assertions.assertThat(chunk2Offset).isEqualTo(4 + numRowsInChunk * 4);

      // Read and check the chunk2
      inputStream.seek(chunk2Offset);
      byte[] chunk2Bytes = new byte[8 * numRowsInChunk];
      Assertions.assertThat(inputStream.read(chunk2Bytes)).isEqualTo(chunk2Bytes.length);
      ByteBuffer chunk2Buffer = ByteBuffer.wrap(chunk2Bytes);
      IntStream.range(0, numRowsInChunk)
          .forEach(i -> Assertions.assertThat(chunk2Buffer.getDouble()).isCloseTo(i, Offset.offset(1e-15)));

      // Read and check the chunk1
      inputStream.seek(chunk1Offset);
      byte[] chunk1Bytes = new byte[4 * numRowsInChunk];
      Assertions.assertThat(inputStream.read(chunk1Bytes)).isEqualTo(chunk1Bytes.length);
      ByteBuffer chunk1Buffer = ByteBuffer.wrap(chunk1Bytes);
      IntStream.range(0, numRowsInChunk)
          .forEach(i -> Assertions.assertThat(chunk1Buffer.getInt()).isEqualTo(i));
    }
  }

  @Test
  public void testSeekEOF() throws IOException {
    String fileName = "someFile";
    store.put(fileName, ByteBuffer.wrap("data1".getBytes(UTF_8)));
    InputFile inputFile = fileIO.newInputFile(fileName);
    Assertions.assertThat(inputFile).isNotNull();

    Assertions.assertThat(inputFile.exists()).isTrue();
    Assertions.assertThat(inputFile.location()).isEqualTo(fileName);
    Assertions.assertThat(inputFile.getLength()).isEqualTo(5);

    try (SeekableInputStream inputStream = inputFile.newStream()) {
      Assertions.assertThatThrownBy(() -> inputStream.seek(-1))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Invalid position -1 within stream of length 5");

      Assertions.assertThatThrownBy(() -> inputStream.seek(6))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Invalid position 6 within stream of length 5");
    }
  }

  @Test
  public void testNewOutputFile() throws IOException {
    String fileName = "file1";

    // Create output file
    OutputFile outputFile = fileIO.newOutputFile(fileName);
    Assertions.assertThat(outputFile).isNotNull();
    Assertions.assertThat(outputFile.location()).isEqualTo(fileName);

    // Create output stream
    try (PositionOutputStream outputStream = outputFile.create()) {
      Assertions.assertThat(outputStream).isNotNull();

      // Write data to the output stream
      outputStream.write("data1".getBytes(UTF_8));
    }

    // Read the data back to check whether it was written.
    byte[] dataRead = new byte[5];
    try (SeekableInputStream seekableInputStream = outputFile.toInputFile().newStream()) {
      Assertions.assertThat(seekableInputStream.read(dataRead)).isEqualTo(5);
      Assertions.assertThat(new String(dataRead, UTF_8)).isEqualTo("data1");
    }
  }

  @Test
  public void testReadAfterClose() throws IOException {
    String fileName = "readAfterClose";
    store.put(fileName, ByteBuffer.wrap("abc".getBytes(StandardCharsets.ISO_8859_1)));
    InputFile inputFile = fileIO.newInputFile(fileName);
    InputStream inputStream = inputFile.newStream();
    byte[] dataRead = new byte[1];
    inputStream.read(dataRead);
    Assertions.assertThat(new String(dataRead, StandardCharsets.ISO_8859_1)).isEqualTo("a");
    inputStream.close();
    Assertions.assertThatThrownBy(inputStream::read)
        .hasMessage("Stream is closed");
  }

  @Test
  public void testWriteAfterClose() throws IOException {
    OutputFile outputFile = fileIO.newOutputFile("writeAfterClose");
    OutputStream outputStream = outputFile.create();
    outputStream.write('a');
    outputStream.write('b');
    outputStream.close();
    Assertions.assertThatThrownBy(() -> outputStream.write('c'))
        .hasMessage("Stream is closed");

    byte[] dataRead = new byte[2];
    try (SeekableInputStream stream = outputFile.toInputFile().newStream()) {
      Assertions.assertThat(stream.read(dataRead)).isEqualTo(dataRead.length);
      Assertions.assertThat(new String(dataRead, StandardCharsets.ISO_8859_1)).isEqualTo("ab");
    }
  }
}
