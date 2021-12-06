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
import java.nio.ByteBuffer;
import java.util.stream.IntStream;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.Before;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class InMemoryFileIOTest {

  private InMemoryFileIO fileIO;
  private InMemoryFileStore store;

  @Before
  public void before() {
    fileIO = new InMemoryFileIO();
    store = fileIO.getStore();
  }

  @Test
  public void testGetStore() {
    assertNotNull(store);
  }

  @Test
  public void testDeleteFile() {
    assertFalse(store.exists("file1"));

    // Create the file
    store.put("file1", ByteBuffer.wrap(new byte[0]));
    assertTrue(store.exists("file1"));

    // Delete the file
    fileIO.deleteFile("file1");
    // Verify that the file has been deleted
    assertFalse(store.exists("file1"));
  }

  @Test
  public void testNewInputFile() throws IOException {
    String fileName = "file1";

    store.put(fileName, ByteBuffer.wrap("data1".getBytes(UTF_8)));

    InputFile inputFile = fileIO.newInputFile(fileName);
    assertNotNull(inputFile);

    assertTrue(inputFile.exists());
    assertEquals(fileName, inputFile.location());
    assertEquals(5, inputFile.getLength());

    SeekableInputStream inputStream = inputFile.newStream();
    assertNotNull(inputStream);

    // Test read()
    assertEquals(0, inputStream.getPos());
    assertEquals('d', inputStream.read());

    // Test read(byte[], index, len)
    byte[] dataRead = new byte[3];
    assertEquals(2, inputStream.read(dataRead, 0, 2));
    assertEquals("at", new String(dataRead, 0, 2, UTF_8));

    inputStream.close();
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
    assertNotNull(inputFile);

    // Check file size
    assertTrue(inputFile.exists());
    assertEquals(fileName, inputFile.location());
    assertEquals(expectedFileSize, inputFile.getLength());

    SeekableInputStream inputStream = inputFile.newStream();
    assertNotNull(inputStream);

    // Seek to footer length
    inputStream.seek(expectedFileSize - 8);

    // Read and check the footer size
    byte[] footerSizeBytes = new byte[4];
    assertEquals(footerSizeBytes.length, inputStream.read(footerSizeBytes));
    ByteBuffer footerSizeBuffer = ByteBuffer.wrap(footerSizeBytes);
    int footerSize = footerSizeBuffer.getInt();
    assertEquals(8, footerSize);

    // Read and check the footer
    inputStream.seek(expectedFileSize - 8 - footerSize);
    byte[] footerBytes = new byte[footerSize];
    assertEquals(footerBytes.length, inputStream.read(footerBytes));
    ByteBuffer footerBuffer = ByteBuffer.wrap(footerBytes);
    int chunk1Offset = footerBuffer.getInt();
    int chunk2Offset = footerBuffer.getInt();
    assertEquals(4, chunk1Offset);
    assertEquals(4 + numRowsInChunk * 4, chunk2Offset);

    // Read and check the chunk2
    inputStream.seek(chunk2Offset);
    byte[] chunk2Bytes = new byte[8 * numRowsInChunk];
    assertEquals(chunk2Bytes.length, inputStream.read(chunk2Bytes));
    ByteBuffer chunk2Buffer = ByteBuffer.wrap(chunk2Bytes);
    IntStream.range(0, numRowsInChunk).forEach(i -> assertEquals(i, chunk2Buffer.getDouble(), 1e-15));

    // Read and check the chunk1
    inputStream.seek(chunk1Offset);
    byte[] chunk1Bytes = new byte[4 * numRowsInChunk];
    assertEquals(chunk1Bytes.length, inputStream.read(chunk1Bytes));
    ByteBuffer chunk1Buffer = ByteBuffer.wrap(chunk1Bytes);
    IntStream.range(0, numRowsInChunk).forEach(i -> assertEquals(i, chunk1Buffer.getInt()));

    inputStream.close();
  }

  @Test
  public void testNewOutputFile() throws IOException {
    String fileName = "file1";

    // Create output file
    OutputFile outputFile = fileIO.newOutputFile(fileName);
    assertNotNull(outputFile);
    assertEquals(fileName, outputFile.location());

    // Create output stream
    PositionOutputStream outputStream = outputFile.create();
    assertNotNull(outputStream);

    // Write data to the output stream
    outputStream.write("data1".getBytes(UTF_8));
    outputStream.close();

    // Read the data back to check whether it was written.
    byte[] dataRead = new byte[5];
    assertEquals(5, outputFile.toInputFile().newStream().read(dataRead));
    assertEquals("data1", new String(dataRead, UTF_8));
  }
}
