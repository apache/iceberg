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
package org.apache.iceberg.io;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIOUtil {
  @Test
  public void testReadFully() throws Exception {
    byte[] buffer = new byte[5];

    MockInputStream stream = new MockInputStream();
    IOUtil.readFully(stream, buffer, 0, buffer.length);

    Assertions.assertThat(buffer)
        .as("Byte array contents should match")
        .isEqualTo(Arrays.copyOfRange(MockInputStream.TEST_ARRAY, 0, 5));

    Assertions.assertThat(stream.getPos())
        .as("Stream position should reflect bytes read")
        .isEqualTo(5);
  }

  @Test
  public void testReadFullySmallReads() throws Exception {
    byte[] buffer = new byte[5];

    MockInputStream stream = new MockInputStream(2, 3, 3);
    IOUtil.readFully(stream, buffer, 0, buffer.length);

    Assertions.assertThat(buffer)
        .as("Byte array contents should match")
        .containsExactly(Arrays.copyOfRange(MockInputStream.TEST_ARRAY, 0, 5));

    Assertions.assertThat(stream.getPos())
        .as("Stream position should reflect bytes read")
        .isEqualTo(5);
  }

  @Test
  public void testReadFullyJustRight() throws Exception {
    final byte[] buffer = new byte[10];

    final MockInputStream stream = new MockInputStream(2, 3, 3);
    IOUtil.readFully(stream, buffer, 0, buffer.length);

    Assertions.assertThat(buffer)
        .as("Byte array contents should match")
        .isEqualTo(MockInputStream.TEST_ARRAY);

    Assertions.assertThat(stream.getPos())
        .as("Stream position should reflect bytes read")
        .isEqualTo(10);

    Assertions.assertThatThrownBy(() -> IOUtil.readFully(stream, buffer, 0, 1))
        .isInstanceOf(EOFException.class)
        .hasMessage("Reached the end of stream with 1 bytes left to read");
  }

  @Test
  public void testReadFullyUnderflow() {
    final byte[] buffer = new byte[11];

    final MockInputStream stream = new MockInputStream(2, 3, 3);

    Assertions.assertThatThrownBy(() -> IOUtil.readFully(stream, buffer, 0, buffer.length))
        .isInstanceOf(EOFException.class)
        .hasMessage("Reached the end of stream with 1 bytes left to read");

    Assertions.assertThat(Arrays.copyOfRange(buffer, 0, 10))
        .as("Should have consumed bytes")
        .isEqualTo(MockInputStream.TEST_ARRAY);

    Assertions.assertThat(stream.getPos())
        .as("Stream position should reflect bytes read")
        .isEqualTo(10);
  }

  @Test
  public void testReadFullyStartAndLength() throws IOException {
    byte[] buffer = new byte[10];

    MockInputStream stream = new MockInputStream();
    IOUtil.readFully(stream, buffer, 2, 5);

    Assertions.assertThat(Arrays.copyOfRange(buffer, 2, 7))
        .as("Byte array contents should match")
        .isEqualTo(Arrays.copyOfRange(MockInputStream.TEST_ARRAY, 0, 5));

    Assertions.assertThat(stream.getPos())
        .as("Stream position should reflect bytes read")
        .isEqualTo(5);
  }

  @Test
  public void testReadFullyZeroByteRead() throws IOException {
    byte[] buffer = new byte[0];

    MockInputStream stream = new MockInputStream();
    IOUtil.readFully(stream, buffer, 0, buffer.length);

    Assertions.assertThat(stream.getPos())
        .as("Stream position should reflect bytes read")
        .isEqualTo(0);
  }

  @Test
  public void testReadFullySmallReadsWithStartAndLength() throws IOException {
    byte[] buffer = new byte[10];

    MockInputStream stream = new MockInputStream(2, 2, 3);
    IOUtil.readFully(stream, buffer, 2, 5);

    Assertions.assertThat(Arrays.copyOfRange(buffer, 2, 7))
        .as("Byte array contents should match")
        .isEqualTo(Arrays.copyOfRange(MockInputStream.TEST_ARRAY, 0, 5));

    Assertions.assertThat(stream.getPos())
        .as("Stream position should reflect bytes read")
        .isEqualTo(5);
  }

  @Test
  public void testWriteFully() throws Exception {
    byte[] input = Strings.repeat("Welcome to Warsaw!\n", 12345).getBytes(StandardCharsets.UTF_8);
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    try (PositionOutputStream outputStream = outputFile.create()) {
      IOUtil.writeFully(outputStream, ByteBuffer.wrap(input.clone()));
    }
    Assertions.assertThat(outputFile.toByteArray()).isEqualTo(input);
  }
}
