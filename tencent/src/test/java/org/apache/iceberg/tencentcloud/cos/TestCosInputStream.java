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
package org.apache.iceberg.tencentcloud.cos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.qcloud.cos.model.ObjectMetadata;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.junit.jupiter.api.Test;

public class TestCosInputStream {
  private final Random random = ThreadLocalRandom.current();

  @Test
  public void testRead() throws Exception {
    CosURI uri = new CosURI(TestUtility.location("read.dat"));
    int dataSize = 1024 * 1024 * 10;
    byte[] data = randomData(dataSize);

    writeCosData(uri, data);

    try (SeekableInputStream in = new CosInputStream(TestUtility.cos(), uri)) {
      int readSize = 1024;

      readAndCheck(in, in.getPos(), readSize, data, false);
      readAndCheck(in, in.getPos(), readSize, data, true);

      // Seek forward in current stream
      int seekSize = 1024;
      readAndCheck(in, in.getPos() + seekSize, readSize, data, false);
      readAndCheck(in, in.getPos() + seekSize, readSize, data, true);

      // Buffered read
      readAndCheck(in, in.getPos(), readSize, data, true);
      readAndCheck(in, in.getPos(), readSize, data, false);

      // Seek with new stream
      long seekNewStreamPosition = 2 * 1024 * 1024;
      readAndCheck(in, in.getPos() + seekNewStreamPosition, readSize, data, true);
      readAndCheck(in, in.getPos() + seekNewStreamPosition, readSize, data, false);

      // Backseek and read
      readAndCheck(in, 0, readSize, data, true);
      readAndCheck(in, 0, readSize, data, false);
    }
  }

  private void readAndCheck(
      SeekableInputStream in, long rangeStart, int size, byte[] original, boolean buffered)
      throws IOException {
    in.seek(rangeStart);
    assertThat(rangeStart).as("Should have the correct position").isEqualTo(in.getPos());

    long rangeEnd = rangeStart + size;
    byte[] actual = new byte[size];

    if (buffered) {
      ByteStreams.readFully(in, actual);
    } else {
      int read = 0;
      while (read < size) {
        actual[read++] = (byte) in.read();
      }
    }

    assertThat(rangeEnd).as("Should have the correct position").isEqualTo(in.getPos());

    assertThat(Arrays.copyOfRange(original, (int) rangeStart, (int) rangeEnd))
        .as("Should have expected range data")
        .containsExactly(actual);
  }

  @Test
  public void testClose() throws Exception {
    CosURI uri = new CosURI(TestUtility.location("closed.dat"));
    SeekableInputStream closed = new CosInputStream(TestUtility.cos(), uri);
    closed.close();
    assertThatThrownBy(() -> closed.seek(0))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot seek: already closed");
  }

  @Test
  public void testSeek() throws Exception {
    CosURI uri = new CosURI(TestUtility.location("seek.dat"));
    byte[] expected = randomData(1024 * 1024);

    writeCosData(uri, expected);

    try (SeekableInputStream in = new CosInputStream(TestUtility.cos(), uri)) {
      in.seek(expected.length / 2);
      byte[] actual = new byte[expected.length / 2];
      ByteStreams.readFully(in, actual);
      assertThat(Arrays.copyOfRange(expected, expected.length / 2, expected.length))
          .as("Should have expected seeking stream")
          .containsExactly(actual);
    }
  }

  private byte[] randomData(int size) {
    byte[] data = new byte[size];
    random.nextBytes(data);
    return data;
  }

  private void writeCosData(CosURI uri, byte[] data) {
    TestUtility.cos()
        .putObject(uri.bucket(), uri.key(), new ByteArrayInputStream(data), new ObjectMetadata());
  }
}
