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
package org.apache.iceberg.huaweicloud.obs;

import static org.apache.iceberg.AssertHelpers.assertThrows;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.junit.Test;

public class TestOBSInputStream extends HuaweicloudOBSTestBase {
  private final Random random = ThreadLocalRandom.current();

  @Test
  public void testRead() throws Exception {
    OBSURI uri = new OBSURI(location("read.dat"));
    int dataSize = 1024 * 1024 * 10;
    byte[] data = randomData(dataSize);

    writeOBSData(uri, data);

    try (SeekableInputStream in = new OBSInputStream(obsClient().get(), uri)) {
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
    assertEquals("Should have the correct position", rangeStart, in.getPos());

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

    assertEquals("Should have the correct position", rangeEnd, in.getPos());

    assertArrayEquals(
        "Should have expected range data",
        Arrays.copyOfRange(original, (int) rangeStart, (int) rangeEnd),
        actual);
  }

  @Test
  public void testClose() throws Exception {
    OBSURI uri = new OBSURI(location("closed.dat"));
    SeekableInputStream closed = new OBSInputStream(obsClient().get(), uri);
    closed.close();
    assertThrows(
        "Cannot seek the input stream after closed.",
        IllegalStateException.class,
        "Cannot seek: already closed",
        () -> {
          closed.seek(0);
          return null;
        });
  }

  @Test
  public void testSeek() throws Exception {
    OBSURI uri = new OBSURI(location("seek.dat"));
    byte[] expected = randomData(1024 * 1024);

    writeOBSData(uri, expected);

    try (SeekableInputStream in = new OBSInputStream(obsClient().get(), uri)) {
      in.seek(expected.length / 2);
      byte[] actual = new byte[expected.length / 2];
      ByteStreams.readFully(in, actual);
      assertArrayEquals(
          "Should have expected seeking stream",
          Arrays.copyOfRange(expected, expected.length / 2, expected.length),
          actual);
    }
  }

  private byte[] randomData(int size) {
    byte[] data = new byte[size];
    random.nextBytes(data);
    return data;
  }

  private void writeOBSData(OBSURI uri, byte[] data) {
    obsClient().get().putObject(uri.bucket(), uri.key(), new ByteArrayInputStream(data));
  }
}
