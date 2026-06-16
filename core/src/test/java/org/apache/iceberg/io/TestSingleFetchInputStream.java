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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.EOFException;
import org.junit.jupiter.api.Test;

public class TestSingleFetchInputStream {

  private SingleFetchInputStream newStream() {
    byte[] data = new byte[35];
    for (int i = 0; i < data.length; i += 1) {
      data[i] = (byte) i;
    }
    return new SingleFetchInputStream(data);
  }

  @Test
  public void testReadAndSeek() throws Exception {
    try (SingleFetchInputStream stream = newStream()) {
      assertThat(stream.read()).as("Should return first byte").isEqualTo(0);
      assertThat(stream.getPos()).as("Position should advance to 1 after read()").isEqualTo(1L);

      byte[] buf = new byte[10];
      assertThat(stream.read(buf, 0, 10)).as("Should read 10 bytes").isEqualTo(10);
      for (int i = 0; i < 10; i += 1) {
        assertThat(buf[i]).as("Byte i should be i + 1").isEqualTo((byte) (i + 1));
      }
      assertThat(stream.getPos()).as("Position should advance to 11").isEqualTo(11L);

      stream.seek(20);
      assertThat(stream.getPos()).as("Position should be 20 after seek").isEqualTo(20L);

      byte[] tail = new byte[15];
      assertThat(stream.read(tail, 0, 15)).as("Should read 15 bytes").isEqualTo(15);
      for (int i = 0; i < 15; i += 1) {
        assertThat(tail[i]).as("Byte i should be 20 + i").isEqualTo((byte) (20 + i));
      }

      assertThat(stream.read()).as("Should return -1 at end of stream").isEqualTo(-1);
      assertThat(stream.getPos()).as("Position should be at end").isEqualTo(35L);
    }
  }

  @Test
  public void testReadFully() throws Exception {
    try (SingleFetchInputStream stream = newStream()) {
      stream.seek(10);

      byte[] buf = new byte[5];
      stream.readFully(20, buf, 0, 5);
      for (int i = 0; i < 5; i += 1) {
        assertThat(buf[i]).as("Byte i should be 20 + i").isEqualTo((byte) (20 + i));
      }

      assertThat(stream.getPos()).as("readFully should not advance position").isEqualTo(10L);

      byte[] offsetBuf = new byte[10];
      stream.readFully(15, offsetBuf, 3, 4);
      for (int i = 0; i < 4; i += 1) {
        assertThat(offsetBuf[3 + i])
            .as("Byte at offset+i should be 15 + i")
            .isEqualTo((byte) (15 + i));
      }

      stream.readFully(35, new byte[0], 0, 0);

      assertThatThrownBy(() -> stream.readFully(35, new byte[1], 0, 1))
          .isInstanceOf(EOFException.class)
          .hasMessageContaining("exceeds stream length");

      assertThatThrownBy(() -> stream.readFully(-1, new byte[1], 0, 1))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("position is negative");
    }
  }

  @Test
  public void testReadTail() throws Exception {
    try (SingleFetchInputStream stream = newStream()) {
      byte[] tail = new byte[5];
      assertThat(stream.readTail(tail, 0, 5))
          .as("Should return number of bytes copied")
          .isEqualTo(5);
      for (int i = 0; i < 5; i += 1) {
        assertThat(tail[i]).as("Byte i should be 30 + i").isEqualTo((byte) (30 + i));
      }

      byte[] over = new byte[40];
      assertThat(stream.readTail(over, 0, 40)).as("Should cap at file length").isEqualTo(35);
      for (int i = 0; i < 35; i += 1) {
        assertThat(over[i]).as("Byte i should be i").isEqualTo((byte) i);
      }

      byte[] offsetBuf = new byte[10];
      assertThat(stream.readTail(offsetBuf, 4, 6))
          .as("Should return number of bytes copied")
          .isEqualTo(6);
      for (int i = 0; i < 6; i += 1) {
        assertThat(offsetBuf[4 + i])
            .as("Byte at offset+i should be 29 + i")
            .isEqualTo((byte) (29 + i));
      }
    }
  }

  @Test
  public void testSeek() throws Exception {
    try (SingleFetchInputStream stream = newStream()) {
      stream.seek(35);
      assertThat(stream.read()).as("Should return -1 after seeking to end of stream").isEqualTo(-1);

      assertThatThrownBy(() -> stream.seek(36))
          .isInstanceOf(EOFException.class)
          .hasMessageContaining("Cannot seek to position 36");

      assertThatThrownBy(() -> stream.seek(-1))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("position is negative");
    }
  }

  @Test
  public void testClose() throws Exception {
    SingleFetchInputStream stream = newStream();
    stream.close();

    assertThatThrownBy(() -> stream.read())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot read: already closed");

    assertThatThrownBy(() -> stream.read(new byte[5], 0, 5))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot read: already closed");

    assertThatThrownBy(() -> stream.seek(0))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot seek: already closed");

    stream.close();
  }
}
