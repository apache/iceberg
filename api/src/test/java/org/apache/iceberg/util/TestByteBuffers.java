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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;

public class TestByteBuffers {
  @Test
  public void testReadByteUnsigned() {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {(byte) 0xFF});
    assertThat(ByteBuffers.readByte(buffer, 0)).isEqualTo(255);
  }

  @Test
  public void testRead2ByteUnsigned() {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF});
    assertThat(ByteBuffers.readLittleEndianUnsigned(buffer, 0, 2)).isEqualTo(65535);
  }

  @Test
  public void testRead2ByteLittleEndian() {
    ByteBuffer buffer =
        ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0x00}).order(ByteOrder.BIG_ENDIAN);
    assertThat(ByteBuffers.readLittleEndianUnsigned(buffer, 0, 2)).isEqualTo(255);
    assertThat(ByteBuffers.readLittleEndianInt16(buffer, 0)).isEqualTo((short) 255);
  }

  @Test
  public void testRead2ByteSigned() {
    ByteBuffer buffer =
        ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF}).order(ByteOrder.BIG_ENDIAN);
    assertThat(ByteBuffers.readLittleEndianInt16(buffer, 0)).isEqualTo((short) -1);
  }

  @Test
  public void testRead3ByteUnsigned() {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
    assertThat(ByteBuffers.readLittleEndianUnsigned(buffer, 0, 3)).isEqualTo(16777215);
  }

  @Test
  public void testRead3ByteLittleEndian() {
    ByteBuffer buffer =
        ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0x00, (byte) 0x00})
            .order(ByteOrder.BIG_ENDIAN);
    assertThat(ByteBuffers.readLittleEndianUnsigned(buffer, 0, 3)).isEqualTo(255);
  }

  @Test
  public void testRead4ByteLittleEndian() {
    ByteBuffer buffer =
        ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0x00, (byte) 0x00, (byte) 0x00})
            .order(ByteOrder.BIG_ENDIAN);
    assertThat(ByteBuffers.readLittleEndianUnsigned(buffer, 0, 4)).isEqualTo(255);
    assertThat(ByteBuffers.readLittleEndianInt32(buffer, 0)).isEqualTo(255);
  }

  @Test
  public void testRead4ByteSigned() {
    ByteBuffer buffer =
        ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF})
            .order(ByteOrder.BIG_ENDIAN);
    assertThat(ByteBuffers.readLittleEndianInt32(buffer, 0)).isEqualTo(-1);
  }

  @Test
  public void testRead8ByteLittleEndian() {
    ByteBuffer buffer =
        ByteBuffer.wrap(
                new byte[] {
                  (byte) 0xFF,
                  (byte) 0x00,
                  (byte) 0x00,
                  (byte) 0x00,
                  (byte) 0x00,
                  (byte) 0x00,
                  (byte) 0x00,
                  (byte) 0x00
                })
            .order(ByteOrder.BIG_ENDIAN);
    assertThat(ByteBuffers.readLittleEndianInt64(buffer, 0)).isEqualTo(255);
  }

  @Test
  public void testRead8ByteSigned() {
    ByteBuffer buffer =
        ByteBuffer.wrap(
                new byte[] {
                  (byte) 0xFF,
                  (byte) 0xFF,
                  (byte) 0xFF,
                  (byte) 0xFF,
                  (byte) 0xFF,
                  (byte) 0xFF,
                  (byte) 0xFF,
                  (byte) 0xFF
                })
            .order(ByteOrder.BIG_ENDIAN);
    assertThat(ByteBuffers.readLittleEndianInt64(buffer, 0)).isEqualTo(-1);
  }

  @Test
  public void testWrite1Byte() {
    ByteBuffer buffer = ByteBuffer.allocate(1);
    ByteBuffers.writeLittleEndianUnsigned(buffer, 255, 0, 1);
    ByteBuffer expected = ByteBuffer.wrap(new byte[] {(byte) 0xFF});
    assertThat(buffer).isEqualTo(expected);
  }

  @Test
  public void testWrite2ByteLittleEndian() {
    ByteBuffer buffer = ByteBuffer.allocate(2);
    ByteBuffers.writeLittleEndianUnsigned(buffer, 255, 0, 2);
    ByteBuffer expected = ByteBuffer.wrap(new byte[] {(byte) 0xFF, 0x00});
    assertThat(buffer).isEqualTo(expected);
  }

  @Test
  public void testWrite3ByteLittleEndian() {
    ByteBuffer buffer = ByteBuffer.allocate(3);
    ByteBuffers.writeLittleEndianUnsigned(buffer, 255, 0, 3);
    ByteBuffer expected = ByteBuffer.wrap(new byte[] {(byte) 0xFF, 0x00, 0x00});
    assertThat(buffer).isEqualTo(expected);
  }

  @Test
  public void testWrite4ByteLittleEndian() {
    ByteBuffer buffer = ByteBuffer.allocate(4);
    ByteBuffers.writeLittleEndianUnsigned(buffer, 255, 0, 4);
    ByteBuffer expected = ByteBuffer.wrap(new byte[] {(byte) 0xFF, 0x00, 0x00, 0x00});
    assertThat(buffer).isEqualTo(expected);
  }
}
