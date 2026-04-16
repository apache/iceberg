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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class TestUUIDUtil {

  @Test
  public void uuidV7HasVersionAndVariant() {
    UUID uuid = UUIDUtil.generateUuidV7();
    assertThat(uuid.version()).isEqualTo(7);
    assertThat(uuid.variant()).isEqualTo(2);
  }

  @Test
  public void convertFromBytesRoundTrip() {
    // the byte[] overload must produce the same result as the UUID overload
    String uuidStr = "f79c3e09-677c-4bbd-a479-3f349cb785e7";
    UUID uuid = UUID.fromString(uuidStr);

    ByteBuffer expected = UUIDUtil.convertToByteBuffer(uuid);
    ByteBuffer actual = UUIDUtil.convertToByteBuffer(uuidStr.getBytes(StandardCharsets.US_ASCII));

    assertThat(actual.array()).isEqualTo(expected.array());
  }

  @Test
  public void convertFromBytesKnownVector() {
    // known test vector from TestConversions
    byte[] uuidBytes = "f79c3e09-677c-4bbd-a479-3f349cb785e7".getBytes(StandardCharsets.US_ASCII);
    ByteBuffer result = UUIDUtil.convertToByteBuffer(uuidBytes);

    byte[] expectedBytes = {
      -9, -100, 62, 9, 103, 124, 75, -67, -92, 121, 63, 52, -100, -73, -123, -25
    };
    assertThat(result.array()).isEqualTo(expectedBytes);
  }

  @Test
  public void convertFromBytesAllZeros() {
    byte[] uuidBytes = "00000000-0000-0000-0000-000000000000".getBytes(StandardCharsets.US_ASCII);
    ByteBuffer result = UUIDUtil.convertToByteBuffer(uuidBytes);
    assertThat(result.array()).isEqualTo(new byte[16]);
  }

  @Test
  public void convertFromBytesAllFs() {
    byte[] uuidBytes = "ffffffff-ffff-ffff-ffff-ffffffffffff".getBytes(StandardCharsets.US_ASCII);
    ByteBuffer result = UUIDUtil.convertToByteBuffer(uuidBytes);

    byte[] expected = new byte[16];
    java.util.Arrays.fill(expected, (byte) 0xFF);
    assertThat(result.array()).isEqualTo(expected);
  }

  @Test
  public void convertFromBytesBufferReuse() {
    byte[] uuidBytes = "f79c3e09-677c-4bbd-a479-3f349cb785e7".getBytes(StandardCharsets.US_ASCII);
    ByteBuffer reuse = ByteBuffer.allocate(16);
    reuse.order(ByteOrder.BIG_ENDIAN);

    ByteBuffer result = UUIDUtil.convertToByteBuffer(uuidBytes, reuse);
    assertThat(result).isSameAs(reuse);

    ByteBuffer expected =
        UUIDUtil.convertToByteBuffer(UUID.fromString("f79c3e09-677c-4bbd-a479-3f349cb785e7"));
    assertThat(result.array()).isEqualTo(expected.array());
  }

  @Test
  public void convertFromBytesMixedCase() {
    byte[] lower = "f79c3e09-677c-4bbd-a479-3f349cb785e7".getBytes(StandardCharsets.US_ASCII);
    byte[] upper = "F79C3E09-677C-4BBD-A479-3F349CB785E7".getBytes(StandardCharsets.US_ASCII);
    byte[] mixed = "f79C3e09-677c-4Bbd-A479-3f349cB785E7".getBytes(StandardCharsets.US_ASCII);

    byte[] expected = UUIDUtil.convertToByteBuffer(lower).array();
    assertThat(UUIDUtil.convertToByteBuffer(upper).array()).isEqualTo(expected);
    assertThat(UUIDUtil.convertToByteBuffer(mixed).array()).isEqualTo(expected);
  }

  @Test
  public void convertFromBytesRandomRoundTrip() {
    for (int i = 0; i < 100; i++) {
      UUID uuid = UUID.randomUUID();
      String str = uuid.toString();
      byte[] asciiBytes = str.getBytes(StandardCharsets.US_ASCII);

      ByteBuffer fromUuid = UUIDUtil.convertToByteBuffer(uuid);
      ByteBuffer fromBytes = UUIDUtil.convertToByteBuffer(asciiBytes);

      assertThat(fromBytes.array())
          .as("Round-trip mismatch for UUID %s", str)
          .isEqualTo(fromUuid.array());
    }
  }

  @Test
  public void convertFromBytesRejectsWrongLength() {
    assertThatThrownBy(
            () ->
                UUIDUtil.convertToByteBuffer(
                    "f79c3e09-677c-4bbd-a479-3f349cb785e".getBytes(StandardCharsets.US_ASCII)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expected 36 bytes");

    assertThatThrownBy(
            () ->
                UUIDUtil.convertToByteBuffer(
                    "f79c3e09-677c-4bbd-a479-3f349cb785e7a".getBytes(StandardCharsets.US_ASCII)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expected 36 bytes");
  }

  @Test
  public void convertFromBytesRejectsMissingHyphen() {
    assertThatThrownBy(
            () ->
                UUIDUtil.convertToByteBuffer(
                    "f79c3e09x677c-4bbd-a479-3f349cb785e7".getBytes(StandardCharsets.US_ASCII)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Expected '-'");
  }

  @Test
  public void convertFromBytesRejectsInvalidHex() {
    assertThatThrownBy(
            () ->
                UUIDUtil.convertToByteBuffer(
                    "g79c3e09-677c-4bbd-a479-3f349cb785e7".getBytes(StandardCharsets.US_ASCII)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid hex character");
  }
}
