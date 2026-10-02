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
import java.nio.charset.StandardCharsets;
import java.util.Locale;
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
  public void convertStringBytesMatchesUuidConversion() {
    for (int i = 0; i < 100; i += 1) {
      UUID uuid = UUID.randomUUID();
      byte[] stringBytes = uuid.toString().getBytes(StandardCharsets.US_ASCII);

      byte[] fromString = UUIDUtil.convertToByteBuffer(stringBytes, null).array();
      byte[] fromUuid = UUIDUtil.convertToByteBuffer(uuid, null).array();

      assertThat(fromString).containsExactly(fromUuid).containsExactly(UUIDUtil.convert(uuid));
    }
  }

  @Test
  public void convertStringBytesHandlesEdgeValues() {
    for (String value :
        new String[] {
          "00000000-0000-0000-0000-000000000000",
          "ffffffff-ffff-ffff-ffff-ffffffffffff",
          "12345678-90ab-cdef-1234-567890abcdef"
        }) {
      UUID uuid = UUID.fromString(value);
      byte[] stringBytes = value.getBytes(StandardCharsets.US_ASCII);

      assertThat(UUIDUtil.convertToByteBuffer(stringBytes, null).array())
          .containsExactly(UUIDUtil.convert(uuid));
    }
  }

  @Test
  public void convertStringBytesAcceptsUppercaseHex() {
    UUID uuid = UUID.randomUUID();
    byte[] upper = uuid.toString().toUpperCase(Locale.ROOT).getBytes(StandardCharsets.US_ASCII);

    assertThat(UUIDUtil.convertToByteBuffer(upper, null).array())
        .containsExactly(UUIDUtil.convert(uuid));
  }

  @Test
  public void convertStringBytesReusesBuffer() {
    UUID uuid = UUID.randomUUID();
    byte[] stringBytes = uuid.toString().getBytes(StandardCharsets.US_ASCII);

    ByteBuffer reuse = ByteBuffer.allocate(16);
    ByteBuffer result = UUIDUtil.convertToByteBuffer(stringBytes, reuse);

    assertThat(result).isSameAs(reuse);
    assertThat(result.array()).containsExactly(UUIDUtil.convert(uuid));
  }

  @Test
  public void convertToStringBytesMatchesUuidToString() {
    for (int i = 0; i < 100; i += 1) {
      UUID uuid = UUID.randomUUID();
      ByteBuffer raw = UUIDUtil.convertToByteBuffer(uuid, null);

      byte[] stringBytes = UUIDUtil.convertToStringBytes(raw, null);

      assertThat(new String(stringBytes, StandardCharsets.US_ASCII)).isEqualTo(uuid.toString());
    }
  }

  @Test
  public void convertToStringBytesIsRoundTrippable() {
    UUID uuid = UUID.randomUUID();

    byte[] stringBytes =
        UUIDUtil.convertToStringBytes(UUIDUtil.convertToByteBuffer(uuid, null), null);
    byte[] raw = UUIDUtil.convertToByteBuffer(stringBytes, null).array();

    assertThat(UUIDUtil.convert(raw)).isEqualTo(uuid);
  }

  @Test
  public void convertToStringBytesReusesBuffer() {
    UUID uuid = UUID.randomUUID();

    byte[] reuse = new byte[36];
    byte[] result = UUIDUtil.convertToStringBytes(UUIDUtil.convertToByteBuffer(uuid, null), reuse);

    assertThat(result).isSameAs(reuse);
    assertThat(new String(result, StandardCharsets.US_ASCII)).isEqualTo(uuid.toString());
  }

  @Test
  public void convertToByteBufferRejectsMalformedInput() {
    assertThatThrownBy(
            () ->
                UUIDUtil.convertToByteBuffer("too-short".getBytes(StandardCharsets.US_ASCII), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expected 36 ASCII bytes");

    // valid length, but the dash positions hold non-dash characters
    assertThatThrownBy(
            () ->
                UUIDUtil.convertToByteBuffer(
                    "000000000000000000000000000000000000".getBytes(StandardCharsets.US_ASCII),
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expected '-'");

    // valid layout, but a non-hex character in a hex group
    assertThatThrownBy(
            () ->
                UUIDUtil.convertToByteBuffer(
                    "zzzzzzzz-0000-0000-0000-000000000000".getBytes(StandardCharsets.US_ASCII),
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not a hex digit");
  }

  @Test
  public void convertToStringBytesRejectsWrongReuseLength() {
    ByteBuffer raw = UUIDUtil.convertToByteBuffer(UUID.randomUUID(), null);
    assertThatThrownBy(() -> UUIDUtil.convertToStringBytes(raw, new byte[16]))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expected 36 bytes");
  }
}
