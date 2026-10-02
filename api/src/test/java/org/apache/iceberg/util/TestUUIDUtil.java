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
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestUUIDUtil {

  @Test
  public void uuidV7HasVersionAndVariant() {
    UUID uuid = UUIDUtil.generateUuidV7();
    assertThat(uuid.version()).isEqualTo(7);
    assertThat(uuid.variant()).isEqualTo(2);
  }

  /**
   * The direct path must agree with UUID.fromString for every UUID, since it replaces it on the
   * Spark write path and the bytes end up in data files.
   */
  @Test
  public void directConversionMatchesUuidFromString() {
    for (int i = 0; i < 1000; i += 1) {
      UUID expected = UUID.randomUUID();
      assertThat(bytesOf(expected.toString()))
          .as("round trip of %s", expected)
          .isEqualTo(UUIDUtil.convert(UUIDUtil.convertToByteBuffer(expected).array()))
          .isEqualTo(expected);
    }
  }

  @Test
  public void directConversionHandlesBoundaryValues() {
    // all-zero, all-one, and a value whose most significant bit is set in both longs
    for (String text :
        new String[] {
          "00000000-0000-0000-0000-000000000000",
          "ffffffff-ffff-ffff-ffff-ffffffffffff",
          "80000000-0000-0000-8000-000000000000",
          "7fffffff-ffff-ffff-7fff-ffffffffffff"
        }) {
      assertThat(bytesOf(text)).as("boundary value %s", text).isEqualTo(UUID.fromString(text));
    }
  }

  @Test
  public void directConversionIsCaseInsensitive() {
    String lower = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
    assertThat(bytesOf(lower.toUpperCase(java.util.Locale.ROOT))).isEqualTo(UUID.fromString(lower));
  }

  @Test
  public void directConversionReusesTheGivenBuffer() {
    ByteBuffer reuse = ByteBuffer.allocate(16);
    UUID uuid = UUID.randomUUID();
    ByteBuffer result =
        UUIDUtil.convertToByteBuffer(uuid.toString().getBytes(StandardCharsets.UTF_8), reuse);

    assertThat(result).isSameAs(reuse);
    assertThat(UUIDUtil.convert(result.duplicate())).isEqualTo(uuid);
  }

  @Test
  public void directConversionReadsAtAnOffset() {
    UUID uuid = UUID.randomUUID();
    byte[] padded = ("xx" + uuid + "yy").getBytes(StandardCharsets.UTF_8);
    ByteBuffer result = UUIDUtil.convertToByteBuffer(padded, 2, 36, null);

    assertThat(UUIDUtil.convert(result.duplicate())).isEqualTo(uuid);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "", // empty
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1", // one character short
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a111", // one character long
        "a0eebc999c0b4ef8bb6d6bb9bd380a11", // no dashes
        "a0eebc99+9c0b-4ef8-bb6d-6bb9bd380a11", // wrong separator
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1g", // non hexadecimal digit
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1 " // trailing space
      })
  public void directConversionRejectsNonCanonicalText(String text) {
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    assertThatThrownBy(() -> UUIDUtil.convertToByteBuffer(bytes, null))
        .as("must reject %s", text)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid UUID text");
  }

  /**
   * UUID.fromString zero-extends short groups, so "1-2-3-4-5" parses there. The direct path is
   * deliberately stricter, because a value that reaches a data file should be canonical.
   */
  @Test
  public void directConversionIsStricterThanUuidFromString() {
    assertThat(UUID.fromString("1-2-3-4-5")).isNotNull();
    assertThatThrownBy(
            () -> UUIDUtil.convertToByteBuffer("1-2-3-4-5".getBytes(StandardCharsets.UTF_8), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expected 36 characters");
  }

  private static UUID bytesOf(String uuidText) {
    ByteBuffer buffer =
        UUIDUtil.convertToByteBuffer(uuidText.getBytes(StandardCharsets.UTF_8), null);
    return UUIDUtil.convert(buffer.duplicate());
  }
}
