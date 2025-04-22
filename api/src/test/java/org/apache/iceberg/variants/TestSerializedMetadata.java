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
package org.apache.iceberg.variants;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.RandomUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestSerializedMetadata {
  private final Random random = new Random(872591);

  @Test
  @SuppressWarnings("checkstyle:AssertThatThrownByWithMessageCheck")
  public void testEmptyVariantMetadata() {
    SerializedMetadata metadata = SerializedMetadata.EMPTY_V1_METADATA;

    assertThat(metadata.isSorted()).isFalse();
    assertThat(metadata.dictionarySize()).isEqualTo(0);
    // no check on the underlying error msg as it might be missing based on the JDK version
    assertThatThrownBy(() -> metadata.get(0)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
  }

  @Test
  public void testHeaderSorted() {
    SerializedMetadata metadata = SerializedMetadata.from(new byte[] {0b10001, 0x00, 0x00});

    assertThat(metadata.isSorted()).isTrue();
    assertThat(metadata.dictionarySize()).isEqualTo(0);
  }

  @Test
  public void testHeaderOffsetSize() {
    // offset size is 4-byte LE = 1
    assertThat(
            SerializedMetadata.from(
                    new byte[] {
                      (byte) 0b11010001,
                      0x01,
                      0x00,
                      0x00,
                      0x00,
                      0x00,
                      0x00,
                      0x00,
                      0x00,
                      0x00,
                      0x00,
                      0x00,
                      0x00
                    })
                .dictionarySize())
        .isEqualTo(1);

    // offset size is 3-byte LE = 1
    assertThat(
            SerializedMetadata.from(
                    new byte[] {
                      (byte) 0b10010001, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
                    })
                .dictionarySize())
        .isEqualTo(1);

    // offset size is 2-byte LE = 1
    assertThat(
            SerializedMetadata.from(
                    new byte[] {(byte) 0b01010001, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00})
                .dictionarySize())
        .isEqualTo(1);

    // offset size is 1-byte LE = 1
    assertThat(
            SerializedMetadata.from(new byte[] {(byte) 0b00010001, 0x01, 0x00, 0x00})
                .dictionarySize())
        .isEqualTo(1);
  }

  @Test
  @SuppressWarnings("checkstyle:AssertThatThrownByWithMessageCheck")
  public void testReadString() {
    SerializedMetadata metadata =
        SerializedMetadata.from(
            new byte[] {
              0b10001, 0x05, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 'a', 'b', 'c', 'd', 'e'
            });

    assertThat(metadata.get(0)).isEqualTo("a");
    assertThat(metadata.get(1)).isEqualTo("b");
    assertThat(metadata.get(2)).isEqualTo("c");
    assertThat(metadata.get(3)).isEqualTo("d");
    assertThat(metadata.get(4)).isEqualTo("e");
    // no check on the underlying error msg as it might be missing based on the JDK version
    assertThatThrownBy(() -> metadata.get(5)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
  }

  @Test
  @SuppressWarnings("checkstyle:AssertThatThrownByWithMessageCheck")
  public void testMultibyteString() {
    SerializedMetadata metadata =
        SerializedMetadata.from(
            new byte[] {
              0b10001, 0x05, 0x00, 0x01, 0x02, 0x05, 0x06, 0x07, 'a', 'b', 'x', 'y', 'z', 'd', 'e'
            });

    assertThat(metadata.get(0)).isEqualTo("a");
    assertThat(metadata.get(1)).isEqualTo("b");
    assertThat(metadata.get(2)).isEqualTo("xyz");
    assertThat(metadata.get(3)).isEqualTo("d");
    assertThat(metadata.get(4)).isEqualTo("e");
    // no check on the underlying error msg as it might be missing based on the JDK version
    assertThatThrownBy(() -> metadata.get(5)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
  }

  @Test
  @SuppressWarnings("checkstyle:AssertThatThrownByWithMessageCheck")
  public void testTwoByteOffsets() {
    SerializedMetadata metadata =
        SerializedMetadata.from(
            new byte[] {
              0b1010001, 0x05, 0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00, 0x05, 0x00, 0x06, 0x00,
              0x07, 0x00, 'a', 'b', 'x', 'y', 'z', 'd', 'e'
            });

    assertThat(metadata.get(0)).isEqualTo("a");
    assertThat(metadata.get(1)).isEqualTo("b");
    assertThat(metadata.get(2)).isEqualTo("xyz");
    assertThat(metadata.get(3)).isEqualTo("d");
    assertThat(metadata.get(4)).isEqualTo("e");
    // no check on the underlying error msg as it might be missing based on the JDK version
    assertThatThrownBy(() -> metadata.get(5)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
  }

  @Test
  public void testFindStringSorted() {
    SerializedMetadata metadata =
        SerializedMetadata.from(
            new byte[] {
              0b10001, 0x05, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 'a', 'b', 'c', 'd', 'e'
            });
    assertThat(metadata.id("A")).isEqualTo(-1);
    assertThat(metadata.id("a")).isEqualTo(0);
    assertThat(metadata.id("aa")).isEqualTo(-1);
    assertThat(metadata.id("b")).isEqualTo(1);
    assertThat(metadata.id("bb")).isEqualTo(-1);
    assertThat(metadata.id("c")).isEqualTo(2);
    assertThat(metadata.id("cc")).isEqualTo(-1);
    assertThat(metadata.id("d")).isEqualTo(3);
    assertThat(metadata.id("dd")).isEqualTo(-1);
    assertThat(metadata.id("e")).isEqualTo(4);
    assertThat(metadata.id("ee")).isEqualTo(-1);
  }

  @Test
  public void testFindStringUnsorted() {
    SerializedMetadata metadata =
        SerializedMetadata.from(
            new byte[] {
              0b00001, 0x05, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 'e', 'd', 'c', 'b', 'a'
            });
    assertThat(metadata.id("A")).isEqualTo(-1);
    assertThat(metadata.id("a")).isEqualTo(4);
    assertThat(metadata.id("aa")).isEqualTo(-1);
    assertThat(metadata.id("b")).isEqualTo(3);
    assertThat(metadata.id("bb")).isEqualTo(-1);
    assertThat(metadata.id("c")).isEqualTo(2);
    assertThat(metadata.id("cc")).isEqualTo(-1);
    assertThat(metadata.id("d")).isEqualTo(1);
    assertThat(metadata.id("dd")).isEqualTo(-1);
    assertThat(metadata.id("e")).isEqualTo(0);
    assertThat(metadata.id("ee")).isEqualTo(-1);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testTwoByteFieldIds(boolean sortFieldNames) {
    Set<String> keySet = Sets.newHashSet();
    String lastKey = null;
    for (int i = 0; i < 10_000; i += 1) {
      lastKey = RandomUtil.generateString(10, random);
      keySet.add(lastKey);
    }

    ByteBuffer buffer = VariantTestUtil.createMetadata(keySet, sortFieldNames);
    SerializedMetadata metadata = SerializedMetadata.from(buffer);

    assertThat(metadata.dictionarySize()).isEqualTo(10_000);
    assertThat(metadata.id(lastKey)).isGreaterThan(0);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testThreeByteFieldIds(boolean sortFieldNames) {
    Set<String> keySet = Sets.newHashSet();
    String lastKey = null;
    for (int i = 0; i < 100_000; i += 1) {
      lastKey = RandomUtil.generateString(10, random);
      keySet.add(lastKey);
    }

    ByteBuffer buffer = VariantTestUtil.createMetadata(keySet, sortFieldNames);
    SerializedMetadata metadata = SerializedMetadata.from(buffer);

    assertThat(metadata.dictionarySize()).isEqualTo(100_000);
    assertThat(metadata.id(lastKey)).isGreaterThan(0);
  }

  @Test
  public void testInvalidMetadataVersion() {
    assertThatThrownBy(() -> SerializedMetadata.from(new byte[] {0x02, 0x00}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unsupported version: 2");
  }

  @Test
  @SuppressWarnings("checkstyle:AssertThatThrownByWithMessageCheck")
  public void testMissingLength() {
    // no check on the underlying error msg as it might be missing based on the JDK version
    assertThatThrownBy(() -> SerializedMetadata.from(new byte[] {0x01}))
        .isInstanceOf(IndexOutOfBoundsException.class);
  }

  @Test
  @SuppressWarnings("checkstyle:AssertThatThrownByWithMessageCheck")
  public void testLengthTooShort() {
    // missing the 4th length byte
    // no check on the underlying error msg as it might be missing based on the JDK version
    assertThatThrownBy(
            () -> SerializedMetadata.from(new byte[] {(byte) 0b11010001, 0x00, 0x00, 0x00}))
        .isInstanceOf(IndexOutOfBoundsException.class);
  }
}
