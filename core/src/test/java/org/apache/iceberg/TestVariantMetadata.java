/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Set;
import org.apache.iceberg.util.RandomUtil;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestVariantMetadata {
  private final Random random = new Random(872591);

  @Test
  public void testEmptyVariantMetadata() {
    VariantMetadata metadata = VariantMetadata.from(VariantMetadata.EMPTY_V1_BUFFER);

    Assertions.assertThat(metadata.isSorted()).isFalse();
    Assertions.assertThat(metadata.dictionarySize()).isEqualTo(0);
    Assertions.assertThatThrownBy(() -> metadata.get(0))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class);
  }

  @Test
  public void testHeaderSorted() {
    VariantMetadata metadata = VariantMetadata.from(new byte[] {0b10001, 0x00});

    Assertions.assertThat(metadata.isSorted()).isTrue();
    Assertions.assertThat(metadata.dictionarySize()).isEqualTo(0);
  }

  @Test
  public void testHeaderOffsetSize() {
    // offset size is 4-byte LE = 1
    Assertions.assertThat(
            VariantMetadata.from(new byte[] {(byte) 0b11010001, 0x01, 0x00, 0x00, 0x00})
                .dictionarySize())
        .isEqualTo(1);

    // offset size is 3-byte LE = 1
    Assertions.assertThat(
            VariantMetadata.from(new byte[] {(byte) 0b10010001, 0x01, 0x00, 0x00}).dictionarySize())
        .isEqualTo(1);

    // offset size is 2-byte LE = 1
    Assertions.assertThat(
            VariantMetadata.from(new byte[] {(byte) 0b01010001, 0x01, 0x00}).dictionarySize())
        .isEqualTo(1);

    // offset size is 1-byte LE = 1
    Assertions.assertThat(
            VariantMetadata.from(new byte[] {(byte) 0b00010001, 0x01}).dictionarySize())
        .isEqualTo(1);
  }

  @Test
  public void testReadString() {
    VariantMetadata metadata =
        VariantMetadata.from(
            new byte[] {
              0b10001, 0x05, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 'a', 'b', 'c', 'd', 'e'
            });

    Assertions.assertThat(metadata.get(0)).isEqualTo("a");
    Assertions.assertThat(metadata.get(1)).isEqualTo("b");
    Assertions.assertThat(metadata.get(2)).isEqualTo("c");
    Assertions.assertThat(metadata.get(3)).isEqualTo("d");
    Assertions.assertThat(metadata.get(4)).isEqualTo("e");
    Assertions.assertThatThrownBy(() -> metadata.get(5))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class);
  }

  @Test
  public void testMultibyteString() {
    VariantMetadata metadata =
        VariantMetadata.from(
            new byte[] {
              0b10001, 0x05, 0x00, 0x01, 0x02, 0x05, 0x06, 0x07, 'a', 'b', 'x', 'y', 'z', 'd', 'e'
            });

    Assertions.assertThat(metadata.get(0)).isEqualTo("a");
    Assertions.assertThat(metadata.get(1)).isEqualTo("b");
    Assertions.assertThat(metadata.get(2)).isEqualTo("xyz");
    Assertions.assertThat(metadata.get(3)).isEqualTo("d");
    Assertions.assertThat(metadata.get(4)).isEqualTo("e");
    Assertions.assertThatThrownBy(() -> metadata.get(5))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class);
  }

  @Test
  public void testTwoByteOffsets() {
    VariantMetadata metadata =
        VariantMetadata.from(
            new byte[] {
              0b1010001, 0x05, 0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00, 0x05, 0x00, 0x06, 0x00,
              0x07, 0x00, 'a', 'b', 'x', 'y', 'z', 'd', 'e'
            });

    Assertions.assertThat(metadata.get(0)).isEqualTo("a");
    Assertions.assertThat(metadata.get(1)).isEqualTo("b");
    Assertions.assertThat(metadata.get(2)).isEqualTo("xyz");
    Assertions.assertThat(metadata.get(3)).isEqualTo("d");
    Assertions.assertThat(metadata.get(4)).isEqualTo("e");
    Assertions.assertThatThrownBy(() -> metadata.get(5))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class);
  }

  @Test
  public void testFindStringSorted() {
    VariantMetadata metadata =
        VariantMetadata.from(
            new byte[] {
              0b10001, 0x05, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 'a', 'b', 'c', 'd', 'e'
            });
    Assertions.assertThat(metadata.id("A")).isEqualTo(-1);
    Assertions.assertThat(metadata.id("a")).isEqualTo(0);
    Assertions.assertThat(metadata.id("aa")).isEqualTo(-1);
    Assertions.assertThat(metadata.id("b")).isEqualTo(1);
    Assertions.assertThat(metadata.id("bb")).isEqualTo(-1);
    Assertions.assertThat(metadata.id("c")).isEqualTo(2);
    Assertions.assertThat(metadata.id("cc")).isEqualTo(-1);
    Assertions.assertThat(metadata.id("d")).isEqualTo(3);
    Assertions.assertThat(metadata.id("dd")).isEqualTo(-1);
    Assertions.assertThat(metadata.id("e")).isEqualTo(4);
    Assertions.assertThat(metadata.id("ee")).isEqualTo(-1);
  }

  @Test
  public void testFindStringUnsorted() {
    VariantMetadata metadata =
        VariantMetadata.from(
            new byte[] {
              0b00001, 0x05, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 'e', 'd', 'c', 'b', 'a'
            });
    Assertions.assertThat(metadata.id("A")).isEqualTo(-1);
    Assertions.assertThat(metadata.id("a")).isEqualTo(4);
    Assertions.assertThat(metadata.id("aa")).isEqualTo(-1);
    Assertions.assertThat(metadata.id("b")).isEqualTo(3);
    Assertions.assertThat(metadata.id("bb")).isEqualTo(-1);
    Assertions.assertThat(metadata.id("c")).isEqualTo(2);
    Assertions.assertThat(metadata.id("cc")).isEqualTo(-1);
    Assertions.assertThat(metadata.id("d")).isEqualTo(1);
    Assertions.assertThat(metadata.id("dd")).isEqualTo(-1);
    Assertions.assertThat(metadata.id("e")).isEqualTo(0);
    Assertions.assertThat(metadata.id("ee")).isEqualTo(-1);
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
    VariantMetadata metadata = VariantMetadata.from(buffer);

    Assertions.assertThat(metadata.dictionarySize()).isEqualTo(10_000);
    Assertions.assertThat(metadata.id(lastKey)).isGreaterThan(0);
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
    VariantMetadata metadata = VariantMetadata.from(buffer);

    Assertions.assertThat(metadata.dictionarySize()).isEqualTo(100_000);
    Assertions.assertThat(metadata.id(lastKey)).isGreaterThan(0);
  }

  @Test
  public void testInvalidMetadataVersion() {
    Assertions.assertThatThrownBy(() -> VariantMetadata.from(new byte[] {0x02, 0x00}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unsupported version: 2");
  }

  @Test
  public void testMissingLength() {
    Assertions.assertThatThrownBy(() -> VariantMetadata.from(new byte[] {0x01}))
        .isInstanceOf(IndexOutOfBoundsException.class);
  }

  @Test
  public void testLengthTooShort() {
    // missing the 4th length byte
    Assertions.assertThatThrownBy(
            () -> VariantMetadata.from(new byte[] {(byte) 0b11010001, 0x00, 0x00, 0x00}))
        .isInstanceOf(IndexOutOfBoundsException.class);
  }
}
