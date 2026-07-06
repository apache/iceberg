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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;

public class TestMalformedVariant {

  private static final ByteBuffer EMPTY_METADATA =
      ByteBuffer.wrap(new byte[] {0x01, 0x00, 0x00}).order(ByteOrder.LITTLE_ENDIAN);

  @Test
  public void testOversizedMetadataDictSize() {
    byte[] bytes = new byte[] {0x01, (byte) 0xFF};

    assertThatThrownBy(() -> SerializedMetadata.from(bytes))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("dictionary size");
  }

  @Test
  public void testUnsupportedMetadataVersionRejected() {
    // header low 4 bits = version; spec version is 1. 0x02 = version 2, must be rejected.
    byte[] bytes = new byte[] {0x02, 0x00, 0x00};

    assertThatThrownBy(() -> SerializedMetadata.from(bytes))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported version");
  }

  @Test
  public void testOversizedArrayNumElements() {
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {(byte) 0b10011, 0x00, 0x00, 0x00, 0x10, 0x00})
            .order(ByteOrder.LITTLE_ENDIAN);

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("element count");
  }

  @Test
  public void testOversizedObjectNumElements() {
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {(byte) 0b1000010, 0x00, 0x00, 0x00, 0x10, 0x00})
            .order(ByteOrder.LITTLE_ENDIAN);

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("element count");
  }

  @Test
  public void testFieldIdOutOfRange() {
    // field id check is lazy, so descend via get() to trigger it
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {0x02, 0x01, 0x05, 0x00, 0x01, 0x00})
            .order(ByteOrder.LITTLE_ENDIAN);

    VariantValue top = VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value);

    assertThatThrownBy(() -> top.asObject().get("anything"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("field id");
  }

  @Test
  public void testOutOfRangeChildOffsetInArray() {
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {0b0011, 0x01, (byte) 0xFF, 0x01, 0x00})
            .order(ByteOrder.LITTLE_ENDIAN);

    VariantValue top = VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value);

    assertThatThrownBy(() -> top.asArray().get(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("out of data region");
  }

  @Test
  public void testMalformedChildOffsetCaughtOnDescent() {
    // outer array constructs cleanly. Inner element is rejected only when accessed
    ByteBuffer value =
        ByteBuffer.wrap(
                new byte[] {0b0011, 0x01, 0x00, 0x06, (byte) 0b10011, 0x00, 0x00, 0x00, 0x10, 0x00})
            .order(ByteOrder.LITTLE_ENDIAN);

    VariantValue top = VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value);

    assertThatThrownBy(() -> top.asArray().get(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("element count");
  }

  @Test
  public void testNegativeMetadataEndOffset() {
    byte[] metadata = {
      (byte) 0b11000001, 0x00, 0x00, 0x00, 0x00, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
    };

    assertThatThrownBy(() -> SerializedMetadata.from(metadata))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("negative end offset");
  }

  @Test
  public void testNegativeDictOffsetInGet() {
    byte[] metadata = {
      (byte) 0b11000001,
      0x01,
      0x00,
      0x00,
      0x00,
      (byte) 0xFF,
      (byte) 0xFF,
      (byte) 0xFF,
      (byte) 0xFF,
      0x00,
      0x00,
      0x00,
      0x00
    };

    SerializedMetadata parsed = SerializedMetadata.from(metadata);

    assertThatThrownBy(() -> parsed.get(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("dict entry");
  }

  @Test
  public void testOversizedPrimitiveStringSize() {
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {(byte) 0b1000000, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x7F})
            .order(ByteOrder.LITTLE_ENDIAN);

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("payload");
  }

  @Test
  public void testOffsetExceedsDeclaredDataLengthInObject() {
    byte[] metadata = {0x01, 0x01, 0x00, 0x01, 'a'};
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {0x02, 0x01, 0x00, 0x32, 0x00, 0x00})
            .order(ByteOrder.LITTLE_ENDIAN);

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(metadata), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("declared data length");
  }

  @Test
  public void testOffsetArithmeticOverflowInObject() {
    // numElements * offsetSize would overflow signed int
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {0x4E, 0x33, 0x33, 0x33, 0x33, (byte) 0xFF, (byte) 0xFF, 0x3F})
            .order(ByteOrder.LITTLE_ENDIAN);

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("element count");
  }

  @Test
  public void testArrayDataOffsetIntOverflow() {
    // large array, offsetSize=4, numElements=0x20000000 (~536M).
    // (1+numElements)*offsetSize overflows signed int; long guard must fire first.
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {(byte) 0b11111, 0x00, 0x00, 0x00, 0x20, 0x00})
            .order(ByteOrder.LITTLE_ENDIAN);

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("element count");
  }

  @Test
  public void testObjectDataOffsetIntOverflow() {
    // large object, offsetSize=4, fieldIdSize=4, numElements=0x20000000 (~536M).
    // int sub-expressions for offsetListOffset and dataOffset overflow; long guard fires first.
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {(byte) 0b1111110, 0x00, 0x00, 0x00, 0x20, 0x00})
            .order(ByteOrder.LITTLE_ENDIAN);

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("element count");
  }

  @Test
  public void testNonMonotonicDictOffset() {
    byte[] metadata = {0x01, 0x02, 0x00, 0x05, 0x03, 'A', 'B', 'C', 'D', 'E'};

    SerializedMetadata parsed = SerializedMetadata.from(metadata);

    assertThatThrownBy(() -> parsed.get(1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("dict entry");
  }

  @Test
  public void testDuplicateFieldOffsetsInObject() {
    byte[] metadata = {0x01, 0x01, 0x00, 0x01, 'a'};
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {0x02, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00})
            .order(ByteOrder.LITTLE_ENDIAN);

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(metadata), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("duplicate field offsets");
  }

  @Test
  public void testEmptyChildValueBufferInArray() {
    // offset[0] == offset[1] produces a zero-length child slice
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {0b0011, 0x01, 0x00, 0x00}).order(ByteOrder.LITTLE_ENDIAN);

    VariantValue top = VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value);

    assertThatThrownBy(() -> top.asArray().get(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("empty value buffer");
  }

  @Test
  public void testShortStringLengthExceedsBuffer() {
    // header declares length=7, but only the header byte is present
    ByteBuffer value = ByteBuffer.wrap(new byte[] {0x1D}).order(ByteOrder.LITTLE_ENDIAN);

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("length");
  }

  @Test
  public void testNegativeObjectNumElements() {
    // largeSize object header, four 0xFF bytes read as int32 -1
    ByteBuffer value =
        ByteBuffer.wrap(
                new byte[] {(byte) 0b1000010, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF})
            .order(ByteOrder.LITTLE_ENDIAN);

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("negative element count");
  }

  @Test
  public void testNegativeMetadataDictSize() {
    // offsetSize=4, four 0xFF bytes read as int32 -1
    byte[] metadata = {(byte) 0b11000001, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};

    assertThatThrownBy(() -> SerializedMetadata.from(metadata))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("negative dictionary size");
  }

  @Test
  public void testOversizedMetadataEndOffset() {
    // dictSize=0, lastOffset=0xFF claims 255 bytes of dict data past the 3-byte buffer
    byte[] metadata = {0x01, 0x00, (byte) 0xFF};

    assertThatThrownBy(() -> SerializedMetadata.from(metadata))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("end offset");
  }

  @Test
  public void testTruncatedPrimitiveSizeField() {
    // string header but buffer is too short to contain the 4-byte size field
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {(byte) 0b1000000, 0x00}).order(ByteOrder.LITTLE_ENDIAN);

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("size field");
  }

  @Test
  public void testOverdeepNestingRejected() {
    int chainDepth = VariantUtil.MAX_VARIANT_DEPTH + 1;
    byte[] bytes = buildNestedArrayChain(chainDepth);
    ByteBuffer value = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

    VariantValue top = VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value);
    VariantArray cursor = top.asArray();
    for (int i = 0; i < VariantUtil.MAX_VARIANT_DEPTH; i += 1) {
      cursor = cursor.get(0).asArray();
    }

    VariantArray finalCursor = cursor;
    assertThatThrownBy(() -> finalCursor.get(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Invalid variant: nesting depth %d exceeds maximum %d",
                VariantUtil.MAX_VARIANT_DEPTH + 1, VariantUtil.MAX_VARIANT_DEPTH));
  }

  private static byte[] buildNestedArrayChain(int arrayLevels) {
    final byte arrayHeader = 0b0111; // small array, offsetSize=2
    final int perLevel = 6;
    final int leafSize = 1;
    int total = perLevel * arrayLevels + leafSize;
    byte[] buf = new byte[total];
    for (int i = 0; i < arrayLevels; i += 1) {
      int pos = i * perLevel;
      int innerLen = total - pos - perLevel;
      buf[pos] = arrayHeader;
      buf[pos + 1] = 0x01;
      buf[pos + 2] = 0x00;
      buf[pos + 3] = 0x00;
      buf[pos + 4] = (byte) (innerLen & 0xFF);
      buf[pos + 5] = (byte) ((innerLen >> 8) & 0xFF);
    }
    buf[total - 1] = 0x00;
    return buf;
  }
}
