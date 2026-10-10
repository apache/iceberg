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
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;

public class TestMalformedVariant {

  private static final ByteBuffer EMPTY_METADATA =
      ByteBuffer.wrap(new byte[] {0x01, 0x00, 0x00}).order(ByteOrder.LITTLE_ENDIAN);

  @Test
  public void testOversizedMetadataDictSize() {
    // metadata: [0x01 header - v1, offsetSize=1] [0xFF dictSize=255] - buffer stops before offsets
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
    // array: [0b10011 header - large, offsetSize=1] [4-byte numElements=0x00100000 (1M)]
    // buffer stops before the offset table can be read
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {(byte) 0b10011, 0x00, 0x00, 0x00, 0x10, 0x00})
            .order(ByteOrder.LITTLE_ENDIAN);

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("element count");
  }

  @Test
  public void testOversizedObjectNumElements() {
    // [0b1000010 hdr - large object] [4-byte numElements=0x00100000] - buffer stops before tables
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {(byte) 0b1000010, 0x00, 0x00, 0x00, 0x10, 0x00})
            .order(ByteOrder.LITTLE_ENDIAN);

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("element count");
  }

  @Test
  public void testFieldIdOutOfRange() {
    // [0x02 hdr] [0x01 numEl] [0x05 fieldId - out of range in empty dict] [0x00,0x01 offsets]
    // [0x00 data]; range check is lazy so descend via get() to trigger
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
    // [0b0011 hdr - small array, offsetSize=1] [0x01 numEl] [0xFF,0x01 offsets - end<start]
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
    // outer [0b0011 hdr] [0x01 numEl] [0x00,0x06 offsets]; inner large-array at byte 4
    // has numElements=0x00100000 - rejected only on descent, not at outer parse
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
    // metadata: [0b11000001 header - v1, offsetSize=4] [4-byte dictSize=0]
    //           [0xFFFFFFFF end offset as int32 = -1]
    byte[] metadata = {
      (byte) 0b11000001, 0x00, 0x00, 0x00, 0x00, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF
    };

    assertThatThrownBy(() -> SerializedMetadata.from(metadata))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("negative end offset");
  }

  @Test
  public void testNegativeDictOffsetInGet() {
    // metadata: [0b11000001 header - v1, offsetSize=4] [4-byte dictSize=1]
    //           [4-byte offset[0]=0xFFFFFFFF (= -1)] [4-byte offset[1]=0]
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
    // primitive: [0b1000000 header - STRING primitive]
    //            [0x7FFFFFFF as 4-byte size = 2GB claimed payload]
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {(byte) 0b1000000, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x7F})
            .order(ByteOrder.LITTLE_ENDIAN);

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("payload");
  }

  @Test
  public void testOffsetExceedsDeclaredDataLengthInObject() {
    // metadata: [0x01 hdr] [0x01 dictSize] [0x00,0x01 offsets] ['a']
    byte[] metadata = {0x01, 0x01, 0x00, 0x01, 'a'};
    // object: [0x02][0x01][0x00 fieldId][0x32 offset[0]=50 > dataLength=0][0x00][0x00]
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {0x02, 0x01, 0x00, 0x32, 0x00, 0x00})
            .order(ByteOrder.LITTLE_ENDIAN);

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(metadata), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("declared data length");
  }

  @Test
  public void testOffsetArithmeticOverflowInObject() {
    // object: [0x4E header - large, fieldIdSize=1, offsetSize=4] [4-byte numElements=0x33333333]
    // numElements * offsetSize overflows signed int; long guard fires first
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {0x4E, 0x33, 0x33, 0x33, 0x33, (byte) 0xFF, (byte) 0xFF, 0x3F})
            .order(ByteOrder.LITTLE_ENDIAN);

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("element count");
  }

  @Test
  public void testArrayDataOffsetIntOverflow() {
    // array: [0b11111 header - large, offsetSize=4] [4-byte numElements=0x20000000 (~536M)]
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
    // [0b1111110 hdr - large object, fieldIdSize=4, offsetSize=4] [4-byte numElements=0x20000000]
    // offsetListOffset and dataOffset int expressions overflow; long guard fires first
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {(byte) 0b1111110, 0x00, 0x00, 0x00, 0x20, 0x00})
            .order(ByteOrder.LITTLE_ENDIAN);

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("element count");
  }

  @Test
  public void testMetadataDataOffsetIntOverflow() {
    // metadata: [0b11000001 header - v1, offsetSize=4] [4-byte dictSize=0x20000000 (~536M)]
    // (1+dictSize)*offsetSize overflows signed int; long guard fires first.
    byte[] metadata = new byte[] {(byte) 0b11000001, 0x00, 0x00, 0x00, 0x20};

    assertThatThrownBy(() -> SerializedMetadata.from(metadata))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("dictionary size");
  }

  @Test
  public void testDictEntryEndBeforeStart() {
    // metadata: [0x01 header] [0x02 dictSize] [0x00,0x05,0x03 offsets - entry 1 ends BEFORE
    //           it starts] ['A','B','C','D','E' data]
    byte[] metadata = {0x01, 0x02, 0x00, 0x05, 0x03, 'A', 'B', 'C', 'D', 'E'};

    SerializedMetadata parsed = SerializedMetadata.from(metadata);

    assertThatThrownBy(() -> parsed.get(1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("dict entry");
  }

  @Test
  public void testSharedFieldOffsetsInObjectSucceeds() {
    // Shared offset is spec-legal (compacting writer dedups).
    // metadata: [0x01 hdr] [0x02 dictSize] [0x00,0x01,0x02 offsets] ['a','b']
    byte[] metadata = {0x01, 0x02, 0x00, 0x01, 0x02, 'a', 'b'};
    // object: [0x02 hdr] [0x02 numEl] [0x00,0x01 fieldIds] [0x00,0x00,0x01 offsets] [0x00 NULL]
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {0x02, 0x02, 0x00, 0x01, 0x00, 0x00, 0x01, 0x00})
            .order(ByteOrder.LITTLE_ENDIAN);

    VariantValue top = VariantValue.from(SerializedMetadata.from(metadata), value);
    VariantObject obj = top.asObject();

    assertThat(obj.numFields()).isEqualTo(2);
    assertThat(obj.get("a").type()).isEqualTo(PhysicalType.NULL);
    assertThat(obj.get("b").type()).isEqualTo(PhysicalType.NULL);
  }

  @Test
  public void testEmptyChildValueBufferInArray() {
    // array: [0b0011 header - small, offsetSize=1] [0x01 numElements]
    //        [0x00,0x00 offsets - offset[0]==offset[1] produces a zero-length child slice]
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {0b0011, 0x01, 0x00, 0x00}).order(ByteOrder.LITTLE_ENDIAN);

    VariantValue top = VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value);

    assertThatThrownBy(() -> top.asArray().get(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("empty value buffer");
  }

  @Test
  public void testShortStringLengthExceedsBuffer() {
    // short string: [0x1D header - length=7 in high 6 bits, basic type=SHORT_STRING]
    // header declares length=7, but only the header byte is present.
    ByteBuffer value = ByteBuffer.wrap(new byte[] {0x1D}).order(ByteOrder.LITTLE_ENDIAN);

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("length");
  }

  @Test
  public void testNegativeObjectNumElements() {
    // object: [0b1000010 header - large, fieldIdSize=1, offsetSize=1]
    //         [0xFFFFFFFF numElements as int32 = -1]
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
    // metadata: [0b11000001 header - v1, offsetSize=4] [0xFFFFFFFF dictSize as int32 = -1]
    byte[] metadata = {(byte) 0b11000001, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};

    assertThatThrownBy(() -> SerializedMetadata.from(metadata))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("negative dictionary size");
  }

  @Test
  public void testOversizedMetadataEndOffset() {
    // metadata: [0x01 header - v1, offsetSize=1] [0x00 dictSize=0]
    //           [0xFF end offset=255 exceeds 3-byte buffer]
    byte[] metadata = {0x01, 0x00, (byte) 0xFF};

    assertThatThrownBy(() -> SerializedMetadata.from(metadata))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("end offset");
  }

  @Test
  public void testTruncatedPrimitiveSizeField() {
    // primitive: [0b1000000 header - STRING primitive] [0x00 only 1 byte, need 4-byte size field]
    ByteBuffer value =
        ByteBuffer.wrap(new byte[] {(byte) 0b1000000, 0x00}).order(ByteOrder.LITTLE_ENDIAN);

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("size field");
  }

  @Test
  public void testUnknownPrimitiveTypeIdRejected() {
    int unknownTypeId = Primitives.TYPE_UUID + 1;
    byte header = (byte) (unknownTypeId << Primitives.PRIMITIVE_TYPE_SHIFT);
    ByteBuffer value = ByteBuffer.wrap(new byte[] {header}).order(ByteOrder.LITTLE_ENDIAN);

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Unknown primitive physical type: " + unknownTypeId);
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

  @Test
  public void testMaxNestingAcceptedArray() {
    // Array chain that reaches exactly depth MAX_VARIANT_DEPTH (the deepest allowed).
    byte[] bytes = buildNestedArrayChain(VariantUtil.MAX_VARIANT_DEPTH);
    ByteBuffer value = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

    VariantValue top = VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value);
    VariantArray cursor = top.asArray();
    for (int i = 0; i < VariantUtil.MAX_VARIANT_DEPTH - 1; i += 1) {
      cursor = cursor.get(0).asArray();
    }
    assertThat(cursor.get(0)).isNotNull();
  }

  @Test
  public void testMaxNestingAcceptedObject() {
    // Object chain that reaches exactly depth MAX_VARIANT_DEPTH. SerializedObject.get()
    // routes through the same depth counter as SerializedArray.get() but was not covered.
    byte[] metadata = {0x01, 0x01, 0x00, 0x01, 'a'};
    byte[] bytes = buildNestedObjectChain(VariantUtil.MAX_VARIANT_DEPTH);
    ByteBuffer value = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

    VariantValue top = VariantValue.from(SerializedMetadata.from(metadata), value);
    VariantObject cursor = top.asObject();
    for (int i = 0; i < VariantUtil.MAX_VARIANT_DEPTH - 1; i += 1) {
      cursor = cursor.get("a").asObject();
    }
    assertThat(cursor.get("a")).isNotNull();
  }

  @Test
  public void testOverdeepNestingRejectedObject() {
    byte[] metadata = {0x01, 0x01, 0x00, 0x01, 'a'};
    byte[] bytes = buildNestedObjectChain(VariantUtil.MAX_VARIANT_DEPTH + 1);
    ByteBuffer value = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

    VariantValue top = VariantValue.from(SerializedMetadata.from(metadata), value);
    VariantObject cursor = top.asObject();
    for (int i = 0; i < VariantUtil.MAX_VARIANT_DEPTH; i += 1) {
      cursor = cursor.get("a").asObject();
    }

    VariantObject finalCursor = cursor;
    assertThatThrownBy(() -> finalCursor.get("a"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Invalid variant: nesting depth %d exceeds maximum %d",
                VariantUtil.MAX_VARIANT_DEPTH + 1, VariantUtil.MAX_VARIANT_DEPTH));
  }

  @Test
  public void testNegativeDepthRejected() {
    // fromBuffer direct-call path; depth < 0 must be rejected before further parsing
    ByteBuffer value = ByteBuffer.wrap(new byte[] {0x00}).order(ByteOrder.LITTLE_ENDIAN);
    VariantMetadata metadata = SerializedMetadata.from(EMPTY_METADATA);

    assertThatThrownBy(() -> VariantUtil.fromBuffer(metadata, value, -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("negative depth");
  }

  @Test
  public void testExcessiveArrayNumElementsRejected() {
    // [0b10011 hdr - large array, offsetSize=1] [4-byte numElements = MAX_ELEMENTS + 1]
    // offsetSize=1 keeps offsetTableEnd within the buffer, so only the absolute cap catches this
    ByteBuffer value = ByteBuffer.allocate(5).order(ByteOrder.LITTLE_ENDIAN);
    value.put((byte) 0b10011);
    value.putInt(VariantUtil.MAX_ELEMENTS + 1);
    value.flip();

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exceeds maximum");
  }

  @Test
  public void testExcessiveObjectNumElementsRejected() {
    // [0b1000010 hdr - large object, fieldIdSize=1, offsetSize=1]
    // [4-byte numElements = MAX_ELEMENTS + 1] the absolute cap fires before the bounds check
    ByteBuffer value = ByteBuffer.allocate(5).order(ByteOrder.LITTLE_ENDIAN);
    value.put((byte) 0b1000010);
    value.putInt(VariantUtil.MAX_ELEMENTS + 1);
    value.flip();

    assertThatThrownBy(() -> VariantValue.from(SerializedMetadata.from(EMPTY_METADATA), value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exceeds maximum");
  }

  @Test
  public void testExcessiveMetadataDictSizeRejected() {
    // metadata: [0b11000001 hdr - v1, offsetSize=4] [4-byte dictSize = MAX_ELEMENTS + 1]
    // the absolute cap fires before the offset-table bounds check
    ByteBuffer metadata = ByteBuffer.allocate(5).order(ByteOrder.LITTLE_ENDIAN);
    metadata.put((byte) 0b11000001);
    metadata.putInt(VariantUtil.MAX_ELEMENTS + 1);
    metadata.flip();

    assertThatThrownBy(() -> SerializedMetadata.from(metadata))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exceeds maximum");
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

  private static byte[] buildNestedObjectChain(int objectLevels) {
    // small object, fieldIdSize=1, offsetSize=2, one field with dict id 0
    final byte objectHeader = 0x06;
    final int perLevel = 7;
    final int leafSize = 1;
    int total = perLevel * objectLevels + leafSize;
    byte[] buf = new byte[total];
    for (int i = 0; i < objectLevels; i += 1) {
      int pos = i * perLevel;
      int innerLen = total - pos - perLevel;
      buf[pos] = objectHeader;
      buf[pos + 1] = 0x01; // numElements
      buf[pos + 2] = 0x00; // fieldId = 0
      buf[pos + 3] = 0x00; // offset[0] low byte
      buf[pos + 4] = 0x00; // offset[0] high byte
      buf[pos + 5] = (byte) (innerLen & 0xFF); // offset[1] = innerLen (data length)
      buf[pos + 6] = (byte) ((innerLen >> 8) & 0xFF);
    }
    buf[total - 1] = 0x00; // leaf NULL primitive
    return buf;
  }
}
