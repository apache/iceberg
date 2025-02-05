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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class VariantTestUtil {
  private VariantTestUtil() {}

  public static void assertEqual(VariantMetadata expected, VariantMetadata actual) {
    assertThat(actual).isNotNull();
    assertThat(expected).isNotNull();
    assertThat(actual.dictionarySize())
        .as("Dictionary size should match")
        .isEqualTo(expected.dictionarySize());

    for (int i = 0; i < expected.dictionarySize(); i += 1) {
      assertThat(actual.get(i)).isEqualTo(expected.get(i));
    }
  }

  public static void assertEqual(VariantValue expected, VariantValue actual) {
    assertThat(actual).isNotNull();
    assertThat(expected).isNotNull();
    assertThat(actual.type()).as("Variant type should match").isEqualTo(expected.type());

    if (expected.type() == PhysicalType.OBJECT) {
      VariantObject expectedObject = expected.asObject();
      VariantObject actualObject = actual.asObject();
      assertThat(actualObject.numFields())
          .as("Variant object num fields should match")
          .isEqualTo(expectedObject.numFields());
      for (String fieldName : expectedObject.fieldNames()) {
        assertEqual(expectedObject.get(fieldName), actualObject.get(fieldName));
      }

    } else if (expected.type() == PhysicalType.ARRAY) {
      VariantArray expectedArray = expected.asArray();
      VariantArray actualArray = actual.asArray();
      assertThat(actualArray.numElements())
          .as("Variant array num element should match")
          .isEqualTo(expectedArray.numElements());
      for (int i = 0; i < expectedArray.numElements(); i += 1) {
        assertEqual(expectedArray.get(i), actualArray.get(i));
      }

    } else {
      assertThat(actual.asPrimitive().get())
          .as("Variant primitive value should match")
          .isEqualTo(expected.asPrimitive().get());
    }
  }

  private static byte primitiveHeader(int primitiveType) {
    return (byte) (primitiveType << 2);
  }

  private static byte metadataHeader(boolean isSorted, int offsetSize) {
    return (byte) (((offsetSize - 1) << 6) | (isSorted ? 0b10000 : 0) | 0b0001);
  }

  /** A hacky absolute put for ByteBuffer */
  private static int writeBufferAbsolute(ByteBuffer buffer, int offset, ByteBuffer toCopy) {
    int originalPosition = buffer.position();
    buffer.position(offset);
    ByteBuffer copy = toCopy.duplicate();
    buffer.put(copy); // duplicate so toCopy is not modified
    buffer.position(originalPosition);
    Preconditions.checkArgument(copy.remaining() <= 0, "Not fully written");
    return toCopy.remaining();
  }

  /** Creates a random string primitive of the given length for forcing large offset sizes */
  static SerializedPrimitive createString(String string) {
    byte[] utf8 = string.getBytes(StandardCharsets.UTF_8);
    ByteBuffer buffer = ByteBuffer.allocate(5 + utf8.length).order(ByteOrder.LITTLE_ENDIAN);
    buffer.put(0, primitiveHeader(16));
    buffer.putInt(1, utf8.length);
    writeBufferAbsolute(buffer, 5, ByteBuffer.wrap(utf8));
    return SerializedPrimitive.from(buffer, buffer.get(0));
  }

  public static ByteBuffer emptyMetadata() {
    return createMetadata(ImmutableList.of(), true);
  }

  public static ByteBuffer createMetadata(Collection<String> fieldNames, boolean sortNames) {
    if (fieldNames.isEmpty()) {
      return SerializedMetadata.EMPTY_V1_BUFFER;
    }

    int numElements = fieldNames.size();
    Stream<String> names = sortNames ? fieldNames.stream().sorted() : fieldNames.stream();
    ByteBuffer[] nameBuffers =
        names
            .map(str -> ByteBuffer.wrap(str.getBytes(StandardCharsets.UTF_8)))
            .toArray(ByteBuffer[]::new);

    int dataSize = 0;
    for (ByteBuffer nameBuffer : nameBuffers) {
      dataSize += nameBuffer.remaining();
    }

    int offsetSize = VariantUtil.sizeOf(dataSize);
    int offsetListOffset = 1 /* header size */ + offsetSize /* dictionary size */;
    int dataOffset = offsetListOffset + ((1 + numElements) * offsetSize);
    int totalSize = dataOffset + dataSize;

    byte header = metadataHeader(sortNames, offsetSize);
    ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);

    buffer.put(0, header);
    VariantUtil.writeLittleEndianUnsigned(buffer, numElements, 1, offsetSize);

    // write offsets and strings
    int nextOffset = 0;
    int index = 0;
    for (ByteBuffer nameBuffer : nameBuffers) {
      // write the offset and the string
      VariantUtil.writeLittleEndianUnsigned(
          buffer, nextOffset, offsetListOffset + (index * offsetSize), offsetSize);
      int nameSize = writeBufferAbsolute(buffer, dataOffset + nextOffset, nameBuffer);
      // update the offset and index
      nextOffset += nameSize;
      index += 1;
    }

    // write the final size of the data section
    VariantUtil.writeLittleEndianUnsigned(
        buffer, nextOffset, offsetListOffset + (index * offsetSize), offsetSize);

    return buffer;
  }

  public static ByteBuffer createObject(ByteBuffer metadataBuffer, Map<String, VariantValue> data) {
    // create the metadata to look up field names
    VariantMetadata metadata = Variants.metadata(metadataBuffer);

    int numElements = data.size();
    boolean isLarge = numElements > 0xFF;

    int dataSize = 0;
    for (Map.Entry<String, VariantValue> field : data.entrySet()) {
      dataSize += field.getValue().sizeInBytes();
    }

    // field ID size is the size needed to store the largest field ID in the data
    int fieldIdSize = VariantUtil.sizeOf(metadata.dictionarySize());
    int fieldIdListOffset = 1 /* header size */ + (isLarge ? 4 : 1) /* num elements size */;

    // offset size is the size needed to store the length of the data section
    int offsetSize = VariantUtil.sizeOf(dataSize);
    int offsetListOffset = fieldIdListOffset + (numElements * fieldIdSize);
    int dataOffset = offsetListOffset + ((1 + numElements) * offsetSize);
    int totalSize = dataOffset + dataSize;

    byte header = VariantUtil.objectHeader(isLarge, fieldIdSize, offsetSize);
    ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);

    buffer.put(0, header);
    if (isLarge) {
      buffer.putInt(1, numElements);
    } else {
      buffer.put(1, (byte) (numElements & 0xFF));
    }

    // write field IDs, values, and offsets
    int nextOffset = 0;
    int index = 0;
    List<String> sortedFieldNames = data.keySet().stream().sorted().collect(Collectors.toList());
    for (String fieldName : sortedFieldNames) {
      int id = metadata.id(fieldName);
      VariantUtil.writeLittleEndianUnsigned(
          buffer, id, fieldIdListOffset + (index * fieldIdSize), fieldIdSize);
      VariantUtil.writeLittleEndianUnsigned(
          buffer, nextOffset, offsetListOffset + (index * offsetSize), offsetSize);
      int valueSize = data.get(fieldName).writeTo(buffer, dataOffset + nextOffset);

      // update next offset and index
      nextOffset += valueSize;
      index += 1;
    }

    // write the final size of the data section
    VariantUtil.writeLittleEndianUnsigned(
        buffer, nextOffset, offsetListOffset + (index * offsetSize), offsetSize);

    return buffer;
  }

  static ByteBuffer createArray(Variants.Serialized... values) {
    int numElements = values.length;
    boolean isLarge = numElements > 0xFF;

    int dataSize = 0;
    for (Variants.Serialized value : values) {
      // TODO: produce size for every variant without serializing
      dataSize += value.buffer().remaining();
    }

    // offset size is the size needed to store the length of the data section
    int offsetSize = VariantUtil.sizeOf(dataSize);
    int offsetListOffset = 1 /* header size */ + (isLarge ? 4 : 1) /* num elements size */;
    int dataOffset = offsetListOffset + ((1 + numElements) * offsetSize) /* offset list size */;
    int totalSize = dataOffset + dataSize;

    byte header = VariantUtil.arrayHeader(isLarge, offsetSize);
    ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);

    buffer.put(0, header);
    if (isLarge) {
      buffer.putInt(1, numElements);
    } else {
      buffer.put(1, (byte) (numElements & 0xFF));
    }

    // write values and offsets
    int nextOffset = 0; // the first offset is always 0
    int index = 0;
    for (Variants.Serialized value : values) {
      // write the offset and value
      VariantUtil.writeLittleEndianUnsigned(
          buffer, nextOffset, offsetListOffset + (index * offsetSize), offsetSize);
      // in a real implementation, the buffer should be passed to serialize
      ByteBuffer valueBuffer = value.buffer();
      int valueSize = writeBufferAbsolute(buffer, dataOffset + nextOffset, valueBuffer);
      // update next offset and index
      nextOffset += valueSize;
      index += 1;
    }

    // write the final size of the data section
    VariantUtil.writeLittleEndianUnsigned(
        buffer, nextOffset, offsetListOffset + (index * offsetSize), offsetSize);

    return buffer;
  }
}
