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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

abstract class VariantBuilderBase {
  private static final int MAX_DECIMAL4_PRECISION = 9;
  private static final int MAX_DECIMAL8_PRECISION = 18;
  protected static final int MAX_DECIMAL16_PRECISION = 38;

  private final ByteBufferWrapper valueBuffer;
  private final Dictionary dict;
  private int startPos;

  ByteBufferWrapper valueBuffer() {
    return valueBuffer;
  }

  Dictionary dict() {
    return dict;
  }

  int startPos() {
    return startPos;
  }

  VariantBuilderBase(ByteBufferWrapper valueBuffer, Dictionary dict) {
    this.valueBuffer = valueBuffer;
    this.dict = dict;
    startPos = valueBuffer.pos;
  }

  /**
   * Builds the variant metadata from `dictionaryKeys` and returns the resulting Variant object.
   *
   * @return The constructed Variant object.
   */
  public Variant build() {
    int numKeys = dict.size();

    // Calculate total size of dictionary strings
    long numStringBytes = dict.totalBytes();

    // Determine the number of bytes required for dictionary size and offset entry
    int offsetSize = sizeOf(Math.max((int) numStringBytes, numKeys));

    // metadata: header byte, dictionary size, offsets and string bytes
    long metadataSize = 1 + offsetSize + (numKeys + 1) * offsetSize + numStringBytes;

    ByteBuffer metadataBuffer =
        ByteBuffer.allocate((int) metadataSize).order(ByteOrder.LITTLE_ENDIAN);

    // Write header byte (version + offset size)
    VariantUtil.writeByte(
        metadataBuffer, VariantUtil.metadataHeader(Variants.VERSION, offsetSize), 0);

    // Write number of keys
    VariantUtil.writeLittleEndianUnsigned(metadataBuffer, numKeys, 1, offsetSize);

    // Write offsets
    int offset = 1 + offsetSize;
    int dictOffset = 0;
    for (byte[] key : dict.keys()) {
      VariantUtil.writeLittleEndianUnsigned(metadataBuffer, dictOffset, offset, offsetSize);
      dictOffset += key.length;
      offset += offsetSize;
    }
    VariantUtil.writeLittleEndianUnsigned(metadataBuffer, numStringBytes, offset, offsetSize);

    // Write dictionary strings
    offset += offsetSize;
    for (byte[] key : dict.keys()) {
      VariantUtil.writeBufferAbsolute(metadataBuffer, offset, ByteBuffer.wrap(key));
      offset += key.length;
    }

    return new VariantImpl(metadataBuffer, valueBuffer.buffer);
  }

  protected void writeNullInternal() {
    valueBuffer.writePrimitive(Variants.PhysicalType.NULL, null);
  }

  protected void writeBooleanInternal(boolean value) {
    valueBuffer.writePrimitive(
        value ? Variants.PhysicalType.BOOLEAN_TRUE : Variants.PhysicalType.BOOLEAN_FALSE, value);
  }

  /**
   * Writes an integral value to the variant builder, automatically choosing the smallest type
   * (INT8, INT16, INT32, or INT64) to store the value efficiently.
   *
   * @param value The integral value to append.
   */
  protected void writeIntegralInternal(long value) {
    if (value == (byte) value) {
      valueBuffer.writePrimitive(Variants.PhysicalType.INT8, (byte) value);
    } else if (value == (short) value) {
      valueBuffer.writePrimitive(Variants.PhysicalType.INT16, (short) value);
    } else if (value == (int) value) {
      valueBuffer.writePrimitive(Variants.PhysicalType.INT32, (int) value);
    } else {
      valueBuffer.writePrimitive(Variants.PhysicalType.INT64, value);
    }
  }

  protected void writeDoubleInternal(double value) {
    valueBuffer.writePrimitive(Variants.PhysicalType.DOUBLE, value);
  }

  /**
   * Writes a decimal value to the variant builder, choosing the smallest decimal type (DECIMAL4,
   * DECIMAL8, DECIMAL16) that fits its precision and scale.
   */
  public void writeDecimalInternal(BigDecimal value) {
    Preconditions.checkArgument(
        value.precision() <= MAX_DECIMAL16_PRECISION,
        "Unsupported Decimal precision: %s",
        value.precision());

    if (value.scale() <= MAX_DECIMAL4_PRECISION && value.precision() <= MAX_DECIMAL4_PRECISION) {
      valueBuffer.writePrimitive(Variants.PhysicalType.DECIMAL4, value);
    } else if (value.scale() <= MAX_DECIMAL8_PRECISION
        && value.precision() <= MAX_DECIMAL8_PRECISION) {
      valueBuffer.writePrimitive(Variants.PhysicalType.DECIMAL8, value);
    } else {
      valueBuffer.writePrimitive(Variants.PhysicalType.DECIMAL16, value);
    }
  }

  protected void writeDateInternal(int daysSinceEpoch) {
    valueBuffer.writePrimitive(Variants.PhysicalType.DATE, daysSinceEpoch);
  }

  /** Writes a timestamp with timezone (microseconds since epoch) to the variant builder. */
  protected void writeTimestampTzInternal(long microsSinceEpoch) {
    valueBuffer.writePrimitive(Variants.PhysicalType.TIMESTAMPTZ, microsSinceEpoch);
  }

  /** Writes a timestamp without timezone (microseconds since epoch) to the variant builder. */
  protected void writeTimestampNtzInternal(long microsSinceEpoch) {
    valueBuffer.writePrimitive(Variants.PhysicalType.TIMESTAMPNTZ, microsSinceEpoch);
  }

  protected void writeFloatInternal(float value) {
    valueBuffer.writePrimitive(Variants.PhysicalType.FLOAT, value);
  }

  protected void writeBinaryInternal(byte[] value) {
    valueBuffer.writePrimitive(Variants.PhysicalType.BINARY, ByteBuffer.wrap(value));
  }

  protected void writeStringInternal(String value) {
    valueBuffer.writePrimitive(Variants.PhysicalType.STRING, value);
  }

  /** Choose the smallest number of bytes to store the given value. */
  protected static int sizeOf(int maxValue) {
    if (maxValue <= 0xFF) {
      return 1;
    } else if (maxValue <= 0xFFFF) {
      return 2;
    } else if (maxValue <= 0xFFFFFF) {
      return 3;
    }

    return 4;
  }

  /**
   * Completes writing an object to the buffer. Object fields are already written, and this method
   * inserts header including header byte, number of elements, field IDs, and field offsets.
   *
   * @param objStartPos The starting position of the object data in the buffer.
   * @param fields The list of field entries (key, ID, offset).
   */
  protected void endObject(int objStartPos, List<FieldEntry> fields) {
    int numElements = fields.size();

    // Sort fields by key and ensure no duplicate keys
    Collections.sort(fields);
    int maxId = numElements == 0 ? 0 : fields.get(0).id;
    for (int i = 1; i < numElements; i++) {
      maxId = Math.max(maxId, fields.get(i).id);
      if (fields.get(i).key.equals(fields.get(i - 1).key)) {
        throw new IllegalStateException("Duplicate key in Variant: " + fields.get(i).key);
      }
    }

    int dataSize = valueBuffer.pos - objStartPos; // Total byte size of the object values
    boolean isLarge = numElements > 0xFF; // Determine whether to use large format
    int sizeBytes = isLarge ? 4 : 1; // Number of bytes for the object size
    int fieldIdSize = sizeOf(maxId); // Number of bytes for each field id
    int fieldOffsetSize = sizeOf(dataSize); // Number of bytes for each field offset
    int headerSize =
        1 + sizeBytes + numElements * fieldIdSize + (numElements + 1) * fieldOffsetSize;

    // Shift existing data to make room for header
    valueBuffer.shift(objStartPos, headerSize);

    valueBuffer.insertByte(
        VariantUtil.objectHeader(isLarge, fieldIdSize, fieldOffsetSize),
        objStartPos); // Insert header byte
    valueBuffer.insertLittleEndianUnsigned(
        numElements, sizeBytes, objStartPos + 1); // Insert number of elements

    // Insert field IDs and offsets
    int fieldIdStart = objStartPos + 1 + sizeBytes;
    int fieldOffsetStart = fieldIdStart + numElements * fieldIdSize;
    for (int i = 0; i < numElements; i++) {
      valueBuffer.insertLittleEndianUnsigned(
          fields.get(i).id, fieldIdSize, fieldIdStart + i * fieldIdSize);
      valueBuffer.insertLittleEndianUnsigned(
          fields.get(i).offset, fieldOffsetSize, fieldOffsetStart + i * fieldOffsetSize);
    }

    // Insert the offset to the end of the data
    valueBuffer.insertLittleEndianUnsigned(
        dataSize, fieldOffsetSize, fieldOffsetStart + numElements * fieldOffsetSize);
  }

  /**
   * Completes writing an array to the buffer. Array values are already written, and this method
   * inserts header including the header byte, number of elements, and field offsets.
   *
   * @param arrStartPos The starting position of the array values in the buffer.
   * @param offsets The offsets for each array value.
   */
  protected void endArray(int arrStartPos, List<Integer> offsets) {
    int dataSize = valueBuffer.pos - arrStartPos; // Total byte size of the array values
    int numElements = offsets.size();

    boolean isLarge = numElements > 0xFF; // Determine whether to use large format
    int sizeBytes = isLarge ? 4 : 1; // Number of bytes for the array size
    int fieldOffsetSize = sizeOf(dataSize); // Number of bytes of each field offset
    int headerSize = 1 + sizeBytes + (numElements + 1) * fieldOffsetSize; // header size
    int offsetStart = arrStartPos + 1 + sizeBytes; // Start position for offsets

    // Shift existing data to make room for header
    valueBuffer.shift(arrStartPos, headerSize);

    valueBuffer.insertByte(
        VariantUtil.arrayHeader(isLarge, fieldOffsetSize), arrStartPos); // Insert header byte
    valueBuffer.insertLittleEndianUnsigned(
        numElements, sizeBytes, arrStartPos + 1); // Insert number of elements

    // Insert field offsets
    for (int i = 0; i < numElements; i++) {
      valueBuffer.insertLittleEndianUnsigned(
          offsets.get(i), fieldOffsetSize, offsetStart + i * fieldOffsetSize);
    }

    // Insert the offset to the end of the data
    valueBuffer.insertLittleEndianUnsigned(
        dataSize, fieldOffsetSize, offsetStart + numElements * fieldOffsetSize);
  }

  /** An auto-growing byte buffer that doubles its size whenever the capacity is exceeded. */
  protected static class ByteBufferWrapper {
    private static final int INITIAL_CAPACITY = 128; // Starting capacity
    private ByteBuffer buffer;
    private int pos = 0;

    ByteBufferWrapper() {
      this(INITIAL_CAPACITY);
    }

    ByteBufferWrapper(int initialCapacity) {
      if (initialCapacity <= 0) {
        throw new IllegalArgumentException("Initial capacity must be positive");
      }
      this.buffer = ByteBuffer.allocate(initialCapacity).order(ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Ensures the buffer has enough capacity to hold additional bytes.
     *
     * @param additional The number of additional bytes required.
     */
    private void ensureCapacity(int additional) {
      int required = pos + additional;
      if (required > buffer.capacity()) {
        int newCapacity = Integer.highestOneBit(required);
        newCapacity = newCapacity < required ? newCapacity * 2 : newCapacity; // Double the capacity

        ByteBuffer newBuffer =
            ByteBuffer.allocate(newCapacity)
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(buffer.array(), 0, pos);
        buffer = newBuffer;
      }
    }

    <T> void writePrimitive(Variants.PhysicalType type, T value) {
      PrimitiveWrapper<T> wrapper = new PrimitiveWrapper<T>(type, value);
      ensureCapacity(pos + wrapper.sizeInBytes());
      wrapper.writeTo(buffer, pos);
      pos += wrapper.sizeInBytes();
    }

    /**
     * Move the bytes of buffer range [start, pos) by the provided offset position. This is used for
     * writing array/object header.
     */
    void shift(int start, int offset) {
      Preconditions.checkArgument(offset > 0, "offset must be positive");
      Preconditions.checkArgument(pos >= start, "start must be no greater than pos");
      ensureCapacity(offset);

      if (pos > start) {
        System.arraycopy(buffer.array(), start, buffer.array(), start + offset, pos - start);
      }

      pos += offset;
    }

    /**
     * Insert a byte into the buffer of the provided position. Note: this assumes shift() has been
     * called to leave space for insert.
     */
    void insertByte(byte value, int insertPos) {
      Preconditions.checkArgument(insertPos < pos, "insertPos must be smaller than pos");
      VariantUtil.writeByteAbsolute(buffer, value, insertPos);
    }

    /**
     * Insert a number into the buffer of the provided position. Note: this assumes shift() has been
     * called to leave space for insert.
     */
    void insertLittleEndianUnsigned(long value, int numBytes, int insertPos) {
      Preconditions.checkArgument(insertPos < pos, "insertPos must be smaller than pos");
      if (numBytes < 1 || numBytes > 8) {
        throw new IllegalArgumentException("numBytes must be between 1 and 8");
      }

      VariantUtil.writeLittleEndianUnsignedAbsolute(buffer, value, insertPos, numBytes);
    }

    int pos() {
      return pos;
    }
  }

  /**
   * A Variant metadata dictionary implementation which assigns a monotonically increasing assigned
   * id to newly added string
   */
  protected static class Dictionary {
    // Store the mapping from a string to a monotonically increasing assigned id
    private final Map<String, Integer> stringIds = Maps.newHashMap();
    // Store all the strings encoded with UTF8 in `dictionary` in the order of assigned ids.
    private final List<byte[]> utf8Strings = Lists.newArrayList();

    /** Return the assigned id if string exists; otherwise, assign the next id and return. */
    int add(String key) {
      return stringIds.computeIfAbsent(
          key,
          k -> {
            int newId = stringIds.size();
            utf8Strings.add(k.getBytes(StandardCharsets.UTF_8));
            return newId;
          });
    }

    int size() {
      return utf8Strings.size();
    }

    long totalBytes() {
      return utf8Strings.stream().mapToLong(key -> key.length).sum();
    }

    List<byte[]> keys() {
      return utf8Strings;
    }
  }

  /**
   * Temporarily store the information of a field. We need to collect all fields in an JSON object,
   * sort them by their keys, and build the variant object in sorted order.
   */
  protected static final class FieldEntry implements Comparable<FieldEntry> {
    private final String key;
    private final int id;
    private final int offset;

    FieldEntry(String key, int id, int offset) {
      this.key = key;
      this.id = id;
      this.offset = offset;
    }

    @Override
    public int compareTo(FieldEntry other) {
      return key.compareTo(other.key);
    }
  }
}
