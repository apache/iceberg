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
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

abstract class VariantBuilderBase {
  protected static final int MAX_SHORT_STR_SIZE = 0x3F;

  protected final ByteBufferWrapper buffer;
  protected final Dictionary dict;
  protected int startPos;

  VariantBuilderBase(ByteBufferWrapper buffer, Dictionary dict) {
    this.buffer = buffer;
    this.dict = dict;
    startPos = buffer.pos;
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
    if (numStringBytes > VariantConstants.SIZE_LIMIT) {
      throw new VariantSizeLimitException();
    }

    // Determine the number of bytes required for dictionary size and offset entry
    int offsetSize = sizeOf(Math.max((int) numStringBytes, numKeys));

    // metadata: header byte, dictionary size, offsets and string bytes
    long metadataSize = 1 + offsetSize + (numKeys + 1) * offsetSize + numStringBytes;

    // Ensure the metadata size is within limits
    if (metadataSize > VariantConstants.SIZE_LIMIT) {
      throw new VariantSizeLimitException();
    }

    ByteBufferWrapper metadataBuffer =
        new ByteBufferWrapper((int) metadataSize, (int) metadataSize);

    // Write header byte (version + offset size)
    metadataBuffer.addByte(VariantUtil.metadataHeader(VariantConstants.VERSION, offsetSize));

    // Write number of keys
    metadataBuffer.writeLittleEndianUnsigned(numKeys, offsetSize);

    // Write offsets
    int currentOffset = 0;
    for (byte[] key : dict.getKeys()) {
      metadataBuffer.writeLittleEndianUnsigned(currentOffset, offsetSize);
      currentOffset += key.length;
    }
    metadataBuffer.writeLittleEndianUnsigned(numStringBytes, offsetSize);

    // Write dictionary strings
    dict.getKeys().forEach(metadataBuffer::addBytes);

    return new Variant(buffer.toByteArray(), metadataBuffer.toByteArray());
  }

  protected void writeNullInternal() {
    buffer.addByte(VariantUtil.primitiveHeader(Variants.Primitives.TYPE_NULL));
  }

  protected void writeBooleanInternal(boolean value) {
    buffer.addByte(
        VariantUtil.primitiveHeader(
            value ? Variants.Primitives.TYPE_TRUE : Variants.Primitives.TYPE_FALSE));
  }

  /**
   * Writes a numeric value to the variant builder, automatically choosing the smallest type (INT8,
   * INT16, INT32, or INT64) to store the value efficiently.
   *
   * @param value The numeric value to append.
   */
  protected void writeNumericInternal(long value) {
    if (value == (byte) value) {
      // INT8: Requires 1 byte for header + 1 byte for value
      buffer.addByte(VariantUtil.primitiveHeader(Variants.Primitives.TYPE_INT8));
      buffer.writeLittleEndianUnsigned(value, 1);
    } else if (value == (short) value) {
      // INT16: Requires 1 byte for header + 2 bytes for value
      buffer.addByte(VariantUtil.primitiveHeader(Variants.Primitives.TYPE_INT16));
      buffer.writeLittleEndianUnsigned(value, 2);
    } else if (value == (int) value) {
      // INT32: Requires 1 byte for header + 4 bytes for value
      buffer.addByte(VariantUtil.primitiveHeader(Variants.Primitives.TYPE_INT32));
      buffer.writeLittleEndianUnsigned(value, 4);
    } else {
      // INT64: Requires 1 byte for header + 8 bytes for value
      buffer.addByte(VariantUtil.primitiveHeader(Variants.Primitives.TYPE_INT64));
      buffer.writeLittleEndianUnsigned(value, 8);
    }
  }

  protected void writeDoubleInternal(double value) {
    buffer.addByte(VariantUtil.primitiveHeader(Variants.Primitives.TYPE_DOUBLE));
    buffer.writeLittleEndianUnsigned(Double.doubleToLongBits(value), 8);
  }

  /**
   * Writes a decimal value to the variant builder, choosing the smallest decimal type (DECIMAL4,
   * DECIMAL8, DECIMAL16) that fits its precision and scale.
   */
  public void writeDecimalInternal(BigDecimal value) {
    Preconditions.checkArgument(
        value.precision() <= VariantConstants.MAX_DECIMAL16_PRECISION,
        "Unsupported Decimal precision: %s",
        value.precision());

    BigInteger unscaled = value.unscaledValue();
    if (value.scale() <= VariantConstants.MAX_DECIMAL4_PRECISION
        && value.precision() <= VariantConstants.MAX_DECIMAL4_PRECISION) {
      buffer.addByte(VariantUtil.primitiveHeader(Variants.Primitives.TYPE_DECIMAL4));
      buffer.addByte((byte) value.scale());
      buffer.writeLittleEndianUnsigned(unscaled.intValueExact(), 4);
    } else if (value.scale() <= VariantConstants.MAX_DECIMAL8_PRECISION
        && value.precision() <= VariantConstants.MAX_DECIMAL8_PRECISION) {
      buffer.addByte(VariantUtil.primitiveHeader(Variants.Primitives.TYPE_DECIMAL8));
      buffer.addByte((byte) value.scale());
      buffer.writeLittleEndianUnsigned(unscaled.longValueExact(), 8);
    } else {
      buffer.addByte(VariantUtil.primitiveHeader(Variants.Primitives.TYPE_DECIMAL16));
      buffer.addByte((byte) value.scale());
      byte[] bytes = unscaled.toByteArray();
      for (int i = 0; i < 16; i++) {
        byte byteValue =
            i < bytes.length ? bytes[bytes.length - 1 - i] : (byte) (bytes[0] < 0 ? -1 : 0);
        buffer.addByte(byteValue);
      }
    }
  }

  protected void writeDateInternal(int daysSinceEpoch) {
    buffer.addByte(VariantUtil.primitiveHeader(Variants.Primitives.TYPE_DATE));
    buffer.writeLittleEndianUnsigned(daysSinceEpoch, 4);
  }

  /** Writes a timestamp with timezone (microseconds since epoch) to the variant builder. */
  protected void writeTimestampTzInternal(long microsSinceEpoch) {
    buffer.addByte(VariantUtil.primitiveHeader(Variants.Primitives.TYPE_TIMESTAMPTZ));
    buffer.writeLittleEndianUnsigned(microsSinceEpoch, 8);
  }

  /** Writes a timestamp without timezone (microseconds since epoch) to the variant builder. */
  protected void writeTimestampNtzInternal(long microsSinceEpoch) {
    buffer.addByte(VariantUtil.primitiveHeader(Variants.Primitives.TYPE_TIMESTAMPNTZ));
    buffer.writeLittleEndianUnsigned(microsSinceEpoch, 8);
  }

  protected void writeFloatInternal(float value) throws VariantSizeLimitException {
    buffer.addByte(VariantUtil.primitiveHeader(Variants.Primitives.TYPE_FLOAT));
    buffer.writeLittleEndianUnsigned(Float.floatToIntBits(value), 4);
  }

  protected void writeBinaryInternal(byte[] value) throws VariantSizeLimitException {
    buffer.addByte(VariantUtil.primitiveHeader(Variants.Primitives.TYPE_BINARY));
    buffer.writeLittleEndianUnsigned(value.length, 4);
    buffer.addBytes(value);
  }

  protected void writeStringInternal(String value) {
    byte[] text = value.getBytes(StandardCharsets.UTF_8);
    boolean longStr = text.length > MAX_SHORT_STR_SIZE;

    // Write header
    if (longStr) {
      buffer.addByte(VariantUtil.primitiveHeader(Variants.Primitives.TYPE_STRING));
      buffer.writeLittleEndianUnsigned(text.length, 4);
    } else {
      buffer.addByte(VariantUtil.shortStrHeader(text.length));
    }

    // Write string content
    buffer.addBytes(text);
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
   * @param startPos The starting position of the object data in the buffer.
   * @param fields The list of field entries (key, ID, offset).
   */
  protected void endObject(int startPos, List<FieldEntry> fields) {
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

    int dataSize = buffer.pos - startPos; // Total byte size of the object values
    boolean isLarge = numElements > 0xFF; // Determine whether to use large format
    int sizeBytes = isLarge ? 4 : 1; // Number of bytes for the object size
    int fieldIdSize = sizeOf(maxId); // Number of bytes for each field id
    int fieldOffsetSize = sizeOf(dataSize); // Number of bytes for each field offset
    int headerSize =
        1 + sizeBytes + numElements * fieldIdSize + (numElements + 1) * fieldOffsetSize;

    // Shift existing data to make room for header
    buffer.shift(startPos, headerSize);

    buffer.insertByte(
        VariantUtil.objectHeader(isLarge, fieldIdSize, fieldOffsetSize),
        startPos); // Insert header byte
    buffer.insertLittleEndianUnsigned(
        numElements, sizeBytes, startPos + 1); // Insert number of elements

    // Insert field IDs and offsets
    int fieldIdStart = startPos + 1 + sizeBytes;
    int fieldOffsetStart = fieldIdStart + numElements * fieldIdSize;
    for (int i = 0; i < numElements; i++) {
      buffer.insertLittleEndianUnsigned(
          fields.get(i).id, fieldIdSize, fieldIdStart + i * fieldIdSize);
      buffer.insertLittleEndianUnsigned(
          fields.get(i).offset, fieldOffsetSize, fieldOffsetStart + i * fieldOffsetSize);
    }

    // Insert the offset to the end of the data
    buffer.insertLittleEndianUnsigned(
        dataSize, fieldOffsetSize, fieldOffsetStart + numElements * fieldOffsetSize);
  }

  /**
   * Completes writing an array to the buffer. Array values are already written, and this method
   * inserts header including the header byte, number of elements, and field offsets.
   *
   * @param startPos The starting position of the array values in the buffer.
   * @param offsets The offsets for each array value.
   */
  protected void endArray(int startPos, List<Integer> offsets) {
    int dataSize = buffer.pos - startPos; // Total byte size of the array values
    int numElements = offsets.size();

    boolean isLarge = numElements > 0xFF; // Determine whether to use large format
    int sizeBytes = isLarge ? 4 : 1; // Number of bytes for the array size
    int fieldOffsetSize = sizeOf(dataSize); // Number of bytes of each field offset
    int headerSize = 1 + sizeBytes + (numElements + 1) * fieldOffsetSize; // header size
    int offsetStart = startPos + 1 + sizeBytes; // Start position for offsets

    // Shift existing data to make room for header
    buffer.shift(startPos, headerSize);

    buffer.insertByte(
        VariantUtil.arrayHeader(isLarge, fieldOffsetSize), startPos); // Insert header byte
    buffer.insertLittleEndianUnsigned(
        numElements, sizeBytes, startPos + 1); // Insert number of elements

    // Insert field offsets
    for (int i = 0; i < numElements; i++) {
      buffer.insertLittleEndianUnsigned(
          offsets.get(i), fieldOffsetSize, offsetStart + i * fieldOffsetSize);
    }

    // Insert the offset to the end of the data
    buffer.insertLittleEndianUnsigned(
        dataSize, fieldOffsetSize, offsetStart + numElements * fieldOffsetSize);
  }

  /** An auto-growing byte buffer that doubles its size whenever the capacity is exceeded. */
  protected static class ByteBufferWrapper {
    private static final int INITIAL_CAPACITY = 128; // Starting capacity
    private byte[] buffer;
    int pos = 0;
    private final int sizeLimit;

    ByteBufferWrapper() {
      this(INITIAL_CAPACITY, VariantConstants.SIZE_LIMIT);
    }

    ByteBufferWrapper(int initialCapacity, int sizeLimit) {
      if (initialCapacity <= 0) {
        throw new IllegalArgumentException("Initial capacity must be positive");
      }
      this.buffer = new byte[initialCapacity];
      this.sizeLimit = sizeLimit;
    }

    /**
     * Ensures the buffer has enough capacity to hold additional bytes.
     *
     * @param additional The number of additional bytes required.
     * @throws VariantSizeLimitException If the required capacity exceeds the size limit.
     */
    private void ensureCapacity(int additional) {
      int required = pos + additional;
      if (required > buffer.length) {
        int newCapacity = Integer.highestOneBit(required);
        newCapacity = newCapacity < required ? newCapacity * 2 : newCapacity; // Double the capacity
        if (newCapacity > this.sizeLimit) {
          throw new VariantSizeLimitException();
        }

        byte[] newBuffer = new byte[newCapacity];
        System.arraycopy(buffer, 0, newBuffer, 0, pos);
        buffer = newBuffer;
      }
    }

    /** Adds a byte to the buffer, growing the buffer if necessary. */
    void addByte(byte value) throws VariantSizeLimitException {
      ensureCapacity(1);
      buffer[pos++] = value;
    }

    /** Adds an array of bytes to the buffer, growing the buffer if necessary. */
    void addBytes(byte[] values) throws VariantSizeLimitException {
      ensureCapacity(values.length);
      System.arraycopy(values, 0, buffer, pos, values.length);
      pos += values.length;
    }

    /**
     * Writes a numeric value in little-endian order to the buffer, growing the buffer if necessary.
     *
     * @param value The numeric value to write.
     * @param numBytes The number of bytes to write (e.g., 2 for short, 4 for int, 8 for long).
     */
    void writeLittleEndianUnsigned(long value, int numBytes) {
      if (numBytes < 1 || numBytes > 8) {
        throw new IllegalArgumentException("numBytes must be between 1 and 8");
      }
      ensureCapacity(numBytes);

      for (int i = 0; i < numBytes; ++i) {
        buffer[pos + i] = (byte) ((value >>> (8 * i)) & 0xFF);
      }
      pos += numBytes;
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
        System.arraycopy(buffer, start, buffer, start + offset, pos - start);
      }

      pos += offset;
    }

    /**
     * Insert a byte into the buffer of the provided position. Note: this assumes shift() has been
     * called to leave space for insert.
     */
    void insertByte(byte value, int insertPos) {
      Preconditions.checkArgument(insertPos < pos, "insertPos must be smaller than pos");

      buffer[insertPos] = value;
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

      for (int i = 0; i < numBytes; ++i) {
        buffer[insertPos + i] = (byte) ((value >>> (8 * i)) & 0xFF);
      }
    }

    /** Returns the underlying byte array. */
    byte[] toByteArray() {
      return Arrays.copyOf(buffer, pos);
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

    List<byte[]> getKeys() {
      return utf8Strings;
    }
  }

  /**
   * Temporarily store the information of a field. We need to collect all fields in an JSON object,
   * sort them by their keys, and build the variant object in sorted order.
   */
  protected static final class FieldEntry implements Comparable<FieldEntry> {
    final String key;
    final int id;
    final int offset;

    FieldEntry(String key, int id, int offset) {
      this.key = key;
      this.id = id;
      this.offset = offset;
    }

    FieldEntry withNewOffset(int newOffset) {
      return new FieldEntry(key, id, newOffset);
    }

    @Override
    public int compareTo(FieldEntry other) {
      return key.compareTo(other.key);
    }
  }
}
