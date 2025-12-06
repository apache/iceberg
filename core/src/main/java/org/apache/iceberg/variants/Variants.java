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
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import org.apache.iceberg.util.DateTimeUtil;

public class Variants {
  private Variants() {}

  public static VariantMetadata emptyMetadata() {
    return SerializedMetadata.EMPTY_V1_METADATA;
  }

  public static VariantMetadata metadata(ByteBuffer metadata) {
    return SerializedMetadata.from(metadata);
  }

  public static VariantMetadata metadata(String... fieldNames) {
    return metadata(Arrays.asList(fieldNames));
  }

  public static VariantMetadata metadata(Collection<String> fieldNames) {
    if (fieldNames.isEmpty()) {
      return emptyMetadata();
    }

    int numElements = fieldNames.size();
    ByteBuffer[] nameBuffers = new ByteBuffer[numElements];
    boolean sorted = true;
    String last = null;
    int pos = 0;
    int dataSize = 0;
    for (String name : fieldNames) {
      nameBuffers[pos] = ByteBuffer.wrap(name.getBytes(StandardCharsets.UTF_8));
      dataSize += nameBuffers[pos].remaining();
      if (last != null && last.compareTo(name) >= 0) {
        sorted = false;
      }

      last = name;
      pos += 1;
    }

    int offsetSize = VariantUtil.sizeOf(dataSize);
    int offsetListOffset = 1 /* header size */ + offsetSize /* dictionary size */;
    int dataOffset = offsetListOffset + ((1 + numElements) * offsetSize);
    int totalSize = dataOffset + dataSize;

    byte header = VariantUtil.metadataHeader(sorted, offsetSize);
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
      int nameSize = VariantUtil.writeBufferAbsolute(buffer, dataOffset + nextOffset, nameBuffer);
      // update the offset and index
      nextOffset += nameSize;
      index += 1;
    }

    // write the final size of the data section
    VariantUtil.writeLittleEndianUnsigned(
        buffer, nextOffset, offsetListOffset + (index * offsetSize), offsetSize);

    return SerializedMetadata.from(buffer);
  }

  public static VariantValue value(VariantMetadata metadata, ByteBuffer value) {
    return VariantValue.from(metadata, value);
  }

  public static ShreddedObject object(VariantMetadata metadata, VariantObject object) {
    return new ShreddedObject(metadata, object);
  }

  public static ShreddedObject object(VariantMetadata metadata) {
    return new ShreddedObject(metadata);
  }

  public static ShreddedObject object(VariantObject object) {
    if (object instanceof ShreddedObject) {
      return new ShreddedObject(((ShreddedObject) object).metadata(), object);
    } else if (object instanceof SerializedObject) {
      return new ShreddedObject(((SerializedObject) object).metadata(), object);
    }

    throw new UnsupportedOperationException("Metadata is required for object: " + object);
  }

  public static boolean isNull(ByteBuffer valueBuffer) {
    return VariantUtil.readByte(valueBuffer, 0) == 0;
  }

  public static ValueArray array() {
    return new ValueArray();
  }

  public static <T> VariantPrimitive<T> of(PhysicalType type, T value) {
    return new PrimitiveWrapper<>(type, value);
  }

  public static VariantPrimitive<Void> ofNull() {
    return new PrimitiveWrapper<>(PhysicalType.NULL, null);
  }

  public static VariantPrimitive<Boolean> of(boolean value) {
    return new PrimitiveWrapper<>(PhysicalType.BOOLEAN_TRUE, value);
  }

  public static VariantPrimitive<Byte> of(byte value) {
    return new PrimitiveWrapper<>(PhysicalType.INT8, value);
  }

  public static VariantPrimitive<Short> of(short value) {
    return new PrimitiveWrapper<>(PhysicalType.INT16, value);
  }

  public static VariantPrimitive<Integer> of(int value) {
    return new PrimitiveWrapper<>(PhysicalType.INT32, value);
  }

  public static VariantPrimitive<Long> of(long value) {
    return new PrimitiveWrapper<>(PhysicalType.INT64, value);
  }

  public static VariantPrimitive<Float> of(float value) {
    return new PrimitiveWrapper<>(PhysicalType.FLOAT, value);
  }

  public static VariantPrimitive<Double> of(double value) {
    return new PrimitiveWrapper<>(PhysicalType.DOUBLE, value);
  }

  public static VariantPrimitive<Integer> ofDate(int value) {
    return new PrimitiveWrapper<>(PhysicalType.DATE, value);
  }

  public static VariantPrimitive<Integer> ofIsoDate(String value) {
    return ofDate(DateTimeUtil.isoDateToDays(value));
  }

  public static VariantPrimitive<Long> ofTimestamptz(long value) {
    return new PrimitiveWrapper<>(PhysicalType.TIMESTAMPTZ, value);
  }

  public static VariantPrimitive<Long> ofIsoTimestamptz(String value) {
    return ofTimestamptz(DateTimeUtil.isoTimestamptzToMicros(value));
  }

  public static VariantPrimitive<Long> ofTimestampntz(long value) {
    return new PrimitiveWrapper<>(PhysicalType.TIMESTAMPNTZ, value);
  }

  public static VariantPrimitive<Long> ofIsoTimestampntz(String value) {
    return ofTimestampntz(DateTimeUtil.isoTimestampToMicros(value));
  }

  public static VariantPrimitive<BigDecimal> of(BigDecimal value) {
    int precision = value.precision();

    if (precision >= 1 && precision <= 9) {
      return new PrimitiveWrapper<>(PhysicalType.DECIMAL4, value);
    } else if (precision >= 10 && precision <= 18) {
      return new PrimitiveWrapper<>(PhysicalType.DECIMAL8, value);
    } else if (precision <= 38) {
      return new PrimitiveWrapper<>(PhysicalType.DECIMAL16, value);
    }

    throw new UnsupportedOperationException("Unsupported decimal precision: " + precision);
  }

  public static VariantPrimitive<ByteBuffer> of(ByteBuffer value) {
    return new PrimitiveWrapper<>(PhysicalType.BINARY, value);
  }

  public static VariantPrimitive<String> of(String value) {
    return new PrimitiveWrapper<>(PhysicalType.STRING, value);
  }

  public static VariantPrimitive<Long> ofTime(long value) {
    return new PrimitiveWrapper<>(PhysicalType.TIME, value);
  }

  public static VariantPrimitive<Long> ofIsoTime(String value) {
    return ofTime(DateTimeUtil.isoTimeToMicros(value));
  }

  public static VariantPrimitive<Long> ofTimestamptzNanos(long value) {
    return new PrimitiveWrapper<>(PhysicalType.TIMESTAMPTZ_NANOS, value);
  }

  public static VariantPrimitive<Long> ofIsoTimestamptzNanos(String value) {
    return ofTimestamptzNanos(DateTimeUtil.isoTimestamptzToNanos(value));
  }

  public static VariantPrimitive<Long> ofTimestampntzNanos(long value) {
    return new PrimitiveWrapper<>(PhysicalType.TIMESTAMPNTZ_NANOS, value);
  }

  public static VariantPrimitive<Long> ofIsoTimestampntzNanos(String value) {
    return ofTimestampntzNanos(DateTimeUtil.isoTimestampToNanos(value));
  }

  public static VariantPrimitive<UUID> ofUUID(UUID uuid) {
    return new PrimitiveWrapper<>(PhysicalType.UUID, uuid);
  }

  public static VariantPrimitive<UUID> ofUUID(String uuid) {
    return ofUUID(UUID.fromString(uuid));
  }
}
