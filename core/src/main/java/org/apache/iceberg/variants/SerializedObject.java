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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;

class SerializedObject extends Variants.SerializedValue implements VariantObject {
  private static final int OFFSET_SIZE_MASK = 0b1100;
  private static final int OFFSET_SIZE_SHIFT = 2;
  private static final int FIELD_ID_SIZE_MASK = 0b110000;
  private static final int FIELD_ID_SIZE_SHIFT = 4;
  private static final int IS_LARGE = 0b1000000;

  static SerializedObject from(SerializedMetadata metadata, byte[] bytes) {
    return from(metadata, ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN), bytes[0]);
  }

  static SerializedObject from(SerializedMetadata metadata, ByteBuffer value, int header) {
    Preconditions.checkArgument(
        value.order() == ByteOrder.LITTLE_ENDIAN, "Unsupported byte order: big endian");
    Variants.BasicType basicType = VariantUtil.basicType(header);
    Preconditions.checkArgument(
        basicType == Variants.BasicType.OBJECT, "Invalid object, basic type: " + basicType);
    return new SerializedObject(metadata, value, header);
  }

  private final SerializedMetadata metadata;
  private final ByteBuffer value;
  private final int fieldIdSize;
  private final int fieldIdListOffset;
  private final Integer[] fieldIds;
  private final int offsetSize;
  private final int offsetListOffset;
  private final int[] offsets;
  private final int[] lengths;
  private final int dataOffset;
  private final VariantValue[] values;

  private SerializedObject(SerializedMetadata metadata, ByteBuffer value, int header) {
    this.metadata = metadata;
    this.value = value;
    this.offsetSize = 1 + ((header & OFFSET_SIZE_MASK) >> OFFSET_SIZE_SHIFT);
    this.fieldIdSize = 1 + ((header & FIELD_ID_SIZE_MASK) >> FIELD_ID_SIZE_SHIFT);
    int numElementsSize = ((header & IS_LARGE) == IS_LARGE) ? 4 : 1;
    int numElements =
        VariantUtil.readLittleEndianUnsigned(value, Variants.HEADER_SIZE, numElementsSize);
    this.fieldIdListOffset = Variants.HEADER_SIZE + numElementsSize;
    this.fieldIds = new Integer[numElements];
    this.offsetListOffset = fieldIdListOffset + (numElements * fieldIdSize);
    this.offsets = new int[numElements];
    this.lengths = new int[numElements];
    this.dataOffset = offsetListOffset + ((1 + numElements) * offsetSize);
    this.values = new VariantValue[numElements];

    if (numElements > 0) {
      initOffsetsAndLengths(numElements);
    }
  }

  private void initOffsetsAndLengths(int numElements) {
    // populate offsets list
    Map<Integer, Integer> offsetToLength = Maps.newHashMap();
    for (int index = 0; index < numElements; index += 1) {
      offsets[index] =
          VariantUtil.readLittleEndianUnsigned(
              value, offsetListOffset + (index * offsetSize), offsetSize);

      offsetToLength.put(offsets[index], 0);
    }

    int dataLength =
        VariantUtil.readLittleEndianUnsigned(
            value, offsetListOffset + (numElements * offsetSize), offsetSize);
    offsetToLength.put(dataLength, 0);

    // populate lengths list by sorting offsets
    List<Integer> sortedOffsets =
        offsetToLength.keySet().stream().sorted().collect(Collectors.toList());
    for (int index = 0; index < numElements; index += 1) {
      int offset = sortedOffsets.get(index);
      int length = sortedOffsets.get(index + 1) - offset;
      offsetToLength.put(offset, length);
    }

    for (int index = 0; index < lengths.length; index += 1) {
      lengths[index] = offsetToLength.get(offsets[index]);
    }
  }

  @VisibleForTesting
  int numElements() {
    return fieldIds.length;
  }

  SerializedMetadata metadata() {
    return metadata;
  }

  Iterable<Pair<String, Integer>> fields() {
    return () ->
        new Iterator<>() {
          private int index = 0;

          @Override
          public boolean hasNext() {
            return index < fieldIds.length;
          }

          @Override
          public Pair<String, Integer> next() {
            Pair<String, Integer> next = Pair.of(metadata.get(id(index)), index);
            index += 1;
            return next;
          }
        };
  }

  public Iterable<String> fieldNames() {
    return () ->
        new Iterator<>() {
          private int index = 0;

          @Override
          public boolean hasNext() {
            return index < fieldIds.length;
          }

          @Override
          public String next() {
            int id = id(index);
            index += 1;
            return metadata.get(id);
          }
        };
  }

  private int id(int index) {
    if (null == fieldIds[index]) {
      fieldIds[index] =
          VariantUtil.readLittleEndianUnsigned(
              value, fieldIdListOffset + (index * fieldIdSize), fieldIdSize);
    }

    return fieldIds[index];
  }

  @Override
  public VariantValue get(String name) {
    // keys are ordered lexicographically by the name
    int index = VariantUtil.find(fieldIds.length, name, pos -> metadata.get(id(pos)));

    if (index < 0) {
      return null;
    }

    if (null == values[index]) {
      values[index] =
          Variants.from(
              metadata, VariantUtil.slice(value, dataOffset + offsets[index], lengths[index]));
    }

    return values[index];
  }

  /**
   * Retrieve a field value as a ByteBuffer.
   *
   * @param name field name
   * @return the field value as a ByteBuffer
   */
  ByteBuffer sliceValue(String name) {
    int index = VariantUtil.find(fieldIds.length, name, pos -> metadata.get(id(pos)));

    if (index < 0) {
      return null;
    }

    return sliceValue(index);
  }

  /**
   * Retrieve a field value as a ByteBuffer.
   *
   * @param index field index within the object
   * @return the field value as a ByteBuffer
   */
  ByteBuffer sliceValue(int index) {
    if (values[index] != null) {
      return ((Variants.Serialized) values[index]).buffer();
    }

    return VariantUtil.slice(value, dataOffset + offsets[index], lengths[index]);
  }

  @Override
  public ByteBuffer buffer() {
    return value;
  }

  @Override
  public int sizeInBytes() {
    return value.remaining();
  }
}
