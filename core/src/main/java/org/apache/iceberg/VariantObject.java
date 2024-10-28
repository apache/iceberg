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
import java.nio.ByteOrder;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

class VariantObject implements Variants.Object, Variants.Serialized {
  private static final int OFFSET_SIZE_MASK = 0b1100;
  private static final int OFFSET_SIZE_SHIFT = 2;
  private static final int FIELD_ID_SIZE_MASK = 0b110000;
  private static final int FIELD_ID_SIZE_SHIFT = 4;
  private static final int IS_LARGE = 0b1000000;

  static VariantObject from(VariantMetadata metadata, byte[] bytes) {
    return from(metadata, ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN), bytes[0]);
  }

  static VariantObject from(VariantMetadata metadata, ByteBuffer value, int header) {
    Preconditions.checkArgument(
        value.order() == ByteOrder.LITTLE_ENDIAN, "Unsupported byte order: big endian");
    int basicType = header & Variants.BASIC_TYPE_MASK;
    Preconditions.checkArgument(
        basicType == Variants.BASIC_TYPE_OBJECT, "Invalid object, basic type != 2: " + basicType);
    return new VariantObject(metadata, value, header);
  }

  private final VariantMetadata metadata;
  private final ByteBuffer value;
  private final int fieldIdSize;
  private final int fieldIdListOffset;
  private final int[] fieldIds;
  private final int offsetSize;
  private final int offsetListOffset;
  private final int dataOffset;
  private final Variants.Value[] values;

  private VariantObject(VariantMetadata metadata, ByteBuffer value, int header) {
    this.metadata = metadata;
    this.value = value;
    this.offsetSize = 1 + ((header & OFFSET_SIZE_MASK) >> OFFSET_SIZE_SHIFT);
    this.fieldIdSize = 1 + ((header & FIELD_ID_SIZE_MASK) >> FIELD_ID_SIZE_SHIFT);
    int numElementsSize = ((header & IS_LARGE) == IS_LARGE) ? 4 : 1;
    int numElements =
        VariantUtil.readLittleEndianUnsigned(value, Variants.HEADER_SIZE, numElementsSize);
    this.fieldIdListOffset = Variants.HEADER_SIZE + numElementsSize;
    this.fieldIds = new int[numElements];
    this.offsetListOffset = fieldIdListOffset + (numElements * fieldIdSize);
    this.dataOffset = offsetListOffset + ((1 + numElements) * offsetSize);
    this.values = new Variants.Value[numElements];
  }

  @VisibleForTesting
  int numElements() {
    return fieldIds.length;
  }

  // keys are ordered lexicographically by the name
  @Override
  public Variants.Value get(String name) {
    int index =
        VariantUtil.find(
            fieldIds.length,
            name,
            pos -> {
              int id =
                  VariantUtil.readLittleEndianUnsigned(
                      value, fieldIdListOffset + (pos * fieldIdSize), fieldIdSize);
              return metadata.get(id);
            });

    if (index < 0) {
      return null;
    }

    if (null == values[index]) {
      int offset =
          VariantUtil.readLittleEndianUnsigned(
              value, offsetListOffset + (index * offsetSize), offsetSize);
      int next =
          VariantUtil.readLittleEndianUnsigned(
              value, offsetListOffset + ((1 + index) * offsetSize), offsetSize);
      values[index] = Variants.from(metadata, VariantUtil.slice(value, dataOffset + offset, next - offset));
    }

    return values[index];
  }

  @Override
  public ByteBuffer buffer() {
    return value;
  }
}
