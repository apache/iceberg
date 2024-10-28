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

class VariantArray implements Variants.Array, Variants.Serialized {
  private static final int OFFSET_SIZE_MASK = 0b1100;
  private static final int OFFSET_SIZE_SHIFT = 2;
  private static final int IS_LARGE = 0b10000;

  @VisibleForTesting
  static VariantArray from(VariantMetadata metadata, byte[] bytes) {
    return from(metadata, ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN), bytes[0]);
  }

  static VariantArray from(VariantMetadata metadata, ByteBuffer value, int header) {
    Preconditions.checkArgument(
        value.order() == ByteOrder.LITTLE_ENDIAN, "Unsupported byte order: big endian");
    int basicType = header & Variants.BASIC_TYPE_MASK;
    Preconditions.checkArgument(
        basicType == Variants.BASIC_TYPE_ARRAY, "Invalid array, basic type != 3: " + basicType);
    return new VariantArray(metadata, value, header);
  }

  private final VariantMetadata metadata;
  private final ByteBuffer value;
  private final int offsetSize;
  private final int offsetListOffset;
  private final int dataOffset;
  private final Variants.Value[] array;

  private VariantArray(VariantMetadata metadata, ByteBuffer value, int header) {
    this.metadata = metadata;
    this.value = value;
    this.offsetSize = 1 + ((header & OFFSET_SIZE_MASK) >> OFFSET_SIZE_SHIFT);
    int numElementsSize = ((header & IS_LARGE) == IS_LARGE) ? 4 : 1;
    int numElements =
        VariantUtil.readLittleEndianUnsigned(value, Variants.HEADER_SIZE, numElementsSize);
    this.offsetListOffset = Variants.HEADER_SIZE + numElementsSize;
    this.dataOffset = offsetListOffset + ((1 + numElements) * offsetSize);
    this.array = new Variants.Value[numElements];
  }

  @VisibleForTesting
  int numElements() {
    return array.length;
  }

  @Override
  public Variants.Value get(int index) {
    if (null == array[index]) {
      int offset =
          VariantUtil.readLittleEndianUnsigned(
              value, offsetListOffset + (offsetSize * index), offsetSize);
      int next =
          VariantUtil.readLittleEndianUnsigned(
              value, offsetListOffset + (offsetSize * (1 + index)), offsetSize);
      array[index] =
          Variants.from(metadata, VariantUtil.slice(value, dataOffset + offset, next - offset));
    }
    return array[index];
  }

  @Override
  public ByteBuffer buffer() {
    return value;
  }
}
