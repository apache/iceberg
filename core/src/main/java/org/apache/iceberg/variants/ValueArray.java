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
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class ValueArray implements VariantArray {
  private SerializationState serializationState = null;
  private List<VariantValue> elements = Lists.newArrayList();

  ValueArray() {}

  @Override
  public VariantValue get(int index) {
    return elements.get(index);
  }

  @Override
  public int numElements() {
    return elements.size();
  }

  public void add(VariantValue value) {
    elements.add(value);
    this.serializationState = null;
  }

  @Override
  public int sizeInBytes() {
    if (null == serializationState) {
      this.serializationState = new SerializationState(elements);
    }

    return serializationState.size();
  }

  @Override
  public int writeTo(ByteBuffer buffer, int offset) {
    Preconditions.checkArgument(
        buffer.order() == ByteOrder.LITTLE_ENDIAN, "Invalid byte order: big endian");

    if (null == serializationState) {
      this.serializationState = new SerializationState(elements);
    }

    return serializationState.writeTo(buffer, offset);
  }

  /** Common state for {@link #size()} and {@link #writeTo(ByteBuffer, int)} */
  private static class SerializationState {
    private final List<VariantValue> elements;
    private final int numElements;
    private final boolean isLarge;
    private final int dataSize;
    private final int offsetSize;

    private SerializationState(List<VariantValue> elements) {
      this.elements = elements;
      this.numElements = elements.size();
      this.isLarge = numElements > 0xFF;

      int totalDataSize = 0;
      for (VariantValue value : elements) {
        totalDataSize += value.sizeInBytes();
      }

      this.dataSize = totalDataSize;
      this.offsetSize = VariantUtil.sizeOf(totalDataSize);
    }

    private int size() {
      return 1 /* header */
          + (isLarge ? 4 : 1) /* num elements size */
          + (1 + numElements) * offsetSize /* offset list size */
          + dataSize;
    }

    private int writeTo(ByteBuffer buffer, int offset) {
      int offsetListOffset =
          offset + 1 /* header size */ + (isLarge ? 4 : 1) /* num elements size */;
      int dataOffset = offsetListOffset + ((1 + numElements) * offsetSize);
      byte header = VariantUtil.arrayHeader(isLarge, offsetSize);

      VariantUtil.writeByte(buffer, header, offset);
      VariantUtil.writeLittleEndianUnsigned(buffer, numElements, offset + 1, isLarge ? 4 : 1);

      // Insert element offsets
      int nextValueOffset = 0;
      int index = 0;
      for (VariantValue element : elements) {
        // write the data offset
        VariantUtil.writeLittleEndianUnsigned(
            buffer, nextValueOffset, offsetListOffset + (index * offsetSize), offsetSize);

        // write the data
        int valueSize = element.writeTo(buffer, dataOffset + nextValueOffset);

        nextValueOffset += valueSize;
        index += 1;
      }

      // write the final size of the data section
      VariantUtil.writeLittleEndianUnsigned(
          buffer, nextValueOffset, offsetListOffset + (index * offsetSize), offsetSize);

      // return the total size
      return (dataOffset - offset) + dataSize;
    }
  }

  @Override
  public String toString() {
    return VariantArray.asString(this);
  }
}
