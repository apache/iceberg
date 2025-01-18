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
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.SortedMerge;

/**
 * A variant Object that handles full or partial shredding.
 *
 * <p>Metadata stored for an object must be the same regardless of whether the object is shredded.
 * This class assumes that the metadata from the unshredded object can be used for the shredded
 * fields. This also does not allow updating or replacing the metadata for the unshredded object,
 * which could require recursively rewriting field IDs.
 */
class ShreddedObject implements VariantObject {
  private final SerializedMetadata metadata;
  private final SerializedObject unshredded;
  private final Map<String, VariantValue> shreddedFields = Maps.newHashMap();
  private SerializationState serializationState = null;

  ShreddedObject(SerializedMetadata metadata) {
    this.metadata = metadata;
    this.unshredded = null;
  }

  ShreddedObject(SerializedObject unshredded) {
    this.metadata = unshredded.metadata();
    this.unshredded = unshredded;
  }

  public void put(String field, VariantValue value) {
    Preconditions.checkArgument(
        metadata.id(field) >= 0, "Cannot find field name in metadata: %s", field);

    // allow setting fields that are contained in unshredded. this avoids read-time failures and
    // simplifies replacing field values.
    shreddedFields.put(field, value);
    this.serializationState = null;
  }

  @Override
  public VariantValue get(String field) {
    // the shredded value takes precedence if there is a conflict
    VariantValue value = shreddedFields.get(field);
    if (value != null) {
      return value;
    }

    if (unshredded != null) {
      return unshredded.get(field);
    }

    return null;
  }

  @Override
  public int sizeInBytes() {
    if (null == serializationState) {
      this.serializationState = new SerializationState(metadata, unshredded, shreddedFields);
    }

    return serializationState.size();
  }

  @Override
  public int writeTo(ByteBuffer buffer, int offset) {
    Preconditions.checkArgument(
        buffer.order() == ByteOrder.LITTLE_ENDIAN, "Invalid byte order: big endian");

    if (null == serializationState) {
      this.serializationState = new SerializationState(metadata, unshredded, shreddedFields);
    }

    return serializationState.writeTo(buffer, offset);
  }

  /** Common state for {@link #size()} and {@link #writeTo(ByteBuffer, int)} */
  private static class SerializationState {
    private final SerializedMetadata metadata;
    private final Map<String, ByteBuffer> unshreddedFields;
    private final Map<String, VariantValue> shreddedFields;
    private final int dataSize;
    private final int numElements;
    private final boolean isLarge;
    private final int fieldIdSize;
    private final int offsetSize;

    private SerializationState(
        SerializedMetadata metadata,
        SerializedObject unshredded,
        Map<String, VariantValue> shreddedFields) {
      this.metadata = metadata;
      // field ID size is the size needed to store the largest field ID in the data
      this.fieldIdSize = VariantUtil.sizeOf(metadata.dictionarySize());
      this.shreddedFields = shreddedFields;

      int totalDataSize = 0;
      // get the unshredded field names and values as byte buffers
      ImmutableMap.Builder<String, ByteBuffer> unshreddedBuilder = ImmutableMap.builder();
      if (unshredded != null) {
        for (Pair<String, Integer> field : unshredded.fields()) {
          // if the value is replaced by an unshredded field, don't include it
          String name = field.first();
          boolean replaced = shreddedFields.containsKey(name);
          if (!replaced) {
            ByteBuffer value = unshredded.sliceValue(field.second());
            unshreddedBuilder.put(name, value);
            totalDataSize += value.remaining();
          }
        }
      }

      this.unshreddedFields = unshreddedBuilder.build();
      // duplicates are suppressed when creating unshreddedFields
      this.numElements = unshreddedFields.size() + shreddedFields.size();
      // object is large if the number of elements can't be stored in 1 byte
      this.isLarge = numElements > 0xFF;

      for (VariantValue value : shreddedFields.values()) {
        totalDataSize += value.sizeInBytes();
      }

      this.dataSize = totalDataSize;
      // offset size is the size needed to store the length of the data section
      this.offsetSize = VariantUtil.sizeOf(totalDataSize);
    }

    private int size() {
      return 1 /* header */
          + (isLarge ? 4 : 1) /* num elements size */
          + numElements * fieldIdSize /* field ID list size */
          + (1 + numElements) * offsetSize /* offset list size */
          + dataSize;
    }

    private int writeTo(ByteBuffer buffer, int offset) {
      int fieldIdListOffset =
          offset + 1 /* header size */ + (isLarge ? 4 : 1) /* num elements size */;
      int offsetListOffset = fieldIdListOffset + (numElements * fieldIdSize);
      int dataOffset = offsetListOffset + ((1 + numElements) * offsetSize);
      byte header = VariantUtil.objectHeader(isLarge, fieldIdSize, offsetSize);

      VariantUtil.writeByte(buffer, header, offset);
      VariantUtil.writeLittleEndianUnsigned(buffer, numElements, offset + 1, isLarge ? 4 : 1);

      // neither iterable is closeable, so it is okay to use Iterable
      Iterable<String> fields =
          SortedMerge.of(
              () -> unshreddedFields.keySet().stream().sorted().iterator(),
              () -> shreddedFields.keySet().stream().sorted().iterator());

      int nextValueOffset = 0;
      int index = 0;
      for (String field : fields) {
        // write the field ID from the metadata dictionary
        int id = metadata.id(field);
        Preconditions.checkState(id >= 0, "Invalid metadata, missing: %s", field);
        VariantUtil.writeLittleEndianUnsigned(
            buffer, id, fieldIdListOffset + (index * fieldIdSize), fieldIdSize);
        // write the data offset
        VariantUtil.writeLittleEndianUnsigned(
            buffer, nextValueOffset, offsetListOffset + (index * offsetSize), offsetSize);

        // copy or serialize the value into the data section
        int valueSize;
        VariantValue shreddedValue = shreddedFields.get(field);
        if (shreddedValue != null) {
          valueSize = shreddedValue.writeTo(buffer, dataOffset + nextValueOffset);
        } else {
          valueSize =
              VariantUtil.writeBufferAbsolute(
                  buffer, dataOffset + nextValueOffset, unshreddedFields.get(field));
        }

        // update tracking
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
}
