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
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ByteBuffers;

/**
 * A variant Object that handles full or partial shredding.
 *
 * <p>Metadata stored for an object must be the same regardless of whether the object is shredded.
 * This class assumes that the metadata from the unshredded object can be used for the shredded
 * fields. This also does not allow updating or replacing the metadata for the unshredded object,
 * which could require recursively rewriting field IDs.
 */
public class ShreddedObject implements VariantObject {
  private final VariantMetadata metadata;
  private final VariantObject unshredded;
  private final Map<String, VariantValue> shreddedFields = Maps.newHashMap();
  private final Set<String> removedFields = Sets.newHashSet();
  private SerializationState serializationState = null;

  ShreddedObject(VariantMetadata metadata) {
    this(metadata, null);
  }

  ShreddedObject(VariantMetadata metadata, VariantObject unshredded) {
    Preconditions.checkArgument(metadata != null, "Invalid metadata: null");
    this.metadata = metadata;
    this.unshredded = unshredded;
  }

  @VisibleForTesting
  VariantMetadata metadata() {
    return metadata;
  }

  private Set<String> nameSet() {
    Set<String> names = Sets.newTreeSet(shreddedFields.keySet());

    if (unshredded != null) {
      Iterables.addAll(names, unshredded.fieldNames());
    }

    names.removeAll(removedFields);

    return names;
  }

  @Override
  public Iterable<String> fieldNames() {
    return nameSet();
  }

  @Override
  public int numFields() {
    return nameSet().size();
  }

  public void remove(String field) {
    shreddedFields.remove(field);
    removedFields.add(field);
    this.serializationState = null;
  }

  public void put(String field, VariantValue value) {
    Preconditions.checkArgument(
        metadata.id(field) >= 0, "Cannot find field name in metadata: %s", field);

    // allow setting fields that are contained in unshredded. this avoids read-time failures and
    // simplifies replacing field values.
    removedFields.remove(field);
    shreddedFields.put(field, value);
    this.serializationState = null;
  }

  @Override
  public VariantValue get(String field) {
    if (removedFields.contains(field)) {
      return null;
    }

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
      this.serializationState =
          new SerializationState(metadata, unshredded, shreddedFields, removedFields);
    }

    return serializationState.size();
  }

  @Override
  public int writeTo(ByteBuffer buffer, int offset) {
    Preconditions.checkArgument(
        buffer.order() == ByteOrder.LITTLE_ENDIAN, "Invalid byte order: big endian");

    if (null == serializationState) {
      this.serializationState =
          new SerializationState(metadata, unshredded, shreddedFields, removedFields);
    }

    return serializationState.writeTo(buffer, offset);
  }

  /** Common state for {@link #size()} and {@link #writeTo(ByteBuffer, int)} */
  private static class SerializationState {
    private final Entry[] entries;
    private final int dataSize;
    private final int numElements;
    private final boolean isLarge;
    private final int fieldIdSize;
    private final int offsetSize;

    private SerializationState(
        VariantMetadata metadata,
        VariantObject unshredded,
        Map<String, VariantValue> shredded,
        Set<String> removedFields) {
      this.fieldIdSize = VariantUtil.sizeOf(metadata.dictionarySize());

      Map<String, Entry> sorted = Maps.newTreeMap();
      int totalDataSize = 0;

      for (Map.Entry<String, VariantValue> field : shredded.entrySet()) {
        String name = field.getKey();
        int id = metadata.id(name);
        Preconditions.checkState(id >= 0, "Invalid metadata, missing: %s", name);
        VariantValue value = field.getValue();
        int valueSize = value.sizeInBytes();
        sorted.put(name, Entry.ofShredded(id, value, valueSize));
        totalDataSize += valueSize;
      }

      if (unshredded instanceof SerializedObject) {
        // for serialized objects, use existing buffers instead of materializing values
        SerializedObject serialized = (SerializedObject) unshredded;
        for (Map.Entry<String, Integer> field : serialized.fields()) {
          String name = field.getKey();
          if (sorted.containsKey(name) || removedFields.contains(name)) {
            continue;
          }
          int id = metadata.id(name);
          Preconditions.checkState(id >= 0, "Invalid metadata, missing: %s", name);
          ByteBuffer value = serialized.sliceValue(field.getValue());
          int valueSize = value.remaining();
          sorted.put(name, Entry.ofBuffer(id, value, valueSize));
          totalDataSize += valueSize;
        }
      } else if (unshredded != null) {
        for (String name : unshredded.fieldNames()) {
          if (sorted.containsKey(name) || removedFields.contains(name)) {
            continue;
          }
          int id = metadata.id(name);
          Preconditions.checkState(id >= 0, "Invalid metadata, missing: %s", name);
          VariantValue value = unshredded.get(name);
          int valueSize = value.sizeInBytes();
          sorted.put(name, Entry.ofShredded(id, value, valueSize));
          totalDataSize += valueSize;
        }
      }

      this.entries = sorted.values().toArray(new Entry[0]);
      this.numElements = entries.length;
      // object is large if the number of elements can't be stored in 1 byte
      this.isLarge = numElements > 0xFF;
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
      int numElementsSize = isLarge ? 4 : 1;
      int fieldIdListOffset = offset + 1 /* header size */ + numElementsSize;
      int offsetListOffset = fieldIdListOffset + (numElements * fieldIdSize);
      int dataOffset = offsetListOffset + ((1 + numElements) * offsetSize);
      byte header = VariantUtil.objectHeader(isLarge, fieldIdSize, offsetSize);

      ByteBuffers.writeByte(buffer, header, offset);
      ByteBuffers.writeLittleEndianUnsigned(buffer, numElements, offset + 1, numElementsSize);

      int nextValueOffset = 0;
      for (int index = 0; index < numElements; index++) {
        Entry entry = entries[index];
        ByteBuffers.writeLittleEndianUnsigned(
            buffer, entry.id, fieldIdListOffset + (index * fieldIdSize), fieldIdSize);
        ByteBuffers.writeLittleEndianUnsigned(
            buffer, nextValueOffset, offsetListOffset + (index * offsetSize), offsetSize);

        if (entry.shredded != null) {
          int writtenSize = entry.shredded.writeTo(buffer, dataOffset + nextValueOffset);
          Preconditions.checkState(
              writtenSize == entry.size,
              "Wrote %s bytes for field id %s but expected %s (writeTo and sizeInBytes disagree)",
              writtenSize,
              entry.id,
              entry.size);
        } else {
          ByteBuffer src = entry.buffer;
          buffer.put(dataOffset + nextValueOffset, src, src.position(), src.remaining());
        }
        nextValueOffset += entry.size;
      }

      // write the final size of the data section
      ByteBuffers.writeLittleEndianUnsigned(
          buffer, nextValueOffset, offsetListOffset + (numElements * offsetSize), offsetSize);

      return (dataOffset - offset) + dataSize;
    }

    private static final class Entry {
      private final int id;
      private final VariantValue shredded;
      private final ByteBuffer buffer;
      private final int size;

      private Entry(int id, VariantValue shredded, ByteBuffer buffer, int size) {
        this.id = id;
        this.shredded = shredded;
        this.buffer = buffer;
        this.size = size;
      }

      static Entry ofShredded(int id, VariantValue value, int size) {
        return new Entry(id, value, null, size);
      }

      static Entry ofBuffer(int id, ByteBuffer buffer, int size) {
        return new Entry(id, null, buffer, size);
      }
    }
  }

  @Override
  public String toString() {
    return VariantObject.asString(this);
  }
}
