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
package org.apache.iceberg.flink.sink.shuffle;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.common.ByteArrayUtil;
import org.apache.datasketches.common.Util;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.sampling.ReservoirItemsSketch;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Only way to implement {@link ReservoirItemsSketch} serializer is to extend from {@link
 * ArrayOfItemsSerDe}, as deserialization uses a private constructor from ReservoirItemsSketch. The
 * implementation is modeled after {@link ArrayOfStringsSerDe}
 */
class SortKeySketchSerializer extends ArrayOfItemsSerDe<SortKey> implements Serializable {
  private static final int DEFAULT_SORT_KEY_SIZE = 128;

  private final TypeSerializer<SortKey> itemSerializer;
  private final ListSerializer<SortKey> listSerializer;
  private final DataInputDeserializer input;

  SortKeySketchSerializer(TypeSerializer<SortKey> itemSerializer) {
    this.itemSerializer = itemSerializer;
    this.listSerializer = new ListSerializer<>(itemSerializer);
    this.input = new DataInputDeserializer();
  }

  @Override
  public byte[] serializeToByteArray(SortKey item) {
    try {
      DataOutputSerializer output = new DataOutputSerializer(DEFAULT_SORT_KEY_SIZE);
      itemSerializer.serialize(item, output);
      byte[] itemBytes = output.getSharedBuffer();
      int numBytes = output.length();
      byte[] out = new byte[numBytes + Integer.BYTES];
      ByteArrayUtil.copyBytes(itemBytes, 0, out, 4, numBytes);
      ByteArrayUtil.putIntLE(out, 0, numBytes);
      return out;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize sort key", e);
    }
  }

  @Override
  public byte[] serializeToByteArray(SortKey[] items) {
    try {
      DataOutputSerializer output = new DataOutputSerializer(DEFAULT_SORT_KEY_SIZE * items.length);
      listSerializer.serialize(Arrays.asList(items), output);
      byte[] itemsBytes = output.getSharedBuffer();
      int numBytes = output.length();
      byte[] out = new byte[Integer.BYTES + numBytes];
      ByteArrayUtil.putIntLE(out, 0, numBytes);
      System.arraycopy(itemsBytes, 0, out, Integer.BYTES, numBytes);
      return out;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize sort key", e);
    }
  }

  @Override
  public SortKey[] deserializeFromMemory(Memory mem, long startingOffset, int numItems) {
    Preconditions.checkArgument(mem != null, "Invalid input memory: null");
    if (numItems <= 0) {
      return new SortKey[0];
    }

    long offset = startingOffset;
    Util.checkBounds(offset, Integer.BYTES, mem.getCapacity());
    int numBytes = mem.getInt(offset);
    offset += Integer.BYTES;

    Util.checkBounds(offset, numBytes, mem.getCapacity());
    byte[] sortKeyBytes = new byte[numBytes];
    mem.getByteArray(offset, sortKeyBytes, 0, numBytes);
    input.setBuffer(sortKeyBytes);

    try {
      List<SortKey> sortKeys = listSerializer.deserialize(input);
      SortKey[] array = new SortKey[numItems];
      sortKeys.toArray(array);
      input.releaseArrays();
      return array;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to deserialize sort key sketch", e);
    }
  }

  @Override
  public int sizeOf(SortKey item) {
    return serializeToByteArray(item).length;
  }

  @Override
  public int sizeOf(Memory mem, long offset, int numItems) {
    Preconditions.checkArgument(mem != null, "Invalid input memory: null");
    if (numItems <= 0) {
      return 0;
    }

    Util.checkBounds(offset, Integer.BYTES, mem.getCapacity());
    int numBytes = mem.getInt(offset);
    return Integer.BYTES + numBytes;
  }

  @Override
  public String toString(SortKey item) {
    return item.toString();
  }

  @Override
  public Class<SortKey> getClassOfT() {
    return SortKey.class;
  }
}
