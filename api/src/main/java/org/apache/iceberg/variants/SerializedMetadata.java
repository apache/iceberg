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
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

class SerializedMetadata implements VariantMetadata, Serialized {
  private static final int HEADER_SIZE = 1;
  private static final int SUPPORTED_VERSION = 1;
  private static final int VERSION_MASK = 0b1111;
  private static final int SORTED_STRINGS = 0b10000;
  private static final int OFFSET_SIZE_MASK = 0b11000000;
  private static final int OFFSET_SIZE_SHIFT = 6;

  static final ByteBuffer EMPTY_V1_BUFFER =
      ByteBuffer.wrap(new byte[] {0x01, 0x00, 0x00}).order(ByteOrder.LITTLE_ENDIAN);
  static final SerializedMetadata EMPTY_V1_METADATA = from(EMPTY_V1_BUFFER);

  static SerializedMetadata from(byte[] bytes) {
    return from(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
  }

  static SerializedMetadata from(ByteBuffer metadata) {
    Preconditions.checkArgument(
        metadata.order() == ByteOrder.LITTLE_ENDIAN, "Unsupported byte order: big endian");
    int header = VariantUtil.readByte(metadata, 0);
    int version = header & VERSION_MASK;
    Preconditions.checkArgument(SUPPORTED_VERSION == version, "Unsupported version: %s", version);
    return new SerializedMetadata(metadata, header);
  }

  private final ByteBuffer metadata;
  private final boolean isSorted;
  private final int offsetSize;
  private final int offsetListOffset;
  private final int dataOffset;
  private final String[] dict;

  private SerializedMetadata(ByteBuffer metadata, int header) {
    this.isSorted = (header & SORTED_STRINGS) == SORTED_STRINGS;
    this.offsetSize = 1 + ((header & OFFSET_SIZE_MASK) >> OFFSET_SIZE_SHIFT);
    int dictSize = VariantUtil.readLittleEndianUnsigned(metadata, HEADER_SIZE, offsetSize);
    this.dict = new String[dictSize];
    this.offsetListOffset = HEADER_SIZE + offsetSize;
    this.dataOffset = offsetListOffset + ((1 + dictSize) * offsetSize);
    int endOffset =
        dataOffset
            + VariantUtil.readLittleEndianUnsigned(
                metadata, offsetListOffset + (offsetSize * dictSize), offsetSize);
    if (endOffset < metadata.limit()) {
      this.metadata = VariantUtil.slice(metadata, 0, endOffset);
    } else {
      this.metadata = metadata;
    }
  }

  @Override
  public int dictionarySize() {
    return dict.length;
  }

  @VisibleForTesting
  boolean isSorted() {
    return isSorted;
  }

  /** Returns the position of the string in the metadata, or -1 if the string is not found. */
  @Override
  public int id(String name) {
    if (name != null) {
      if (isSorted) {
        return VariantUtil.find(dict.length, name, this::get);
      } else {
        for (int id = 0; id < dict.length; id += 1) {
          if (name.equals(get(id))) {
            return id;
          }
        }
      }
    }

    return -1;
  }

  /** Returns the string for the given dictionary id. */
  @Override
  public String get(int index) {
    if (null == dict[index]) {
      int offset =
          VariantUtil.readLittleEndianUnsigned(
              metadata, offsetListOffset + (offsetSize * index), offsetSize);
      int next =
          VariantUtil.readLittleEndianUnsigned(
              metadata, offsetListOffset + (offsetSize * (1 + index)), offsetSize);
      dict[index] = VariantUtil.readString(metadata, dataOffset + offset, next - offset);
    }
    return dict[index];
  }

  @Override
  public ByteBuffer buffer() {
    return metadata;
  }

  @Override
  public int sizeInBytes() {
    return buffer().remaining();
  }

  @Override
  public int writeTo(ByteBuffer buffer, int offset) {
    ByteBuffer value = buffer();
    VariantUtil.writeBufferAbsolute(buffer, offset, value);
    return value.remaining();
  }

  @Override
  public String toString() {
    return VariantMetadata.asString(this);
  }
}
