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
package org.apache.iceberg.arrow.vectorized;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UuidVector;
import org.apache.iceberg.arrow.DictEncodedArrowConverter;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;

public class TestDictEncodedArrowConverter {

  /**
   * A dictionary-encoded UUID column is read as an {@link IntVector} of dictionary ids backed by a
   * {@link Dictionary}. The converter must materialize it into the canonical Arrow UUID extension
   * vector, matching the {@link UuidVector} produced by the non-dictionary-encoded read path.
   */
  @Test
  public void testDictionaryEncodedUuidIsDecodedToUuidExtensionVector() {
    UUID first = UUID.fromString("11111111-1111-1111-1111-111111111111");
    UUID second = UUID.fromString("22222222-2222-2222-2222-222222222222");
    byte[][] dictionaryEntries = {UUIDUtil.convert(first), UUIDUtil.convert(second)};

    Dictionary dictionary =
        new Dictionary(Encoding.RLE_DICTIONARY) {
          @Override
          public Binary decodeToBinary(int id) {
            return Binary.fromConstantByteArray(dictionaryEntries[id]);
          }

          @Override
          public int getMaxId() {
            return dictionaryEntries.length - 1;
          }
        };

    try (BufferAllocator allocator = new RootAllocator()) {
      IntVector dictionaryIds = new IntVector("uuid", allocator);
      dictionaryIds.allocateNew(3);
      dictionaryIds.set(0, 1); // -> second
      dictionaryIds.set(1, 0); // -> first
      // index 2 is null; its dictionary id is never decoded
      dictionaryIds.setValueCount(3);

      NullabilityHolder nullabilityHolder = new NullabilityHolder(3);
      nullabilityHolder.setNotNull(0);
      nullabilityHolder.setNotNull(1);
      nullabilityHolder.setNull(2);

      VectorHolder holder =
          new VectorHolder(
              fixedLenByteArrayDescriptor("uuid", 16),
              dictionaryIds,
              true,
              dictionary,
              nullabilityHolder,
              Types.NestedField.optional(1, "uuid", Types.UUIDType.get()));

      // The accessor owns and closes dictionaryIds.
      try (ArrowVectorAccessor<?, String, ?, ?> accessor =
              ArrowVectorAccessors.getVectorAccessor(holder);
          FieldVector converted = DictEncodedArrowConverter.toArrowVector(holder, accessor)) {
        assertThat(converted).isInstanceOf(UuidVector.class);

        UuidVector uuids = (UuidVector) converted;
        assertThat(uuids.getValueCount()).isEqualTo(3);

        // Read values back the way the read path does: from the FixedSizeBinaryVector storage.
        FixedSizeBinaryVector storage = uuids.getUnderlyingVector();
        assertThat(uuids.isNull(0)).isFalse();
        assertThat(UUIDUtil.convert(storage.get(0))).isEqualTo(second);
        assertThat(uuids.isNull(1)).isFalse();
        assertThat(UUIDUtil.convert(storage.get(1))).isEqualTo(first);
        assertThat(uuids.isNull(2)).isTrue();
      }
    }
  }

  /**
   * A dictionary-encoded fixed-length binary column is read as an {@link IntVector} of dictionary
   * ids backed by a {@link Dictionary}. The converter must materialize it into a {@link
   * FixedSizeBinaryVector}, matching the non-dictionary-encoded read path.
   */
  @Test
  public void testDictionaryEncodedFixedIsDecodedToFixedSizeBinaryVector() {
    byte[] first = new byte[] {1, 2, 3, 4};
    byte[] second = new byte[] {5, 6, 7, 8};
    byte[][] dictionaryEntries = {first, second};

    Dictionary dictionary =
        new Dictionary(Encoding.RLE_DICTIONARY) {
          @Override
          public Binary decodeToBinary(int id) {
            return Binary.fromConstantByteArray(dictionaryEntries[id]);
          }

          @Override
          public int getMaxId() {
            return dictionaryEntries.length - 1;
          }
        };

    try (BufferAllocator allocator = new RootAllocator()) {
      IntVector dictionaryIds = new IntVector("fixed", allocator);
      dictionaryIds.allocateNew(3);
      dictionaryIds.set(0, 1); // -> second
      dictionaryIds.set(1, 0); // -> first
      // index 2 is null; its dictionary id is never decoded
      dictionaryIds.setValueCount(3);

      NullabilityHolder nullabilityHolder = new NullabilityHolder(3);
      nullabilityHolder.setNotNull(0);
      nullabilityHolder.setNotNull(1);
      nullabilityHolder.setNull(2);

      VectorHolder holder =
          new VectorHolder(
              fixedLenByteArrayDescriptor("fixed", 4),
              dictionaryIds,
              true,
              dictionary,
              nullabilityHolder,
              Types.NestedField.optional(1, "fixed", Types.FixedType.ofLength(4)));

      // The accessor owns and closes dictionaryIds.
      try (ArrowVectorAccessor<?, String, ?, ?> accessor =
              ArrowVectorAccessors.getVectorAccessor(holder);
          FieldVector converted = DictEncodedArrowConverter.toArrowVector(holder, accessor)) {
        assertThat(converted).isInstanceOf(FixedSizeBinaryVector.class);

        FixedSizeBinaryVector fixed = (FixedSizeBinaryVector) converted;
        assertThat(fixed.getValueCount()).isEqualTo(3);
        assertThat(fixed.isNull(0)).isFalse();
        assertThat(fixed.get(0)).isEqualTo(second);
        assertThat(fixed.isNull(1)).isFalse();
        assertThat(fixed.get(1)).isEqualTo(first);
        assertThat(fixed.isNull(2)).isTrue();
      }
    }
  }

  private static ColumnDescriptor fixedLenByteArrayDescriptor(String name, int length) {
    PrimitiveType type =
        org.apache.parquet.schema.Types.optional(
                PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(length)
            .named(name);
    return new ColumnDescriptor(new String[] {name}, type, 0, 1);
  }
}
