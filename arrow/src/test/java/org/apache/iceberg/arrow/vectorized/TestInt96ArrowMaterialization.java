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

import static org.apache.parquet.schema.Types.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.iceberg.parquet.Int96TestUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestInt96ArrowMaterialization {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void failedMaterializationReleasesAllocatedVector(boolean nanos) {
    try (RootAllocator allocator = new RootAllocator();
        ColumnVector column = column(allocator, nanos, false)) {
      long allocatedBefore = allocator.getAllocatedMemory();

      assertThatThrownBy(column::getArrowVector)
          .isInstanceOf(ArithmeticException.class)
          .hasMessageContaining("overflow");

      assertThat(allocator.getAllocatedMemory()).isEqualTo(allocatedBefore);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void nullSlotsDoNotDecodeInvalidDictionaryValues(boolean nanos) {
    try (RootAllocator allocator = new RootAllocator();
        ColumnVector column = column(allocator, nanos, true)) {
      long allocatedBefore = allocator.getAllocatedMemory();
      try (FieldVector decoded = column.getArrowVector()) {
        assertThat(decoded.getValueCount()).isEqualTo(1);
        assertThat(decoded.isNull(0)).isTrue();
      }

      assertThat(allocator.getAllocatedMemory()).isEqualTo(allocatedBefore);
    }
  }

  private static ColumnVector column(RootAllocator allocator, boolean nanos, boolean nullValue) {
    PrimitiveType primitive = optional(PrimitiveTypeName.INT96).named("ts");
    ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {"ts"}, primitive, 0, 1);
    BigInteger invalid =
        BigInteger.valueOf(Long.MAX_VALUE)
            .add(BigInteger.ONE)
            .multiply(BigInteger.valueOf(nanos ? 1 : 1000));
    Dictionary dictionary =
        new DictionaryPage(
                BytesInput.from(Int96TestUtil.encode(invalid).getBytes()), 1, Encoding.PLAIN)
            .decode(descriptor);
    IntVector ids = new IntVector("ts", allocator);
    ids.allocateNew(1);
    ids.set(0, 0);
    ids.setValueCount(1);
    NullabilityHolder nulls = new NullabilityHolder(1);
    if (nullValue) {
      nulls.setNull(0);
    }

    return new ColumnVector(
        new VectorHolder(
            descriptor,
            ids,
            true,
            dictionary,
            nulls,
            Types.NestedField.optional(
                2,
                "ts",
                nanos ? Types.TimestampNanoType.withZone() : Types.TimestampType.withZone())));
  }
}
