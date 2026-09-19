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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.UUIDUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests that a column read as a constant produces a vector holding that value in every row. Values
 * are read back through {@link ColumnVector} because that is how the batch reader hands them out.
 */
public class TestConstantVectors {

  private static final int NUM_ROWS = 3;

  private BufferAllocator allocator;

  @BeforeEach
  public void before() {
    this.allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void after() {
    allocator.close();
  }

  @Test
  public void testNullConstantIsNullInEveryRow() {
    try (ColumnVector column = constantColumn(Types.IntegerType.get(), null)) {
      assertThat(column.hasNull()).isTrue();
      assertThat(column.numNulls()).isEqualTo(NUM_ROWS);
      for (int row = 0; row < NUM_ROWS; row += 1) {
        assertThat(column.isNullAt(row)).isTrue();
      }
    }
  }

  @Test
  public void testNullConstantForEveryPrimitiveType() {
    // a column added by a schema change has no default, so it is read as a null of its own type
    for (Type type : primitiveTypes()) {
      try (ColumnVector column = constantColumn(type, null)) {
        assertThat(column.isNullAt(0)).as("null constant of %s", type).isTrue();
        assertThat(column.getFieldVector().getValueCount()).isEqualTo(NUM_ROWS);
      }
    }
  }

  @Test
  public void testNumericConstants() {
    try (ColumnVector booleans = constantColumn(Types.BooleanType.get(), true);
        ColumnVector ints = constantColumn(Types.IntegerType.get(), 42);
        ColumnVector longs = constantColumn(Types.LongType.get(), 42L);
        ColumnVector floats = constantColumn(Types.FloatType.get(), 1.5f);
        ColumnVector doubles = constantColumn(Types.DoubleType.get(), 2.5d)) {
      for (int row = 0; row < NUM_ROWS; row += 1) {
        assertThat(booleans.getBoolean(row)).isTrue();
        assertThat(ints.getInt(row)).isEqualTo(42);
        assertThat(longs.getLong(row)).isEqualTo(42L);
        assertThat(floats.getFloat(row)).isEqualTo(1.5f);
        assertThat(doubles.getDouble(row)).isEqualTo(2.5d);
        assertThat(ints.isNullAt(row)).isFalse();
      }
    }
  }

  @Test
  public void testTemporalConstants() {
    // dates are days from the epoch, times and timestamps are microseconds, nano timestamps are
    // nanoseconds
    try (ColumnVector dates = constantColumn(Types.DateType.get(), 19_000);
        ColumnVector times = constantColumn(Types.TimeType.get(), 3_600_000_000L);
        ColumnVector timestamptz = constantColumn(Types.TimestampType.withZone(), 1_600_000_000L);
        ColumnVector timestamp = constantColumn(Types.TimestampType.withoutZone(), 1_600_000_000L);
        ColumnVector nanotz =
            constantColumn(Types.TimestampNanoType.withZone(), 1_600_000_000_000L);
        ColumnVector nanos =
            constantColumn(Types.TimestampNanoType.withoutZone(), 1_600_000_000_000L)) {
      for (int row = 0; row < NUM_ROWS; row += 1) {
        assertThat(dates.getInt(row)).isEqualTo(19_000);
        assertThat(times.getLong(row)).isEqualTo(3_600_000_000L);
        assertThat(timestamptz.getLong(row)).isEqualTo(1_600_000_000L);
        assertThat(timestamp.getLong(row)).isEqualTo(1_600_000_000L);
        assertThat(nanotz.getLong(row)).isEqualTo(1_600_000_000_000L);
        assertThat(nanos.getLong(row)).isEqualTo(1_600_000_000_000L);
      }
    }
  }

  @Test
  public void testStringAndBinaryConstants() {
    UUID uuid = UUID.fromString("9b9e2b46-9b53-4bcb-a1a5-4f0b0a15d5c1");
    byte[] fixed = new byte[] {1, 2, 3, 4};
    byte[] binary = new byte[] {5, 6, 7};

    try (ColumnVector strings = constantColumn(Types.StringType.get(), "iceberg");
        ColumnVector uuids = constantColumn(Types.UUIDType.get(), uuid);
        ColumnVector fixedValues = constantColumn(Types.FixedType.ofLength(4), fixed);
        ColumnVector binaryValues =
            constantColumn(Types.BinaryType.get(), ByteBuffer.wrap(binary))) {
      for (int row = 0; row < NUM_ROWS; row += 1) {
        assertThat(strings.getString(row)).isEqualTo("iceberg");
        assertThat(uuids.getBinary(row)).isEqualTo(UUIDUtil.convert(uuid));
        assertThat(fixedValues.getBinary(row)).isEqualTo(fixed);
        assertThat(binaryValues.getBinary(row)).isEqualTo(binary);
      }
    }
  }

  @Test
  public void testDecimalConstant() {
    BigDecimal decimal = new BigDecimal("123.45");
    try (ColumnVector decimals = constantColumn(Types.DecimalType.of(9, 2), decimal)) {
      for (int row = 0; row < NUM_ROWS; row += 1) {
        assertThat(decimals.getDecimal(row, 9, 2)).isEqualTo(decimal);
      }
    }
  }

  @Test
  public void testStringConstantAcceptsAnyCharSequence() {
    CharSequence value = new StringBuilder("iceberg");
    try (ColumnVector strings = constantColumn(Types.StringType.get(), value)) {
      assertThat(strings.getString(0)).isEqualTo("iceberg");
    }
  }

  @Test
  public void testUnsupportedConstantType() {
    Types.NestedField field =
        Types.NestedField.optional(
            1,
            "c",
            Types.StructType.of(Types.NestedField.optional(2, "n", Types.IntegerType.get())));

    assertThatThrownBy(() -> ConstantVectors.holder(field, "value", NUM_ROWS, allocator))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageStartingWith("Unsupported constant type:");
  }

  private ColumnVector constantColumn(Type type, Object constant) {
    Types.NestedField field = Types.NestedField.optional(1, "c", type);
    return new ColumnVector(ConstantVectors.holder(field, constant, NUM_ROWS, allocator));
  }

  private static Type[] primitiveTypes() {
    return new Type[] {
      Types.BooleanType.get(),
      Types.IntegerType.get(),
      Types.LongType.get(),
      Types.FloatType.get(),
      Types.DoubleType.get(),
      Types.DateType.get(),
      Types.TimeType.get(),
      Types.TimestampType.withZone(),
      Types.TimestampType.withoutZone(),
      Types.TimestampNanoType.withZone(),
      Types.TimestampNanoType.withoutZone(),
      Types.StringType.get(),
      Types.UUIDType.get(),
      Types.FixedType.ofLength(4),
      Types.BinaryType.get(),
      Types.DecimalType.of(9, 2),
    };
  }
}
