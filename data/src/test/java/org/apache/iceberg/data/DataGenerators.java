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
package org.apache.iceberg.data;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

/**
 * Test data generators for different schema types. Add new generators to ALL array to include them
 * in format model tests.
 */
class DataGenerators {

  static final DataGenerator[] ALL =
      new DataGenerator[] {
        new Primitives(),
        new UUID(),
        new Fixed(),
        new Binary(),
        new Decimals(),
        new StructOfPrimitive(),
        new ListOfPrimitive(),
        new MapOfPrimitive(),
        new TimestampNano()
      };

  private DataGenerators() {}

  static class Primitives implements DataGenerator {
    private final Schema schema =
        new Schema(
            Types.NestedField.required(1, "col_a", Types.StringType.get()),
            Types.NestedField.required(2, "col_b", Types.IntegerType.get()),
            Types.NestedField.required(3, "col_c", Types.LongType.get()),
            Types.NestedField.required(4, "col_d", Types.FloatType.get()),
            Types.NestedField.required(5, "col_e", Types.DoubleType.get()),
            Types.NestedField.required(6, "boolean_col", Types.BooleanType.get()),
            Types.NestedField.required(7, "decimal_col", Types.DecimalType.of(9, 2)),
            Types.NestedField.required(8, "date_col", Types.DateType.get()),
            Types.NestedField.required(9, "time_col", Types.TimeType.get()),
            Types.NestedField.required(10, "timestamp_col", Types.TimestampType.withoutZone()),
            Types.NestedField.required(11, "timestamp_tz_col", Types.TimestampType.withZone()));

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public String toString() {
      return "Primitives";
    }
  }

  /**
   * Narrow five-column schema used as a default fixture for tests that exercise generic
   * read/write/projection/evolution flows without needing every primitive type. Not included in
   * {@link #ALL} because the broader {@link Primitives} schema already covers parameterized
   * coverage.
   */
  static class DefaultSchema implements DataGenerator {
    private final Schema schema =
        new Schema(
            Types.NestedField.required(1, "col_a", Types.StringType.get()),
            Types.NestedField.required(2, "col_b", Types.IntegerType.get()),
            Types.NestedField.required(3, "col_c", Types.LongType.get()),
            Types.NestedField.required(4, "col_d", Types.FloatType.get()),
            Types.NestedField.required(5, "col_e", Types.DoubleType.get()));

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public String toString() {
      return "DefaultSchema";
    }
  }

  static class UUID implements DataGenerator {
    private final Schema schema =
        new Schema(Types.NestedField.required(1, "uuid_col", Types.UUIDType.get()));

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public String toString() {
      return "UUID";
    }
  }

  static class Fixed implements DataGenerator {
    private final Schema schema =
        new Schema(Types.NestedField.required(1, "fixed_col", Types.FixedType.ofLength(16)));

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public String toString() {
      return "Fixed";
    }
  }

  static class Binary implements DataGenerator {
    private final Schema schema =
        new Schema(Types.NestedField.required(1, "binary_col", Types.BinaryType.get()));

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public String toString() {
      return "Binary";
    }
  }

  static class StructOfPrimitive implements DataGenerator {
    private final Schema schema =
        new Schema(
            Types.NestedField.required(1, "row_id", Types.StringType.get()),
            Types.NestedField.required(
                2,
                "struct_of_primitive",
                Types.StructType.of(
                    required(101, "id", Types.IntegerType.get()),
                    required(102, "name", Types.StringType.get()))));

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public String toString() {
      return "StructOfPrimitive";
    }
  }

  static class Decimals implements DataGenerator {
    private final Schema schema =
        new Schema(
            required(1, "dec_9_2", Types.DecimalType.of(9, 2)),
            required(2, "dec_15_3", Types.DecimalType.of(15, 3)),
            required(3, "dec_38_10", Types.DecimalType.of(38, 10)));

    @Override
    public Schema schema() {
      return schema;
    }
  }

  static class ListOfPrimitive implements DataGenerator {
    private final Schema schema =
        new Schema(
            Types.NestedField.required(1, "row_id", Types.StringType.get()),
            Types.NestedField.required(
                2, "list_col", Types.ListType.ofRequired(3, Types.StringType.get())));

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public String toString() {
      return "ListOfPrimitive";
    }
  }

  static class MapOfPrimitive implements DataGenerator {
    private final Schema schema =
        new Schema(
            Types.NestedField.required(1, "row_id", Types.StringType.get()),
            Types.NestedField.required(
                2,
                "map_col",
                Types.MapType.ofRequired(3, 4, Types.StringType.get(), Types.IntegerType.get())));

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public String toString() {
      return "MapOfPrimitive";
    }
  }

  static class TimestampNano implements DataGenerator {
    private final Schema schema =
        new Schema(
            Types.NestedField.required(
                1, "timestamp_ns_col", Types.TimestampNanoType.withoutZone()),
            Types.NestedField.required(
                2, "timestamp_ns_tz_col", Types.TimestampNanoType.withZone()));

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public String toString() {
      return "TimestampNano";
    }
  }

  static class FloatDoubleSchema implements DataGenerator {
    private final Schema schema =
        new Schema(
            Types.NestedField.required(1, "col_float", Types.FloatType.get()),
            Types.NestedField.required(2, "col_double", Types.DoubleType.get()));

    @Override
    public Schema schema() {
      return schema;
    }
  }

  // Generator for reader default-value tests across primitive types.
  // FIXED is excluded: Spark's InternalRowConverter expects a ByteBuffer but the generator produces
  // byte[] (ClassCastException).
  // TODO: include FIXED once Spark's converter handles it.
  static class PrimitiveDefaults implements DataGenerator {
    static final Schema READ_SCHEMA =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optionalWithDefault(2, "bool_with_default", Types.BooleanType.get(), Literal.of(false)),
            optionalWithDefault(3, "int_with_default", Types.IntegerType.get(), Literal.of(34)),
            optionalWithDefault(
                4, "long_with_default", Types.LongType.get(), Literal.of(4900000000L)),
            optionalWithDefault(5, "float_with_default", Types.FloatType.get(), Literal.of(12.21F)),
            optionalWithDefault(
                6, "double_with_default", Types.DoubleType.get(), Literal.of(-0.0D)),
            optionalWithDefault(
                7,
                "date_with_default",
                Types.DateType.get(),
                Literal.of(DateTimeUtil.isoDateToDays("2024-12-17"))),
            optionalWithDefault(
                8,
                "timestamptz_with_default",
                Types.TimestampType.withZone(),
                Literal.of(
                    DateTimeUtil.isoTimestamptzToMicros("2024-12-17T23:59:59.999999+00:00"))),
            optionalWithDefault(
                9,
                "timestamp_with_default",
                Types.TimestampType.withoutZone(),
                Literal.of(DateTimeUtil.isoTimestampToMicros("2024-12-17T23:59:59.999999"))),
            optionalWithDefault(
                10, "string_with_default", Types.StringType.get(), Literal.of("iceberg")),
            optionalWithDefault(
                11,
                "uuid_with_default",
                Types.UUIDType.get(),
                Literal.of(java.util.UUID.randomUUID())),
            optionalWithDefault(
                12,
                "binary_with_default",
                Types.BinaryType.get(),
                Literal.of(ByteBuffer.wrap(new byte[] {0x0a, 0x0b}))),
            optionalWithDefault(
                13,
                "decimal_with_default",
                Types.DecimalType.of(9, 2),
                Literal.of(new BigDecimal("12.34"))),
            optionalWithDefault(
                14,
                "time_with_default",
                Types.TimeType.get(),
                Literal.of(DateTimeUtil.isoTimeToMicros("23:59:59.999999"))));

    static final Schema WRITE_SCHEMA = new Schema(required(1, "id", Types.LongType.get()));

    private static Types.NestedField optionalWithDefault(
        int id, String name, Type type, Literal<?> initialDefault) {
      return Types.NestedField.optional(name)
          .withId(id)
          .ofType(type)
          .withInitialDefault(initialDefault)
          .build();
    }

    @Override
    public Schema schema() {
      return READ_SCHEMA;
    }
  }
}
