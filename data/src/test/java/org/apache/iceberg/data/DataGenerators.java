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

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

/**
 * Test data generators for different schema types. Add new generators to ALL array to include them
 * in format model tests.
 */
class DataGenerators {

  static final DataGenerator[] ALL =
      new DataGenerator[] {
        new DefaultSchema(),
        new Decimals(),
        new StructOfPrimitive(),
        new ListOfPrimitive(),
        new MapOfPrimitive()
      };

  private DataGenerators() {}

  static class DefaultSchema implements DataGenerator {
    private final Schema schema =
        new Schema(
            Types.NestedField.required(1, "boolean_col", Types.BooleanType.get()),
            Types.NestedField.required(2, "int_col", Types.IntegerType.get()),
            Types.NestedField.required(3, "long_col", Types.LongType.get()),
            Types.NestedField.required(4, "float_col", Types.FloatType.get()),
            Types.NestedField.required(5, "double_col", Types.DoubleType.get()),
            Types.NestedField.required(6, "decimal_col", Types.DecimalType.of(9, 2)),
            Types.NestedField.required(7, "date_col", Types.DateType.get()),
            Types.NestedField.required(8, "time_col", Types.TimeType.get()),
            Types.NestedField.required(9, "timestamp_col", Types.TimestampType.withoutZone()),
            Types.NestedField.required(10, "timestamp_tz_col", Types.TimestampType.withZone()),
            Types.NestedField.required(11, "string_col", Types.StringType.get()),
            Types.NestedField.required(12, "uuid_col", Types.UUIDType.get()),
            Types.NestedField.required(13, "fixed_col", Types.FixedType.ofLength(16)),
            Types.NestedField.required(14, "binary_col", Types.BinaryType.get()));

    @Override
    public Schema schema() {
      return schema;
    }

    @Override
    public String toString() {
      return "DefaultSchema";
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
}
