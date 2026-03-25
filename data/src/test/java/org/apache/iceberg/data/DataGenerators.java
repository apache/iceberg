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

  static final DataGenerator[] ALL = new DataGenerator[] {new StructOfPrimitive()};

  private DataGenerators() {}

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
  }

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
  }
}
