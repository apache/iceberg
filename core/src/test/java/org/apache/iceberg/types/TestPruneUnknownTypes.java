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
package org.apache.iceberg.types;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Schema;
import org.junit.jupiter.api.Test;

public class TestPruneUnknownTypes {

  @Test
  public void testPruneTopLevelUnknown() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "int", Types.IntegerType.get()),
            Types.NestedField.optional(2, "unk", Types.UnknownType.get()));

    Schema expectedSchema =
        new Schema(Types.NestedField.optional(1, "int", Types.IntegerType.get()));

    Schema actualSchema = PruneUnknownTypes.convert(schema);
    assertThat(actualSchema.asStruct()).isEqualTo(expectedSchema.asStruct());
  }

  @Test
  public void testPruneNestedWithAnUnknown() {
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, "int", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "nested",
                Types.StructType.of(
                    Types.NestedField.optional(20, "unk", Types.UnknownType.get()),
                    Types.NestedField.optional(21, "int", Types.IntegerType.get()))));

    Schema expectedSchema =
        new Schema(
            Types.NestedField.optional(1, "int", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "nested",
                Types.StructType.of(
                    Types.NestedField.optional(21, "int", Types.IntegerType.get()))));

    Schema actualSchema = PruneUnknownTypes.convert(schema);
    assertThat(actualSchema.asStruct()).isEqualTo(expectedSchema.asStruct());
  }

  @Test
  public void testPruneComplexNested() {
    Schema schema =
        new Schema(
            Types.NestedField.required(
                12,
                "list",
                Types.ListType.ofRequired(
                    13,
                    Types.ListType.ofRequired(
                        14,
                        Types.MapType.ofRequired(
                            15,
                            16,
                            Types.IntegerType.get(),
                            Types.StructType.of(
                                Types.NestedField.required(17, "x", Types.IntegerType.get()),
                                Types.NestedField.required(18, "y", Types.IntegerType.get()),
                                Types.NestedField.optional(
                                    19, "unk", Types.UnknownType.get())))))));

    Schema expectedSchema =
        new Schema(
            Types.NestedField.required(
                12,
                "list",
                Types.ListType.ofRequired(
                    13,
                    Types.ListType.ofRequired(
                        14,
                        Types.MapType.ofRequired(
                            15,
                            16,
                            Types.IntegerType.get(),
                            Types.StructType.of(
                                Types.NestedField.required(17, "x", Types.IntegerType.get()),
                                Types.NestedField.required(18, "y", Types.IntegerType.get())))))));

    Schema actualSchema = PruneUnknownTypes.convert(schema);
    assertThat(actualSchema.asStruct()).isEqualTo(expectedSchema.asStruct());
  }
}
