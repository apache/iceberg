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
package org.apache.iceberg.util;

import java.io.IOException;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

public class TestVariant {

  private static Accessor<StructLike> direct(Type type) {
    Schema schema = new Schema(required(17, "field_" + type.typeId(), type));
    return schema.accessorForField(17);
  }

  private static Accessor<StructLike> nested1(Type type) {
    Schema schema =
        new Schema(
            required(
                11,
                "struct1",
                Types.StructType.of(
                    Types.NestedField.required(17, "field_" + type.typeId(), type))));
    return schema.accessorForField(17);
  }

  private static Accessor<StructLike> nested2(Type type) {
    Schema schema =
        new Schema(
            required(
                11,
                "s",
                Types.StructType.of(
                    Types.NestedField.required(
                        22,
                        "s2",
                        Types.StructType.of(
                            Types.NestedField.required(17, "field_" + type.typeId(), type))))));
    return schema.accessorForField(17);
  }

  private static Accessor<StructLike> nested3(Type type) {
    Schema schema =
        new Schema(
            required(
                11,
                "s",
                Types.StructType.of(
                    Types.NestedField.required(
                        22,
                        "s2",
                        Types.StructType.of(
                            Types.NestedField.required(
                                33,
                                "s3",
                                Types.StructType.of(
                                    Types.NestedField.required(
                                        17, "field_" + type.typeId(), type))))))));
    return schema.accessorForField(17);
  }

  private static Accessor<StructLike> nested3optional(Type type) {
    Schema schema =
        new Schema(
            optional(
                11,
                "s",
                Types.StructType.of(
                    Types.NestedField.optional(
                        22,
                        "s2",
                        Types.StructType.of(
                            Types.NestedField.optional(
                                33,
                                "s3",
                                Types.StructType.of(
                                    Types.NestedField.optional(
                                        17, "field_" + type.typeId(), type))))))));
    return schema.accessorForField(17);
  }

  private static Accessor<StructLike> nested4(Type type) {
    Schema schema =
        new Schema(
            required(
                11,
                "s",
                Types.StructType.of(
                    Types.NestedField.required(
                        22,
                        "s2",
                        Types.StructType.of(
                            Types.NestedField.required(
                                33,
                                "s3",
                                Types.StructType.of(
                                    Types.NestedField.required(
                                        44,
                                        "s4",
                                        Types.StructType.of(
                                            Types.NestedField.required(
                                                17, "field_" + type.typeId(), type))))))))));
    return schema.accessorForField(17);
  }

  private void assertAccessorReturns(Type type, Object value) {
    assertThat(direct(type).get(Row.of(value))).isEqualTo(value);

    assertThat(nested1(type).get(Row.of(Row.of(value)))).isEqualTo(value);
    assertThat(nested2(type).get(Row.of(Row.of(Row.of(value))))).isEqualTo(value);
    assertThat(nested3(type).get(Row.of(Row.of(Row.of(Row.of(value)))))).isEqualTo(value);
    assertThat(nested4(type).get(Row.of(Row.of(Row.of(Row.of(Row.of(value))))))).isEqualTo(value);

    assertThat(nested3optional(type).get(Row.of(Row.of(Row.of(Row.of(value)))))).isEqualTo(value);
  }

  @Test
  public void testVariant() throws IOException {
    assertAccessorReturns(Types.VariantType.get(), VariantUtil.encodeJSON("{\"name\": \"john\"}"));
  }
}
