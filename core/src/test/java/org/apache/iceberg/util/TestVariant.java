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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.types.variant.Variant;
import org.apache.spark.types.variant.VariantBuilder;
import org.junit.jupiter.api.Test;

public class TestVariant {
  @Test
  public void testVariant() throws IOException {
    // TODO make sure the test is reasonable
    assertAccessorReturns(Types.VariantType.get(), encodeJSON("{\"name\": \"john\"}"));
  }

  private static Record encodeJSON(String json) throws IOException {
    Variant variant = VariantBuilder.parseJson(json);

    Types.NestedField valueType = required(1, "Value", Types.BinaryType.get());
    Types.NestedField metadataType = required(2, "Metadata", Types.BinaryType.get());

    Schema schema = new Schema(List.of(valueType, metadataType));
    return GenericRecord.create(schema)
        .copy("Value", variant.getValue(), "Metadata", variant.getMetadata());
  }

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
}
