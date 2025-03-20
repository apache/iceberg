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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestSparkFixupTypes {

  @Test
  void fixupShouldCorrectlyFixUUID() {
    Schema schema = new Schema(Types.NestedField.required(1, "field", Types.StringType.get()));
    Schema referenceSchema =
        new Schema(Types.NestedField.required(1, "field", Types.UUIDType.get()));

    Schema fixedSchema = SparkFixupTypes.fixup(schema, referenceSchema);

    assertThat(fixedSchema.findType("field")).isSameAs(Types.UUIDType.get());
  }

  @Test
  void fixupShouldCorrectlyFixBinary() {
    Schema schema = new Schema(Types.NestedField.required(1, "field", Types.BinaryType.get()));
    Schema referenceSchema =
        new Schema(Types.NestedField.required(1, "field", Types.FixedType.ofLength(16)));

    Schema fixedSchema = SparkFixupTypes.fixup(schema, referenceSchema);

    assertThat(fixedSchema.findType("field")).isEqualTo(Types.FixedType.ofLength(16));
  }

  @Test
  void fixupShouldCorrectlyFixVariant() {
    Schema schema = new Schema(Types.NestedField.required(1, "field", Types.BinaryType.get()));
    Schema referenceSchema =
        new Schema(Types.NestedField.required(1, "field", Types.VariantType.get()));

    Schema fixedSchema = SparkFixupTypes.fixup(schema, referenceSchema);

    assertThat(fixedSchema.findType("field")).isSameAs(Types.VariantType.get());
  }

  @Test
  void fixupShouldCorrectlyFixTimestamp() {
    Schema schema =
        new Schema(Types.NestedField.required(1, "field", Types.TimestampType.withZone()));
    Schema referenceSchema =
        new Schema(Types.NestedField.required(1, "field", Types.TimestampType.withoutZone()));

    Schema fixedSchema = SparkFixupTypes.fixup(schema, referenceSchema);

    assertThat(fixedSchema.findType("field")).isSameAs(Types.TimestampType.withoutZone());
  }

  @Test
  void fixupShouldHandleNestedStructs() {
    Schema schema =
        new Schema(
            Types.NestedField.required(
                1,
                "struct",
                Types.StructType.of(
                    Types.NestedField.required(2, "field", Types.StringType.get()))));
    Schema referenceSchema =
        new Schema(
            Types.NestedField.required(
                1,
                "struct",
                Types.StructType.of(Types.NestedField.required(2, "field", Types.UUIDType.get()))));

    Schema fixedSchema = SparkFixupTypes.fixup(schema, referenceSchema);

    assertThat(fixedSchema.findType("struct"))
        .isEqualTo(
            Types.StructType.of(Types.NestedField.required(2, "field", Types.UUIDType.get())));
  }

  @Test
  void fixupShouldHandleLists() {
    Schema schema =
        new Schema(
            Types.NestedField.required(
                1, "list", Types.ListType.ofRequired(2, Types.StringType.get())));
    Schema referenceSchema =
        new Schema(
            Types.NestedField.required(
                1, "list", Types.ListType.ofRequired(2, Types.UUIDType.get())));

    Schema fixedSchema = SparkFixupTypes.fixup(schema, referenceSchema);

    assertThat(fixedSchema.findType("list"))
        .isEqualTo(Types.ListType.ofRequired(2, Types.UUIDType.get()));
  }

  @Test
  void fixupShouldHandleMaps() {
    Schema schema =
        new Schema(
            Types.NestedField.required(
                1,
                "map",
                Types.MapType.ofRequired(2, 3, Types.StringType.get(), Types.StringType.get())));
    Schema referenceSchema =
        new Schema(
            Types.NestedField.required(
                1,
                "map",
                Types.MapType.ofRequired(2, 3, Types.UUIDType.get(), Types.UUIDType.get())));

    Schema fixedSchema = SparkFixupTypes.fixup(schema, referenceSchema);

    assertThat(fixedSchema.findType("map"))
        .isEqualTo(Types.MapType.ofRequired(2, 3, Types.UUIDType.get(), Types.UUIDType.get()));
  }
}
