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

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.VariantType;
import org.apache.iceberg.variants.Variant;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

public class TestVectorizedReaderBuilder {

  @Test
  public void testVariantNotSupportedInVectorizedReads() {
    Schema icebergSchema =
        new Schema(
            NestedField.required(1, "id", IntegerType.get()),
            NestedField.optional(2, "data", VariantType.get()));

    MessageType parquetSchema = parquetSchemaWithVariant();

    VectorizedReaderBuilder builder =
        new VectorizedReaderBuilder(
            icebergSchema, parquetSchema, false, ImmutableMap.of(), readers -> null);

    assertThatThrownBy(
            () -> TypeWithSchemaVisitor.visit(icebergSchema.asStruct(), parquetSchema, builder))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Vectorized reads are not supported yet for variant fields");
  }

  @Test
  public void testVariantSkippedWhenNotInProjection() {
    Schema icebergSchema = new Schema(NestedField.required(1, "id", IntegerType.get()));

    MessageType parquetSchema = parquetSchemaWithVariant();

    VectorizedReaderBuilder builder =
        new VectorizedReaderBuilder(
            icebergSchema, parquetSchema, false, ImmutableMap.of(), readers -> null);

    assertThatNoException()
        .describedAs("Variant not in projection should not throw")
        .isThrownBy(
            () -> TypeWithSchemaVisitor.visit(icebergSchema.asStruct(), parquetSchema, builder));
  }

  private static MessageType parquetSchemaWithVariant() {
    return Types.buildMessage()
        .addField(
            Types.primitive(PrimitiveTypeName.INT32, Type.Repetition.REQUIRED).id(1).named("id"))
        .addField(
            Types.buildGroup(Type.Repetition.OPTIONAL)
                .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
                .addField(
                    Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                        .named("metadata"))
                .addField(
                    Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                        .named("value"))
                .id(2)
                .named("data"))
        .named("table");
  }
}
