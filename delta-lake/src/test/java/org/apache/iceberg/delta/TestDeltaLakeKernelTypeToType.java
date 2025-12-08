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
package org.apache.iceberg.delta;

import static org.assertj.core.api.Assertions.assertThat;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.types.VariantType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestDeltaLakeKernelTypeToType {
  private StructType deltaAtomicSchema;

  @BeforeEach
  public void constructDeltaLakeSchema() {
    deltaAtomicSchema =
        new StructType()
            .add("testBoolType", BooleanType.BOOLEAN, false)
            .add("testIntType", IntegerType.INTEGER)
            .add("testLongType", LongType.LONG, false)
            .add("testFloatType", FloatType.FLOAT)
            .add("testDoubleType", DoubleType.DOUBLE, false)
            .add("testStringType", StringType.STRING)
            .add("testBinaryType", BinaryType.BINARY, false)
            .add("testDateType", DateType.DATE)
            .add("testDecimalType", DecimalType.USER_DEFAULT, false)
            .add("testDecimalMaxType", new DecimalType(38, 38));
  }

  @Test
  public void testAtomicTypeConversion() {
    Type converted =
        new DeltaLakeKernelTypeToType(deltaAtomicSchema).convertType(deltaAtomicSchema);
    Schema convertedSchema = new Schema(converted.asNestedType().asStructType().fields());

    assertToAtomicSchema(convertedSchema);
  }

  private void assertToAtomicSchema(Schema convertedSchema) {
    assertThat(convertedSchema.columns().size()).isEqualTo(deltaAtomicSchema.fields().size());
    for (StructField field : deltaAtomicSchema.fields()) {
      assertThat(convertedSchema.findField(field.getName())).isNotNull();
      assertThat(convertedSchema.findField(field.getName()).isOptional())
          .isEqualTo(field.isNullable());
    }

    assertThat(convertedSchema.findType("testBoolType")).isInstanceOf(Types.BooleanType.class);
    assertThat(convertedSchema.findType("testIntType")).isInstanceOf(Types.IntegerType.class);
    assertThat(convertedSchema.findType("testLongType")).isInstanceOf(Types.LongType.class);
    assertThat(convertedSchema.findType("testFloatType")).isInstanceOf(Types.FloatType.class);
    assertThat(convertedSchema.findType("testDoubleType")).isInstanceOf(Types.DoubleType.class);
    assertThat(convertedSchema.findType("testStringType")).isInstanceOf(Types.StringType.class);
    assertThat(convertedSchema.findType("testBinaryType")).isInstanceOf(Types.BinaryType.class);
    assertThat(convertedSchema.findType("testDateType")).isInstanceOf(Types.DateType.class);
    assertThat(convertedSchema.findType("testDecimalType")).isInstanceOf(Types.DecimalType.class);

    assertThat(convertedSchema.findType("testDecimalMaxType"))
        .isInstanceOf(Types.DecimalType.class);
    Types.DecimalType testDecimalMaxType =
        (Types.DecimalType) convertedSchema.findType("testDecimalMaxType");
    assertThat(testDecimalMaxType.precision()).isEqualTo(38);
    assertThat(testDecimalMaxType.scale()).isEqualTo(38);
  }

  @Test
  public void testUpcastConversion() {
    StructType deltaSchema =
        new StructType()
            .add("nullableByte", ByteType.BYTE)
            .add("requiredShort", ShortType.SHORT, false);
    Type converted = new DeltaLakeKernelTypeToType(deltaSchema).convertType(deltaSchema);

    Schema convertedSchema = new Schema(converted.asNestedType().asStructType().fields());

    assertThat(convertedSchema.findType("nullableByte")).isInstanceOf(Types.IntegerType.class);
    assertThat(convertedSchema.findField("nullableByte").isOptional()).isTrue();
    assertThat(convertedSchema.findType("requiredShort")).isInstanceOf(Types.IntegerType.class);
    assertThat(convertedSchema.findField("requiredShort").isRequired()).isTrue();
  }

  @Test
  public void testTimestampsConversion() {
    StructType deltaSchema =
        new StructType()
            .add("nullableTimestamp", TimestampType.TIMESTAMP)
            .add("requiredTimestampNtz", TimestampNTZType.TIMESTAMP_NTZ, false);
    Type converted = new DeltaLakeKernelTypeToType(deltaSchema).convertType(deltaSchema);

    Schema convertedSchema = new Schema(converted.asNestedType().asStructType().fields());

    assertThat(convertedSchema.findType("nullableTimestamp"))
        .isInstanceOf(Types.TimestampType.class);
    assertThat(
            ((Types.TimestampType) convertedSchema.findType("nullableTimestamp"))
                .shouldAdjustToUTC())
        .isTrue();
    assertThat(convertedSchema.findField("nullableTimestamp").isOptional()).isTrue();
    assertThat(convertedSchema.findType("requiredTimestampNtz"))
        .isInstanceOf(Types.TimestampType.class);
    assertThat(
            ((Types.TimestampType) convertedSchema.findType("requiredTimestampNtz"))
                .shouldAdjustToUTC())
        .isFalse();
    assertThat(convertedSchema.findField("requiredTimestampNtz").isRequired()).isTrue();
  }

  @Test
  public void testVariantConversion() {
    StructType deltaSchema =
        new StructType() // Spec v3 is required
            .add("nullableVariant", VariantType.VARIANT);
    Type converted = new DeltaLakeKernelTypeToType(deltaSchema).convertType(deltaSchema);

    Schema convertedSchema = new Schema(converted.asNestedType().asStructType().fields());

    assertThat(convertedSchema.findType("nullableVariant")).isInstanceOf(Types.VariantType.class);
    assertThat(convertedSchema.findField("nullableVariant").isOptional()).isTrue();
  }

  @Test
  public void testArrayConversion() {
    StructType deltaSchema =
        new StructType().add("testDoubleArray", new ArrayType(DoubleType.DOUBLE, true), false);

    Type converted = new DeltaLakeKernelTypeToType(deltaSchema).convertType(deltaSchema);

    Schema convertedSchema = new Schema(converted.asNestedType().asStructType().fields());

    assertThat(convertedSchema.findType("testDoubleArray")).isInstanceOf(Types.ListType.class);
    assertThat(convertedSchema.findField("testDoubleArray").isRequired()).isTrue();
    assertThat(convertedSchema.findType("testDoubleArray").asListType().elementType())
        .isInstanceOf(Types.DoubleType.class);
    assertThat(convertedSchema.findType("testDoubleArray").asListType().isElementOptional())
        .isTrue();
  }

  @Test
  public void testMapConversion() {
    StructType deltaSchema =
        new StructType()
            .add("testStringLongMap", new MapType(StringType.STRING, LongType.LONG, false), false);
    Type converted = new DeltaLakeKernelTypeToType(deltaSchema).convertType(deltaSchema);

    Schema convertedSchema = new Schema(converted.asNestedType().asStructType().fields());

    assertThat(convertedSchema.findType("testStringLongMap")).isInstanceOf(Types.MapType.class);
    assertThat(convertedSchema.findField("testStringLongMap").isRequired()).isTrue();
    assertThat(convertedSchema.findType("testStringLongMap").asMapType().keyType())
        .isInstanceOf(Types.StringType.class);
    assertThat(convertedSchema.findType("testStringLongMap").asMapType().valueType())
        .isInstanceOf(Types.LongType.class);
    assertThat(convertedSchema.findType("testStringLongMap").asMapType().isValueRequired())
        .isTrue();
  }

  @Test
  public void testNestedTypeConversion() {
    StructType deltaNestedSchema =
        new StructType()
            .add("testInnerAtomicSchema", deltaAtomicSchema)
            .add("testStructArrayType", new ArrayType(deltaAtomicSchema, true), false)
            .add(
                "testStringStructMap",
                new MapType(StringType.STRING, deltaAtomicSchema, false),
                false);
    Type converted =
        new DeltaLakeKernelTypeToType(deltaNestedSchema).convertType(deltaNestedSchema);
    Schema convertedSchema = new Schema(converted.asNestedType().asStructType().fields());

    assertThat(convertedSchema.findType("testInnerAtomicSchema"))
        .isInstanceOf(Types.StructType.class);
    assertThat(convertedSchema.findField("testInnerAtomicSchema").isOptional()).isTrue();
    assertToAtomicSchema(
        convertedSchema.findType("testInnerAtomicSchema").asStructType().asSchema());

    assertThat(convertedSchema.findType("testStructArrayType")).isInstanceOf(Types.ListType.class);
    assertThat(convertedSchema.findField("testStructArrayType").isRequired()).isTrue();
    assertThat(convertedSchema.findType("testStructArrayType").asListType().isElementOptional())
        .isTrue();
    assertThat(convertedSchema.findType("testStructArrayType").asListType().elementType())
        .isInstanceOf(Types.StructType.class);
    assertToAtomicSchema(
        convertedSchema
            .findType("testStructArrayType")
            .asListType()
            .elementType()
            .asStructType()
            .asSchema());

    assertThat(convertedSchema.findType("testStringStructMap")).isInstanceOf(Types.MapType.class);
    assertThat(convertedSchema.findField("testStringStructMap").isRequired()).isTrue();
    assertThat(convertedSchema.findType("testStringStructMap").asMapType().keyType())
        .isInstanceOf(Types.StringType.class);
    assertThat(convertedSchema.findType("testStringStructMap").asMapType().valueType())
        .isInstanceOf(Types.StructType.class);
    assertThat(convertedSchema.findType("testStringStructMap").asMapType().isValueRequired())
        .isTrue();
    assertToAtomicSchema(
        convertedSchema
            .findType("testStringStructMap")
            .asMapType()
            .valueType()
            .asStructType()
            .asSchema());
  }
}
