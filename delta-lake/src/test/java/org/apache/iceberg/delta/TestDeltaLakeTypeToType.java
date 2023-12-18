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

import io.delta.standalone.types.ArrayType;
import io.delta.standalone.types.BinaryType;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.MapType;
import io.delta.standalone.types.NullType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestDeltaLakeTypeToType {
  private static final String optionalBooleanType = "testNullableBoolType";
  private static final String requiredBinaryType = "testRequiredBinaryType";
  private static final String doubleArrayType = "testNullableArrayType";
  private static final String structArrayType = "testStructArrayType";
  private static final String innerAtomicSchema = "testInnerAtomicSchema";
  private static final String stringLongMapType = "testStringLongMap";
  private static final String nullType = "testNullType";
  private StructType deltaAtomicSchema;
  private StructType deltaNestedSchema;
  private StructType deltaShallowNullTypeSchema;
  private StructType deltaNullTypeSchema;

  @BeforeEach
  public void constructDeltaLakeSchema() {
    deltaAtomicSchema =
        new StructType()
            .add(optionalBooleanType, new BooleanType())
            .add(requiredBinaryType, new BinaryType(), false);
    deltaNestedSchema =
        new StructType()
            .add(innerAtomicSchema, deltaAtomicSchema)
            .add(doubleArrayType, new ArrayType(new DoubleType(), true), false)
            .add(structArrayType, new ArrayType(deltaAtomicSchema, true), false)
            .add(stringLongMapType, new MapType(new StringType(), new LongType(), false), false);
    deltaNullTypeSchema =
        new StructType()
            .add(innerAtomicSchema, deltaAtomicSchema)
            .add(doubleArrayType, new ArrayType(new DoubleType(), true), false)
            .add(stringLongMapType, new MapType(new NullType(), new LongType(), false), false);
    deltaShallowNullTypeSchema = new StructType().add(nullType, new NullType(), false);
  }

  @Test
  public void testAtomicTypeConversion() {
    Type converted =
        DeltaLakeDataTypeVisitor.visit(
            deltaAtomicSchema, new DeltaLakeTypeToType(deltaAtomicSchema));
    Schema convertedSchema = new Schema(converted.asNestedType().asStructType().fields());

    Assertions.assertThat(convertedSchema.findType(optionalBooleanType))
        .isInstanceOf(Types.BooleanType.class);
    Assertions.assertThat(convertedSchema.findField(optionalBooleanType).isOptional()).isTrue();
    Assertions.assertThat(convertedSchema.findType(requiredBinaryType))
        .isInstanceOf(Types.BinaryType.class);
    Assertions.assertThat(convertedSchema.findField(requiredBinaryType).isRequired()).isTrue();
  }

  @Test
  public void testNestedTypeConversion() {
    Type converted =
        DeltaLakeDataTypeVisitor.visit(
            deltaNestedSchema, new DeltaLakeTypeToType(deltaNestedSchema));
    Schema convertedSchema = new Schema(converted.asNestedType().asStructType().fields());

    Assertions.assertThat(convertedSchema.findType(innerAtomicSchema))
        .isInstanceOf(Types.StructType.class);
    Assertions.assertThat(convertedSchema.findField(innerAtomicSchema).isOptional()).isTrue();
    Assertions.assertThat(
            convertedSchema
                .findType(innerAtomicSchema)
                .asStructType()
                .fieldType(optionalBooleanType))
        .isInstanceOf(Types.BooleanType.class);
    Assertions.assertThat(
            convertedSchema
                .findType(innerAtomicSchema)
                .asStructType()
                .fieldType(requiredBinaryType))
        .isInstanceOf(Types.BinaryType.class);
    Assertions.assertThat(
            convertedSchema
                .findType(innerAtomicSchema)
                .asStructType()
                .field(requiredBinaryType)
                .isRequired())
        .isTrue();
    Assertions.assertThat(convertedSchema.findType(stringLongMapType))
        .isInstanceOf(Types.MapType.class);
    Assertions.assertThat(convertedSchema.findType(stringLongMapType).asMapType().keyType())
        .isInstanceOf(Types.StringType.class);
    Assertions.assertThat(convertedSchema.findType(stringLongMapType).asMapType().valueType())
        .isInstanceOf(Types.LongType.class);
    Assertions.assertThat(convertedSchema.findType(doubleArrayType))
        .isInstanceOf(Types.ListType.class);
    Assertions.assertThat(convertedSchema.findField(doubleArrayType).isRequired()).isTrue();
    Assertions.assertThat(
            convertedSchema.findType(doubleArrayType).asListType().isElementOptional())
        .isTrue();
    Assertions.assertThat(convertedSchema.findType(structArrayType))
        .isInstanceOf(Types.ListType.class);
    Assertions.assertThat(convertedSchema.findField(structArrayType).isRequired()).isTrue();
    Assertions.assertThat(
            convertedSchema.findType(structArrayType).asListType().isElementOptional())
        .isTrue();
    Assertions.assertThat(convertedSchema.findType(structArrayType).asListType().elementType())
        .isInstanceOf(Types.StructType.class);
    Assertions.assertThat(
            convertedSchema
                .findType(structArrayType)
                .asListType()
                .elementType()
                .asStructType()
                .fieldType(optionalBooleanType))
        .isInstanceOf(Types.BooleanType.class);
    Assertions.assertThat(
            convertedSchema
                .findType(structArrayType)
                .asListType()
                .elementType()
                .asStructType()
                .field(optionalBooleanType)
                .isOptional())
        .isTrue();
    Assertions.assertThat(
            convertedSchema
                .findType(structArrayType)
                .asListType()
                .elementType()
                .asStructType()
                .fieldType(requiredBinaryType))
        .isInstanceOf(Types.BinaryType.class);
    Assertions.assertThat(
            convertedSchema
                .findType(structArrayType)
                .asListType()
                .elementType()
                .asStructType()
                .field(requiredBinaryType)
                .isRequired())
        .isTrue();
  }

  @Test
  public void testNullTypeConversion() {
    Assertions.assertThatThrownBy(
            () ->
                DeltaLakeDataTypeVisitor.visit(
                    deltaNullTypeSchema, new DeltaLakeTypeToType(deltaNullTypeSchema)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(String.format("Not a supported type: %s", new NullType().getCatalogString()));
    Assertions.assertThatThrownBy(
            () ->
                DeltaLakeDataTypeVisitor.visit(
                    deltaShallowNullTypeSchema,
                    new DeltaLakeTypeToType(deltaShallowNullTypeSchema)))
        .isInstanceOf(ValidationException.class)
        .hasMessage(String.format("Not a supported type: %s", new NullType().getCatalogString()));
  }
}
