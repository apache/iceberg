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
package org.apache.iceberg.delta.utils;

import io.delta.standalone.types.ArrayType;
import io.delta.standalone.types.BinaryType;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.MapType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDeltaLakeTypeToType {
  private static final String optionalBooleanType = "testNullableBoolType";

  private static final String requiredBinaryType = "testRequiredBinaryType";

  private static final String doubleArrayType = "testNullableArrayType";

  private static final String innerAtomicSchema = "testInnerAtomicSchema";

  private static final String stringLongMapType = "testStringLongMap";

  private StructType deltaAtomicSchema;

  private StructType deltaNestedSchema;

  @Before
  public void constructDeltaLakeSchema() {
    deltaAtomicSchema =
        new StructType()
            .add(optionalBooleanType, new BooleanType())
            .add(requiredBinaryType, new BinaryType(), false);
    deltaNestedSchema =
        new StructType()
            .add(innerAtomicSchema, deltaAtomicSchema)
            .add(doubleArrayType, new ArrayType(new DoubleType(), true), false)
            .add(stringLongMapType, new MapType(new StringType(), new LongType(), false), false);
  }

  @Test
  public void testAtomicTypeConversion() {
    Type converted =
        DeltaLakeDataTypeVisitor.visit(
            deltaAtomicSchema, new DeltaLakeTypeToType(deltaAtomicSchema));
    Schema convertedSchema = new Schema(converted.asNestedType().asStructType().fields());
    Assert.assertTrue(
        "The BooleanType should be converted to BooleanType",
        convertedSchema.findType(optionalBooleanType) instanceof Types.BooleanType);
    Assert.assertTrue(
        "The converted BooleanType field is optional",
        convertedSchema.findField(optionalBooleanType).isOptional());
    Assert.assertTrue(
        "The BinaryType is converted to BinaryType",
        convertedSchema.findType(requiredBinaryType) instanceof Types.BinaryType);
    Assert.assertTrue(
        "The converted BinaryType field is required",
        convertedSchema.findField(requiredBinaryType).isRequired());
  }

  @Test
  public void testNestedTypeConversion() {
    Type converted =
        DeltaLakeDataTypeVisitor.visit(
            deltaNestedSchema, new DeltaLakeTypeToType(deltaNestedSchema));
    Schema convertedSchema = new Schema(converted.asNestedType().asStructType().fields());
    Assert.assertTrue(
        "The StructType is converted to StructType",
        convertedSchema.findType(innerAtomicSchema) instanceof Types.StructType);
    Assert.assertTrue(
        "The converted StructType contains subfield BooleanType",
        convertedSchema.findType(innerAtomicSchema).asStructType().fieldType(optionalBooleanType)
            instanceof Types.BooleanType);
    Assert.assertTrue(
        "The converted StructType contains subfield BinaryType",
        convertedSchema.findType(innerAtomicSchema).asStructType().fieldType(requiredBinaryType)
            instanceof Types.BinaryType);

    Assert.assertTrue(
        "The MapType is converted to MapType",
        convertedSchema.findType(stringLongMapType) instanceof Types.MapType);
    Assert.assertTrue(
        "The converted MapType has key as StringType",
        convertedSchema.findType(stringLongMapType).asMapType().keyType()
            instanceof Types.StringType);
    Assert.assertTrue(
        "The converted MapType has value as LongType",
        convertedSchema.findType(stringLongMapType).asMapType().valueType()
            instanceof Types.LongType);

    Assert.assertTrue(
        "The ArrayType is converted to ListType",
        convertedSchema.findType(doubleArrayType) instanceof Types.ListType);
    Assert.assertTrue(
        "The converted ListType field is required",
        convertedSchema.findField(doubleArrayType).isRequired());
    Assert.assertTrue(
        "The converted ListType field contains optional doubleType element",
        convertedSchema.findType(doubleArrayType).asListType().isElementOptional());
  }
}
