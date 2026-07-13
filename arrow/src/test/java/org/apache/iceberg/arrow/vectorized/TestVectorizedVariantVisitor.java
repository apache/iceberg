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
import org.apache.iceberg.parquet.ParquetVariantVisitor;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.VariantType;
import org.apache.iceberg.variants.Variant;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

public class TestVectorizedVariantVisitor {

  private static final Schema ICEBERG_SCHEMA =
      new Schema(NestedField.optional(1, "data", VariantType.get()));

  @Test
  public void testUnshreddedVariantDoesNotThrow() {
    MessageType schema = variantSchema(unshreddedGroup());
    assertThatNoException().isThrownBy(() -> visit(schema));
  }

  @Test
  public void testShreddedPrimitiveThrows() {
    MessageType schema = variantSchema(shreddedPrimitiveGroup());
    assertThatThrownBy(() -> visit(schema))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Unsupported variant: shredded typed_value primitive");
  }

  @Test
  public void testShreddedObjectThrows() {
    GroupType variantGroup = shreddedObjectGroup();
    int fieldCount = variantGroup.getType("typed_value").asGroupType().getFieldCount();
    MessageType schema = variantSchema(variantGroup);
    assertThatThrownBy(() -> visit(schema))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(
            "Unsupported variant: shredded typed_value object with " + fieldCount + " fields");
  }

  @Test
  public void testShreddedArrayThrows() {
    MessageType schema = variantSchema(shreddedArrayGroup());
    assertThatThrownBy(() -> visit(schema))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Unsupported variant: shredded typed_value array");
  }

  private static VectorizedReader<?> visit(MessageType schema) {
    GroupType variantGroup = schema.getType("data").asGroupType();
    VectorizedVariantVisitor visitor =
        new VectorizedVariantVisitor(new String[] {"data"}, schema, ICEBERG_SCHEMA, null, false);
    return ParquetVariantVisitor.visit(variantGroup, visitor);
  }

  private static MessageType variantSchema(GroupType variantGroup) {
    return Types.buildMessage().addField(variantGroup).named("table");
  }

  private static GroupType unshreddedGroup() {
    return Types.buildGroup(Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
        .addField(
            Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("metadata"))
        .addField(
            Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL).named("value"))
        .id(1)
        .named("data");
  }

  private static GroupType shreddedPrimitiveGroup() {
    return Types.buildGroup(Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
        .addField(
            Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("metadata"))
        .addField(
            Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL).named("value"))
        .addField(
            Types.primitive(PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL).named("typed_value"))
        .id(1)
        .named("data");
  }

  private static GroupType shreddedObjectGroup() {
    GroupType typedField =
        Types.buildGroup(Type.Repetition.OPTIONAL)
            .addField(
                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL).named("value"))
            .named("field_a");
    GroupType typedValue =
        Types.buildGroup(Type.Repetition.OPTIONAL).addField(typedField).named("typed_value");
    return Types.buildGroup(Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
        .addField(
            Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("metadata"))
        .addField(
            Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL).named("value"))
        .addField(typedValue)
        .id(1)
        .named("data");
  }

  private static GroupType shreddedArrayGroup() {
    GroupType elementValue =
        Types.buildGroup(Type.Repetition.OPTIONAL)
            .addField(
                Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL).named("value"))
            .named("element");
    GroupType repeated =
        Types.buildGroup(Type.Repetition.REPEATED).addField(elementValue).named("list");
    GroupType typedValue =
        Types.buildGroup(Type.Repetition.OPTIONAL)
            .as(LogicalTypeAnnotation.listType())
            .addField(repeated)
            .named("typed_value");
    return Types.buildGroup(Type.Repetition.OPTIONAL)
        .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
        .addField(
            Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).named("metadata"))
        .addField(
            Types.primitive(PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL).named("value"))
        .addField(typedValue)
        .id(1)
        .named("data");
  }
}
