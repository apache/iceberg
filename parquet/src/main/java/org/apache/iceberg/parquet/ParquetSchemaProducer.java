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
package org.apache.iceberg.parquet;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.variants.VariantArray;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantPrimitive;
import org.apache.iceberg.variants.VariantVisitor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

public class ParquetSchemaProducer extends VariantVisitor<Type> {
  @Override
  public Type object(VariantObject object, List<String> names, List<Type> typedValues) {
    if (object.numFields() < 1) {
      // Parquet cannot write  typed_value group with no fields
      return null;
    }

    List<GroupType> fields = Lists.newArrayList();
    int index = 0;
    for (String name : names) {
      Type typedValue = typedValues.get(index);
      fields.add(field(name, typedValue));
      index += 1;
    }

    return objectFields(fields);
  }

  @Override
  public Type array(VariantArray array, List<Type> elementResults) {
    if (elementResults.isEmpty()) {
      return null;
    }

    // Shred if all the elements are of a uniform type and build 3-level list
    Type shredType = elementResults.get(0);
    if (shredType != null
            && elementResults.stream().allMatch(type -> Objects.equals(type, shredType))) {
      return list(shredType);
    }

    return null;
  }

  private static GroupType list(Type shreddedType) {
    GroupType elementType = field("element", shreddedType);
    checkField(elementType);

    return Types.optionalList().element(elementType).named("typed_value");
  }

  @Override
  public Type primitive(VariantPrimitive<?> primitive) {
    switch (primitive.type()) {
      case NULL:
        return null;
      case BOOLEAN_TRUE:
      case BOOLEAN_FALSE:
        return shreddedPrimitive(PrimitiveType.PrimitiveTypeName.BOOLEAN);
      case INT8:
        return shreddedPrimitive(
            PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8));
      case INT16:
        return shreddedPrimitive(
            PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(16));
      case INT32:
        return shreddedPrimitive(PrimitiveType.PrimitiveTypeName.INT32);
      case INT64:
        return shreddedPrimitive(PrimitiveType.PrimitiveTypeName.INT64);
      case FLOAT:
        return shreddedPrimitive(PrimitiveType.PrimitiveTypeName.FLOAT);
      case DOUBLE:
        return shreddedPrimitive(PrimitiveType.PrimitiveTypeName.DOUBLE);
      case DECIMAL4:
        BigDecimal decimal4 = (BigDecimal) primitive.get();
        return shreddedPrimitive(
            PrimitiveType.PrimitiveTypeName.INT32,
            LogicalTypeAnnotation.decimalType(decimal4.scale(), 9));
      case DECIMAL8:
        BigDecimal decimal8 = (BigDecimal) primitive.get();
        return shreddedPrimitive(
            PrimitiveType.PrimitiveTypeName.INT64,
            LogicalTypeAnnotation.decimalType(decimal8.scale(), 18));
      case DECIMAL16:
        BigDecimal decimal16 = (BigDecimal) primitive.get();
        return shreddedPrimitive(
            PrimitiveType.PrimitiveTypeName.BINARY,
            LogicalTypeAnnotation.decimalType(decimal16.scale(), 38));
      case DATE:
        return shreddedPrimitive(
            PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType());
      case TIMESTAMPTZ:
        return shreddedPrimitive(
            PrimitiveType.PrimitiveTypeName.INT64,
            LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS));
      case TIMESTAMPNTZ:
        return shreddedPrimitive(
            PrimitiveType.PrimitiveTypeName.INT64,
            LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS));
      case BINARY:
        return shreddedPrimitive(PrimitiveType.PrimitiveTypeName.BINARY);
      case STRING:
        return shreddedPrimitive(
            PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());
      case TIME:
        return shreddedPrimitive(
            PrimitiveType.PrimitiveTypeName.INT64,
            LogicalTypeAnnotation.timeType(false, LogicalTypeAnnotation.TimeUnit.MICROS));
      case TIMESTAMPTZ_NANOS:
        return shreddedPrimitive(
            PrimitiveType.PrimitiveTypeName.INT64,
            LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS));
      case TIMESTAMPNTZ_NANOS:
        return shreddedPrimitive(
            PrimitiveType.PrimitiveTypeName.INT64,
            LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS));
      case UUID:
        return shreddedPrimitive(
            PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
            LogicalTypeAnnotation.uuidType(),
            16);
    }

    throw new UnsupportedOperationException("Unsupported shredding type: " + primitive.type());
  }

  static GroupType objectFields(List<GroupType> fields) {
    Types.GroupBuilder<GroupType> builder = Types.buildGroup(Type.Repetition.OPTIONAL);
    for (GroupType field : fields) {
      checkField(field);
      builder.addField(field);
    }

    return builder.named("typed_value");
  }

  static void checkField(GroupType fieldType) {
    Preconditions.checkArgument(
        fieldType.isRepetition(Type.Repetition.REQUIRED),
        "Invalid field type repetition: %s should be REQUIRED",
        fieldType.getRepetition());
  }

  static GroupType field(String name, Type shreddedType) {
    Types.GroupBuilder<GroupType> builder =
        Types.buildGroup(Type.Repetition.REQUIRED)
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .named("value");

    if (shreddedType != null) {
      checkShreddedType(shreddedType);
      builder.addField(shreddedType);
    }

    return builder.named(name);
  }

  static void checkShreddedType(Type shreddedType) {
    Preconditions.checkArgument(
        shreddedType.getName().equals("typed_value"),
        "Invalid shredded type name: %s should be typed_value",
        shreddedType.getName());
    Preconditions.checkArgument(
        shreddedType.isRepetition(Type.Repetition.OPTIONAL),
        "Invalid shredded type repetition: %s should be OPTIONAL",
        shreddedType.getRepetition());
  }

  static Type shreddedPrimitive(PrimitiveType.PrimitiveTypeName primitive) {
    return Types.optional(primitive).named("typed_value");
  }

  static Type shreddedPrimitive(
      PrimitiveType.PrimitiveTypeName primitive, LogicalTypeAnnotation annotation) {
    return Types.optional(primitive).as(annotation).named("typed_value");
  }

  static Type shreddedPrimitive(
      PrimitiveType.PrimitiveTypeName primitive, LogicalTypeAnnotation annotation, int length) {
    return Types.optional(primitive).as(annotation).length(length).named("typed_value");
  }
}
