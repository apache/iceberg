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

import java.util.Optional;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

abstract class BaseMessageTypeToType extends ParquetTypeVisitor<Type> {

  @Override
  public Type primitive(PrimitiveType primitive) {
    // first, use the logical type annotation, if present
    LogicalTypeAnnotation logicalType = primitive.getLogicalTypeAnnotation();
    if (logicalType != null) {
      Optional<Type> converted = logicalType.accept(ParquetLogicalTypeVisitor.get());
      if (converted.isPresent()) {
        return converted.get();
      }
    }

    // last, use the primitive type
    switch (primitive.getPrimitiveTypeName()) {
      case BOOLEAN:
        return Types.BooleanType.get();
      case INT32:
        return Types.IntegerType.get();
      case INT64:
        return Types.LongType.get();
      case FLOAT:
        return Types.FloatType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case FIXED_LEN_BYTE_ARRAY:
        return Types.FixedType.ofLength(primitive.getTypeLength());
      case INT96:
        return Types.TimestampType.withZone();
      case BINARY:
        return Types.BinaryType.get();
    }

    throw new UnsupportedOperationException(
        "Cannot convert unknown primitive type: " + primitive);
  }

  private static class ParquetLogicalTypeVisitor implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Type> {
    private static final ParquetLogicalTypeVisitor INSTANCE = new ParquetLogicalTypeVisitor();

    private static ParquetLogicalTypeVisitor get() {
      return INSTANCE;
    }

    @Override
    public Optional<Type> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringType) {
      return Optional.of(Types.StringType.get());
    }

    @Override
    public Optional<Type> visit(LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumType) {
      return Optional.of(Types.StringType.get());
    }

    @Override
    public Optional<Type> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType) {
      return Optional.of(Types.DecimalType.of(decimalType.getPrecision(), decimalType.getScale()));
    }

    @Override
    public Optional<Type> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation dateType) {
      return Optional.of(Types.DateType.get());
    }

    @Override
    public Optional<Type> visit(LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeType) {
      return Optional.of(Types.TimeType.get());
    }

    @Override
    public Optional<Type> visit(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampType) {
      return Optional.of(timestampType.isAdjustedToUTC() ? TimestampType.withZone() : TimestampType.withoutZone());
    }

    @Override
    public Optional<Type> visit(LogicalTypeAnnotation.IntLogicalTypeAnnotation intType) {
      Preconditions.checkArgument(intType.isSigned() || intType.getBitWidth() < 64,
          "Cannot use uint64: not a supported Java type");
      if (intType.getBitWidth() < 32) {
        return Optional.of(Types.IntegerType.get());
      } else if (intType.getBitWidth() == 32 && intType.isSigned()) {
        return Optional.of(Types.IntegerType.get());
      } else {
        return Optional.of(Types.LongType.get());
      }
    }

    @Override
    public Optional<Type> visit(LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonType) {
      return Optional.of(Types.StringType.get());
    }

    @Override
    public Optional<Type> visit(LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonType) {
      return Optional.of(Types.BinaryType.get());
    }
  }
}
