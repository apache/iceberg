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

package org.apache.iceberg.arrow;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.function.IntConsumer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.iceberg.arrow.vectorized.ArrowVectorAccessor;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * This converts dictionary encoded arrow vectors to a correctly typed arrow vector.
 */
public final class DictEncodedArrowConverter {

  private DictEncodedArrowConverter() {
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public static FieldVector allocateNonDictEncodedArrowVector(
      Types.NestedField icebergField, int size, BufferAllocator allocator) {
    Preconditions.checkArgument(null != icebergField, "icebergField cannot be null");
    Preconditions.checkArgument(null != allocator, "BufferAllocator cannot be null");
    if (Type.TypeID.DECIMAL.equals(icebergField.type().typeId())) {
      return allocateDecimalVector(icebergField, size, allocator);
    } else if (Type.TypeID.TIMESTAMP.equals(icebergField.type().typeId())) {
      return allocateTimestampVector(icebergField, size, allocator);
    } else if (Type.TypeID.LONG.equals(icebergField.type().typeId())) {
      return allocateBigIntVector(icebergField, size, allocator);
    } else if (Type.TypeID.FLOAT.equals(icebergField.type().typeId())) {
      return allocateFloat4Vector(icebergField, size, allocator);
    } else if (Type.TypeID.DOUBLE.equals(icebergField.type().typeId())) {
      return allocateFloat8Vector(icebergField, size, allocator);
    } else if (Type.TypeID.STRING.equals(icebergField.type().typeId())) {
      return allocateVarCharVector(icebergField, size, allocator);
    } else if (Type.TypeID.BINARY.equals(icebergField.type().typeId())) {
      return allocateVarBinaryVector(icebergField, size, allocator);
    } else if (Type.TypeID.TIME.equals(icebergField.type().typeId())) {
      return allocateTimeMicroVector(icebergField, size, allocator);
    }

    throw new IllegalArgumentException(String.format(
        "Cannot convert dict encoded field '%s' of type '%s' to Arrow vector as it is currently not supported",
        icebergField.name(), icebergField.type().typeId()));
  }

  private static DecimalVector allocateDecimalVector(
      Types.NestedField icebergField, int size, BufferAllocator allocator) {
    DecimalVector vector = new DecimalVector(
        icebergField.name(),
        ArrowSchemaUtil.convert(icebergField).getFieldType(),
        allocator);
    vector.allocateNew(size);
    return vector;
  }

  private static TimeStampVector allocateTimestampVector(
      Types.NestedField icebergField, int size, BufferAllocator allocator) {
    TimeStampVector vector;
    if (((Types.TimestampType) icebergField.type()).shouldAdjustToUTC()) {
      vector = new TimeStampMicroTZVector(
          icebergField.name(),
          ArrowSchemaUtil.convert(icebergField).getFieldType(),
          allocator);
    } else {
      vector = new TimeStampMicroVector(
          icebergField.name(),
          ArrowSchemaUtil.convert(icebergField).getFieldType(),
          allocator);
    }
    vector.allocateNew(size);
    return vector;
  }

  private static BigIntVector allocateBigIntVector(
      Types.NestedField icebergField, int size, BufferAllocator allocator) {
    BigIntVector vector =
        new BigIntVector(icebergField.name(), ArrowSchemaUtil.convert(icebergField).getFieldType(), allocator);
    vector.allocateNew(size);
    return vector;
  }

  private static Float4Vector allocateFloat4Vector(
      Types.NestedField icebergField, int size, BufferAllocator allocator) {
    Float4Vector vector = new Float4Vector(
        icebergField.name(),
        ArrowSchemaUtil.convert(icebergField).getFieldType(),
        allocator);
    vector.allocateNew(size);
    return vector;
  }

  private static Float8Vector allocateFloat8Vector(
      Types.NestedField icebergField, int size, BufferAllocator allocator) {
    Float8Vector vector = new Float8Vector(
        icebergField.name(),
        ArrowSchemaUtil.convert(icebergField).getFieldType(),
        allocator);
    vector.allocateNew(size);
    return vector;
  }

  private static VarCharVector allocateVarCharVector(
      Types.NestedField icebergField, int size, BufferAllocator allocator) {
    VarCharVector vector = new VarCharVector(
        icebergField.name(),
        ArrowSchemaUtil.convert(icebergField).getFieldType(),
        allocator);
    vector.allocateNew(size);
    return vector;
  }

  private static VarBinaryVector allocateVarBinaryVector(
      Types.NestedField icebergField, int size, BufferAllocator allocator) {
    VarBinaryVector vector = new VarBinaryVector(
        icebergField.name(),
        ArrowSchemaUtil.convert(icebergField).getFieldType(),
        allocator);
    vector.allocateNew(size);
    return vector;
  }

  private static TimeMicroVector allocateTimeMicroVector(
      Types.NestedField icebergField, int size, BufferAllocator allocator) {
    TimeMicroVector vector = new TimeMicroVector(
        icebergField.name(),
        ArrowSchemaUtil.convert(icebergField).getFieldType(),
        allocator);
    vector.allocateNew(size);
    return vector;
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public static FieldVector copyToNonDictEncodedVector(
      FieldVector source, FieldVector target, Types.NestedField icebergField, NullabilityHolder nullabilityHolder,
      ArrowVectorAccessor<?, String, ?, ?> accessor) {
    Preconditions.checkArgument(null != source, "Source FieldVector cannot be null");
    Preconditions.checkArgument(null != target, "Target FieldVector cannot be null");
    Preconditions.checkArgument(null != nullabilityHolder, "NullabilityHolder cannot be null");
    Preconditions.checkArgument(null != accessor, "ArrowVectorAccessor cannot be null");

    if (Type.TypeID.DECIMAL.equals(icebergField.type().typeId())) {
      return copyToDecimalVector(source, target, icebergField, nullabilityHolder, accessor);
    } else if (Type.TypeID.TIMESTAMP.equals(icebergField.type().typeId())) {
      return copyToTimestampVector(source, target, nullabilityHolder, accessor);
    } else if (Type.TypeID.LONG.equals(icebergField.type().typeId())) {
      return copyToBigIntVector(source, target, nullabilityHolder, accessor);
    } else if (Type.TypeID.FLOAT.equals(icebergField.type().typeId())) {
      return copyToFloat4Vector(source, target, nullabilityHolder, accessor);
    } else if (Type.TypeID.DOUBLE.equals(icebergField.type().typeId())) {
      return copyToFloat8Vector(source, target, nullabilityHolder, accessor);
    } else if (Type.TypeID.STRING.equals(icebergField.type().typeId())) {
      return copyToVarCharVector(source, target, nullabilityHolder, accessor);
    } else if (Type.TypeID.BINARY.equals(icebergField.type().typeId())) {
      return copyToVarBinaryVector(source, target, nullabilityHolder, accessor);
    } else if (Type.TypeID.TIME.equals(icebergField.type().typeId())) {
      return copyToTimeMicroVector(source, target, nullabilityHolder, accessor);
    }

    throw new IllegalArgumentException(String.format("Cannot convert dict encoded field '%s' of type '%s' to Arrow " +
            "vector as it is currently not supported",
        icebergField.name(), icebergField.type().typeId()));
  }

  private static boolean isNullAt(NullabilityHolder nullabilityHolder, int idx) {
    return nullabilityHolder.isNullAt(idx) == 1;
  }

  private static DecimalVector copyToDecimalVector(
      FieldVector source, FieldVector target, Types.NestedField icebergField, NullabilityHolder nullabilityHolder,
      ArrowVectorAccessor<?, String, ?, ?> accessor) {
    Preconditions.checkArgument(target instanceof DecimalVector, "ValueVector must be of type " +
        DecimalVector.class + " but is " + target);
    DecimalVector vector = (DecimalVector) target;
    int precision = ((Types.DecimalType) icebergField.type()).precision();
    int scale = ((Types.DecimalType) icebergField.type()).scale();

    initVector(source, vector, nullabilityHolder, idx -> vector.setSafe(idx, (BigDecimal) accessor.getDecimal(idx,
        precision, scale)));
    return vector;
  }

  private static TimeStampVector copyToTimestampVector(
      FieldVector source, FieldVector target, NullabilityHolder nullabilityHolder,
      ArrowVectorAccessor<?, String, ?, ?> accessor) {
    Preconditions.checkArgument(target instanceof TimeStampVector, "ValueVector must be of type " +
        TimeStampVector.class + " but is " + target);
    TimeStampVector vector = (TimeStampVector) target;
    initVector(source, vector, nullabilityHolder, idx -> vector.set(idx, accessor.getLong(idx)));
    return vector;
  }

  private static BigIntVector copyToBigIntVector(
      FieldVector source, FieldVector target, NullabilityHolder nullabilityHolder,
      ArrowVectorAccessor<?, String, ?, ?> accessor) {
    Preconditions.checkArgument(target instanceof BigIntVector, "ValueVector must be of type " +
        BigIntVector.class + " but is " + target);
    BigIntVector vector = (BigIntVector) target;
    initVector(source, vector, nullabilityHolder, idx -> vector.set(idx, accessor.getLong(idx)));
    return vector;
  }

  private static Float4Vector copyToFloat4Vector(
      FieldVector source, FieldVector target, NullabilityHolder nullabilityHolder,
      ArrowVectorAccessor<?, String, ?, ?> accessor) {
    Preconditions.checkArgument(target instanceof Float4Vector, "ValueVector must be of type " +
        Float4Vector.class + " but is " + target);
    Float4Vector vector = (Float4Vector) target;
    initVector(source, vector, nullabilityHolder, idx -> vector.set(idx, accessor.getFloat(idx)));
    return vector;
  }

  private static Float8Vector copyToFloat8Vector(
      FieldVector source, FieldVector target, NullabilityHolder nullabilityHolder,
      ArrowVectorAccessor<?, String, ?, ?> accessor) {
    Preconditions.checkArgument(target instanceof Float8Vector, "ValueVector must be of type " +
        Float8Vector.class + " but is " + target);
    Float8Vector vector = (Float8Vector) target;
    initVector(source, vector, nullabilityHolder, idx -> vector.set(idx, accessor.getDouble(idx)));
    return vector;
  }

  private static VarCharVector copyToVarCharVector(
      FieldVector source, FieldVector target, NullabilityHolder nullabilityHolder,
      ArrowVectorAccessor<?, String, ?, ?> accessor) {
    Preconditions.checkArgument(target instanceof VarCharVector, "ValueVector must be of type " +
        VarCharVector.class + " but is " + target);
    VarCharVector vector = (VarCharVector) target;

    initVector(
        source, vector, nullabilityHolder,
        idx -> vector.set(idx, accessor.getUTF8String(idx).getBytes(StandardCharsets.UTF_8)));
    return vector;
  }

  private static VarBinaryVector copyToVarBinaryVector(
      FieldVector source, FieldVector target, NullabilityHolder nullabilityHolder,
      ArrowVectorAccessor<?, String, ?, ?> accessor) {
    Preconditions.checkArgument(target instanceof VarBinaryVector, "ValueVector must be of type " +
        VarBinaryVector.class + " but is " + target);
    VarBinaryVector vector = (VarBinaryVector) target;

    initVector(source, vector, nullabilityHolder, idx -> vector.set(idx, accessor.getBinary(idx)));
    return vector;
  }

  private static TimeMicroVector copyToTimeMicroVector(
      FieldVector source, FieldVector target, NullabilityHolder nullabilityHolder,
      ArrowVectorAccessor<?, String, ?, ?> accessor) {
    Preconditions.checkArgument(target instanceof TimeMicroVector, "ValueVector must be of type " +
        TimeMicroVector.class + " but is " + target);
    TimeMicroVector vector = (TimeMicroVector) target;
    initVector(source, vector, nullabilityHolder, idx -> vector.set(idx, accessor.getLong(idx)));
    return vector;
  }

  private static void initVector(
      FieldVector source, BaseFixedWidthVector target, NullabilityHolder nullabilityHolder,
      IntConsumer consumer) {
    for (int i = 0; i < source.getValueCount(); i++) {
      if (isNullAt(nullabilityHolder, i)) {
        target.setNull(i);
      } else {
        consumer.accept(i);
      }
    }
    target.setValueCount(source.getValueCount());
  }

  private static void initVector(
      FieldVector source, BaseVariableWidthVector target,
      NullabilityHolder nullabilityHolder, IntConsumer consumer) {
    for (int i = 0; i < source.getValueCount(); i++) {
      if (isNullAt(nullabilityHolder, i)) {
        target.setNull(i);
      } else {
        consumer.accept(i);
      }
    }
    target.setValueCount(source.getValueCount());
  }
}
