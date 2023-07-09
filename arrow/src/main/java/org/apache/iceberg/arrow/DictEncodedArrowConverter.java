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
import org.apache.iceberg.arrow.vectorized.VectorHolder;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** This converts dictionary encoded arrow vectors to a correctly typed arrow vector. */
public class DictEncodedArrowConverter {

  private DictEncodedArrowConverter() {}

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public static FieldVector toArrowVector(
      VectorHolder vectorHolder, ArrowVectorAccessor<?, String, ?, ?> accessor) {
    Preconditions.checkArgument(null != vectorHolder, "Invalid vector holder: null");
    Preconditions.checkArgument(null != accessor, "Invalid arrow vector accessor: null");

    if (vectorHolder.isDictionaryEncoded()) {
      if (Type.TypeID.DECIMAL.equals(vectorHolder.icebergType().typeId())) {
        return toDecimalVector(vectorHolder, accessor);
      } else if (Type.TypeID.TIMESTAMP.equals(vectorHolder.icebergType().typeId())) {
        return toTimestampVector(vectorHolder, accessor);
      } else if (Type.TypeID.LONG.equals(vectorHolder.icebergType().typeId())) {
        return toBigIntVector(vectorHolder, accessor);
      } else if (Type.TypeID.FLOAT.equals(vectorHolder.icebergType().typeId())) {
        return toFloat4Vector(vectorHolder, accessor);
      } else if (Type.TypeID.DOUBLE.equals(vectorHolder.icebergType().typeId())) {
        return toFloat8Vector(vectorHolder, accessor);
      } else if (Type.TypeID.STRING.equals(vectorHolder.icebergType().typeId())) {
        return toVarCharVector(vectorHolder, accessor);
      } else if (Type.TypeID.BINARY.equals(vectorHolder.icebergType().typeId())) {
        return toVarBinaryVector(vectorHolder, accessor);
      } else if (Type.TypeID.TIME.equals(vectorHolder.icebergType().typeId())) {
        return toTimeMicroVector(vectorHolder, accessor);
      }

      throw new IllegalArgumentException(
          String.format(
              "Cannot convert dict encoded field '%s' of type '%s' to Arrow "
                  + "vector as it is currently not supported",
              vectorHolder.icebergField().name(), vectorHolder.icebergType().typeId()));
    }

    return vectorHolder.vector();
  }

  private static DecimalVector toDecimalVector(
      VectorHolder vectorHolder, ArrowVectorAccessor<?, String, ?, ?> accessor) {
    int precision = ((Types.DecimalType) vectorHolder.icebergType()).precision();
    int scale = ((Types.DecimalType) vectorHolder.icebergType()).scale();

    DecimalVector vector =
        new DecimalVector(
            vectorHolder.vector().getName(),
            ArrowSchemaUtil.convert(vectorHolder.icebergField()).getFieldType(),
            vectorHolder.vector().getAllocator());

    initVector(
        vector,
        vectorHolder,
        idx -> vector.set(idx, (BigDecimal) accessor.getDecimal(idx, precision, scale)));
    return vector;
  }

  private static TimeStampVector toTimestampVector(
      VectorHolder vectorHolder, ArrowVectorAccessor<?, String, ?, ?> accessor) {
    TimeStampVector vector;
    if (((Types.TimestampType) vectorHolder.icebergType()).shouldAdjustToUTC()) {
      vector =
          new TimeStampMicroTZVector(
              vectorHolder.vector().getName(),
              ArrowSchemaUtil.convert(vectorHolder.icebergField()).getFieldType(),
              vectorHolder.vector().getAllocator());
    } else {
      vector =
          new TimeStampMicroVector(
              vectorHolder.vector().getName(),
              ArrowSchemaUtil.convert(vectorHolder.icebergField()).getFieldType(),
              vectorHolder.vector().getAllocator());
    }

    initVector(vector, vectorHolder, idx -> vector.set(idx, accessor.getLong(idx)));
    return vector;
  }

  private static BigIntVector toBigIntVector(
      VectorHolder vectorHolder, ArrowVectorAccessor<?, String, ?, ?> accessor) {
    BigIntVector vector =
        new BigIntVector(
            vectorHolder.vector().getName(),
            ArrowSchemaUtil.convert(vectorHolder.icebergField()).getFieldType(),
            vectorHolder.vector().getAllocator());

    initVector(vector, vectorHolder, idx -> vector.set(idx, accessor.getLong(idx)));
    return vector;
  }

  private static Float4Vector toFloat4Vector(
      VectorHolder vectorHolder, ArrowVectorAccessor<?, String, ?, ?> accessor) {
    Float4Vector vector =
        new Float4Vector(
            vectorHolder.vector().getName(),
            ArrowSchemaUtil.convert(vectorHolder.icebergField()).getFieldType(),
            vectorHolder.vector().getAllocator());

    initVector(vector, vectorHolder, idx -> vector.set(idx, accessor.getFloat(idx)));
    return vector;
  }

  private static Float8Vector toFloat8Vector(
      VectorHolder vectorHolder, ArrowVectorAccessor<?, String, ?, ?> accessor) {
    Float8Vector vector =
        new Float8Vector(
            vectorHolder.vector().getName(),
            ArrowSchemaUtil.convert(vectorHolder.icebergField()).getFieldType(),
            vectorHolder.vector().getAllocator());

    initVector(vector, vectorHolder, idx -> vector.set(idx, accessor.getDouble(idx)));
    return vector;
  }

  private static VarCharVector toVarCharVector(
      VectorHolder vectorHolder, ArrowVectorAccessor<?, String, ?, ?> accessor) {
    VarCharVector vector =
        new VarCharVector(
            vectorHolder.vector().getName(),
            ArrowSchemaUtil.convert(vectorHolder.icebergField()).getFieldType(),
            vectorHolder.vector().getAllocator());

    initVector(
        vector,
        vectorHolder,
        idx -> vector.setSafe(idx, accessor.getUTF8String(idx).getBytes(StandardCharsets.UTF_8)));
    return vector;
  }

  private static VarBinaryVector toVarBinaryVector(
      VectorHolder vectorHolder, ArrowVectorAccessor<?, String, ?, ?> accessor) {
    VarBinaryVector vector =
        new VarBinaryVector(
            vectorHolder.vector().getName(),
            ArrowSchemaUtil.convert(vectorHolder.icebergField()).getFieldType(),
            vectorHolder.vector().getAllocator());

    initVector(vector, vectorHolder, idx -> vector.setSafe(idx, accessor.getBinary(idx)));
    return vector;
  }

  private static TimeMicroVector toTimeMicroVector(
      VectorHolder vectorHolder, ArrowVectorAccessor<?, String, ?, ?> accessor) {
    TimeMicroVector vector =
        new TimeMicroVector(
            vectorHolder.vector().getName(),
            ArrowSchemaUtil.convert(vectorHolder.icebergField()).getFieldType(),
            vectorHolder.vector().getAllocator());

    initVector(vector, vectorHolder, idx -> vector.set(idx, accessor.getLong(idx)));
    return vector;
  }

  private static void initVector(
      BaseFixedWidthVector vector, VectorHolder vectorHolder, IntConsumer consumer) {
    vector.allocateNew(vectorHolder.vector().getValueCount());
    init(vector, vectorHolder, consumer, vectorHolder.vector().getValueCount());
  }

  private static void initVector(
      BaseVariableWidthVector vector, VectorHolder vectorHolder, IntConsumer consumer) {
    vector.allocateNew(vectorHolder.vector().getValueCount());
    init(vector, vectorHolder, consumer, vectorHolder.vector().getValueCount());
  }

  private static void init(
      FieldVector vector, VectorHolder vectorHolder, IntConsumer consumer, int valueCount) {
    for (int i = 0; i < valueCount; i++) {
      if (isNullAt(vectorHolder, i)) {
        vector.setNull(i);
      } else {
        consumer.accept(i);
      }
    }

    vector.setValueCount(valueCount);
  }

  private static boolean isNullAt(VectorHolder vectorHolder, int idx) {
    return vectorHolder.nullabilityHolder().isNullAt(idx) == 1;
  }
}
