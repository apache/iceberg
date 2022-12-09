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
package org.apache.iceberg.spark.actions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampType;
import org.davidmoten.hilbert.HilbertCurve;
import scala.collection.JavaConverters;
import scala.collection.Seq;

class SparkHilbertUDF implements SparkSpaceCurveUDF, Serializable {
  private static final long PRIMITIVE_EMPTY = Long.MAX_VALUE;

  private static final int BITS_NUM = 63;

  SparkHilbertUDF() {}

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
  }

  byte[] hilbertCurvePosBytes(Seq<Long> points) {
    java.util.List<Long> longs = JavaConverters.seqAsJavaList(points);
    long[] longs1 = new long[longs.size()];
    for (int i = 0; i < longs.size(); i++) {
      longs1[i] = longs.get(i);
    }
    HilbertCurve hilbertCurve = HilbertCurve.bits(BITS_NUM).dimensions(points.size());
    BigInteger index = hilbertCurve.index(longs1);
    return BinaryUtil.paddingToNByte(index.toByteArray(), BITS_NUM);
  }

  private UserDefinedFunction tinyToOrderedLongUDF() {
    UserDefinedFunction udf =
        functions
            .udf(
                (Byte value) -> {
                  if (value == null) {
                    return PRIMITIVE_EMPTY;
                  }
                  return BinaryUtil.convertBytesToLong(new byte[] {value});
                },
                DataTypes.LongType)
            .withName("TINY_ORDERED_BYTES");

    return udf;
  }

  private UserDefinedFunction shortToOrderedLongUDF() {
    UserDefinedFunction udf =
        functions
            .udf(
                (Short value) -> {
                  if (value == null) {
                    return PRIMITIVE_EMPTY;
                  }
                  return (long) value;
                },
                DataTypes.LongType)
            .withName("SHORT_ORDERED_BYTES");

    return udf;
  }

  private UserDefinedFunction intToOrderedLongUDF() {
    UserDefinedFunction udf =
        functions
            .udf(
                (Integer value) -> {
                  if (value == null) {
                    return PRIMITIVE_EMPTY;
                  }
                  return (long) value;
                },
                DataTypes.LongType)
            .withName("INT_ORDERED_BYTES");

    return udf;
  }

  private UserDefinedFunction longToOrderedLongUDF() {
    UserDefinedFunction udf =
        functions
            .udf(
                (Long value) -> {
                  if (value == null) {
                    return PRIMITIVE_EMPTY;
                  }
                  return value;
                },
                DataTypes.LongType)
            .withName("LONG_ORDERED_BYTES");

    return udf;
  }

  private UserDefinedFunction floatToOrderedLongUDF() {
    UserDefinedFunction udf =
        functions
            .udf(
                (Float value) -> {
                  if (value == null) {
                    return PRIMITIVE_EMPTY;
                  }
                  return Double.doubleToLongBits((double) value);
                },
                DataTypes.LongType)
            .withName("FLOAT_ORDERED_BYTES");

    return udf;
  }

  private UserDefinedFunction doubleToOrderedLongUDF() {
    UserDefinedFunction udf =
        functions
            .udf(
                (Double value) -> {
                  if (value == null) {
                    return PRIMITIVE_EMPTY;
                  }
                  return Double.doubleToLongBits(value);
                },
                DataTypes.LongType)
            .withName("DOUBLE_ORDERED_BYTES");

    return udf;
  }

  private UserDefinedFunction booleanToOrderedLongUDF() {
    UserDefinedFunction udf =
        functions
            .udf((Boolean value) -> value ? PRIMITIVE_EMPTY : 0, DataTypes.LongType)
            .withName("BOOLEAN-LEXICAL-BYTES");
    return udf;
  }

  private UserDefinedFunction stringToOrderedLongUDF() {
    UserDefinedFunction udf =
        functions
            .udf((String value) -> BinaryUtil.convertStringToLong(value), DataTypes.LongType)
            .withName("STRING-LEXICAL-BYTES");

    return udf;
  }

  private UserDefinedFunction bytesTruncateUDF() {
    UserDefinedFunction udf =
        functions
            .udf((byte[] value) -> BinaryUtil.convertBytesToLong(value), DataTypes.LongType)
            .withName("BYTE-TRUNCATE");

    return udf;
  }

  private UserDefinedFunction decimalTypeToOrderedLongUDF() {
    UserDefinedFunction udf =
        functions
            .udf((BigDecimal value) -> value.longValue(), DataTypes.LongType)
            .withName("BYTE-TRUNCATE");

    return udf;
  }

  @Override
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public Column sortedLexicographically(Column column, DataType type) {
    if (type instanceof ByteType) {
      return tinyToOrderedLongUDF().apply(column);
    } else if (type instanceof ShortType) {
      return shortToOrderedLongUDF().apply(column);
    } else if (type instanceof IntegerType) {
      return intToOrderedLongUDF().apply(column);
    } else if (type instanceof LongType) {
      return longToOrderedLongUDF().apply(column);
    } else if (type instanceof FloatType) {
      return floatToOrderedLongUDF().apply(column);
    } else if (type instanceof DoubleType) {
      return doubleToOrderedLongUDF().apply(column);
    } else if (type instanceof StringType) {
      return stringToOrderedLongUDF().apply(column);
    } else if (type instanceof BinaryType) {
      return bytesTruncateUDF().apply(column);
    } else if (type instanceof BooleanType) {
      return booleanToOrderedLongUDF().apply(column);
    } else if (type instanceof TimestampType) {
      return longToOrderedLongUDF().apply(column.cast(DataTypes.LongType));
    } else if (type instanceof DecimalType) {
      return decimalTypeToOrderedLongUDF().apply(column.cast(DataTypes.LongType));
    } else if (type instanceof DateType) {
      return longToOrderedLongUDF().apply(column.cast(DataTypes.LongType));
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Cannot use column %s of type %s in ZOrdering, the type is unsupported",
              column, type));
    }
  }

  private final UserDefinedFunction hilbertCurveUDF =
      functions
          .udf((Seq<Long> points) -> hilbertCurvePosBytes(points), DataTypes.BinaryType)
          .withName("HILBERT_LONG");

  @Override
  public Column transform(Column arrayBinary) {
    return hilbertCurveUDF.apply(arrayBinary);
  }
}
