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
package org.apache.iceberg.spark.functions;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.TruncateUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A Spark function implementation for the Iceberg truncate transform.
 *
 * <p>Example usage: {@code SELECT system.truncate(1, 'abc')}, which returns the String 'a'.
 *
 * <p>Note that for performance reasons, the given input width is not validated in the
 * implementations used in code-gen. The width must remain non-negative to give meaningful results.
 */
public class TruncateFunction implements UnboundFunction {

  private static final int WIDTH_ORDINAL = 0;
  private static final int VALUE_ORDINAL = 1;

  private static final Set<DataType> SUPPORTED_WIDTH_TYPES =
      ImmutableSet.of(DataTypes.ByteType, DataTypes.ShortType, DataTypes.IntegerType);

  @Override
  public BoundFunction bind(StructType inputType) {
    if (inputType.size() != 2) {
      throw new UnsupportedOperationException("Wrong number of inputs (expected width and value)");
    }

    StructField widthField = inputType.fields()[WIDTH_ORDINAL];
    StructField valueField = inputType.fields()[VALUE_ORDINAL];

    if (!SUPPORTED_WIDTH_TYPES.contains(widthField.dataType())) {
      throw new UnsupportedOperationException(
          "Expected truncation width to be tinyint, shortint or int");
    }

    DataType valueType = valueField.dataType();
    if (valueType instanceof ByteType) {
      return new TruncateTinyInt();
    } else if (valueType instanceof ShortType) {
      return new TruncateSmallInt();
    } else if (valueType instanceof IntegerType) {
      return new TruncateInt();
    } else if (valueType instanceof LongType) {
      return new TruncateBigInt();
    } else if (valueType instanceof DecimalType) {
      return new TruncateDecimal(
          ((DecimalType) valueType).precision(), ((DecimalType) valueType).scale());
    } else if (valueType instanceof StringType) {
      return new TruncateString();
    } else if (valueType instanceof BinaryType) {
      return new TruncateBinary();
    } else {
      throw new UnsupportedOperationException(
          "Expected truncation col to be tinyint, shortint, int, bigint, decimal, string, or binary");
    }
  }

  @Override
  public String description() {
    return name()
        + "(width, col) - Call Iceberg's truncate transform\n"
        + "  width :: width for truncation, e.g. truncate(10, 255) -> 250 (must be an integer)\n"
        + "  col :: column to truncate (must be an integer, decimal, string, or binary)";
  }

  @Override
  public String name() {
    return "truncate";
  }

  public abstract static class TruncateBase<T> implements ScalarFunction<T> {
    @Override
    public String name() {
      return "truncate";
    }
  }

  public static class TruncateTinyInt extends TruncateBase<Byte> {
    public static byte invoke(int width, byte value) {
      return TruncateUtil.truncateByte(width, value);
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {DataTypes.IntegerType, DataTypes.ByteType};
    }

    @Override
    public DataType resultType() {
      return DataTypes.ByteType;
    }

    @Override
    public String canonicalName() {
      return "iceberg.truncate(tinyint)";
    }

    @Override
    public Byte produceResult(InternalRow input) {
      if (input.isNullAt(WIDTH_ORDINAL) || input.isNullAt(VALUE_ORDINAL)) {
        return null;
      } else {
        return invoke(input.getInt(WIDTH_ORDINAL), input.getByte(VALUE_ORDINAL));
      }
    }
  }

  public static class TruncateSmallInt extends TruncateBase<Short> {
    // magic method used in codegen
    public static short invoke(int width, short value) {
      return TruncateUtil.truncateShort(width, value);
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {DataTypes.IntegerType, DataTypes.ShortType};
    }

    @Override
    public DataType resultType() {
      return DataTypes.ShortType;
    }

    @Override
    public String canonicalName() {
      return "iceberg.truncate(smallint)";
    }

    @Override
    public Short produceResult(InternalRow input) {
      if (input.isNullAt(WIDTH_ORDINAL) || input.isNullAt(VALUE_ORDINAL)) {
        return null;
      } else {
        return invoke(input.getInt(WIDTH_ORDINAL), input.getShort(VALUE_ORDINAL));
      }
    }
  }

  public static class TruncateInt extends TruncateBase<Integer> {
    // magic method used in codegen
    public static int invoke(int width, int value) {
      return TruncateUtil.truncateInt(width, value);
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {DataTypes.IntegerType, DataTypes.IntegerType};
    }

    @Override
    public DataType resultType() {
      return DataTypes.IntegerType;
    }

    @Override
    public String canonicalName() {
      return "iceberg.truncate(int)";
    }

    @Override
    public Integer produceResult(InternalRow input) {
      if (input.isNullAt(WIDTH_ORDINAL) || input.isNullAt(VALUE_ORDINAL)) {
        return null;
      } else {
        return invoke(input.getInt(WIDTH_ORDINAL), input.getInt(VALUE_ORDINAL));
      }
    }
  }

  public static class TruncateBigInt extends TruncateBase<Long> {
    // magic function for usage with codegen
    public static long invoke(int width, long value) {
      return TruncateUtil.truncateLong(width, value);
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {DataTypes.IntegerType, DataTypes.LongType};
    }

    @Override
    public DataType resultType() {
      return DataTypes.LongType;
    }

    @Override
    public String canonicalName() {
      return "iceberg.truncate(bigint)";
    }

    @Override
    public Long produceResult(InternalRow input) {
      if (input.isNullAt(WIDTH_ORDINAL) || input.isNullAt(VALUE_ORDINAL)) {
        return null;
      } else {
        return invoke(input.getInt(WIDTH_ORDINAL), input.getLong(VALUE_ORDINAL));
      }
    }
  }

  public static class TruncateString extends TruncateBase<UTF8String> {
    // magic function for usage with codegen
    public static UTF8String invoke(int width, UTF8String value) {
      if (value == null) {
        return null;
      }

      return value.substring(0, width);
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {DataTypes.IntegerType, DataTypes.StringType};
    }

    @Override
    public DataType resultType() {
      return DataTypes.StringType;
    }

    @Override
    public String canonicalName() {
      return "iceberg.truncate(string)";
    }

    @Override
    public UTF8String produceResult(InternalRow input) {
      if (input.isNullAt(WIDTH_ORDINAL) || input.isNullAt(VALUE_ORDINAL)) {
        return null;
      } else {
        return invoke(input.getInt(WIDTH_ORDINAL), input.getUTF8String(VALUE_ORDINAL));
      }
    }
  }

  public static class TruncateBinary extends TruncateBase<byte[]> {
    // magic method used in codegen
    public static byte[] invoke(int width, byte[] value) {
      if (value == null) {
        return null;
      }

      return ByteBuffers.toByteArray(
          BinaryUtil.truncateBinaryUnsafe(ByteBuffer.wrap(value), width));
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {DataTypes.IntegerType, DataTypes.BinaryType};
    }

    @Override
    public DataType resultType() {
      return DataTypes.BinaryType;
    }

    @Override
    public String canonicalName() {
      return "iceberg.truncate(binary)";
    }

    @Override
    public byte[] produceResult(InternalRow input) {
      if (input.isNullAt(WIDTH_ORDINAL) || input.isNullAt(VALUE_ORDINAL)) {
        return null;
      } else {
        return invoke(input.getInt(WIDTH_ORDINAL), input.getBinary(VALUE_ORDINAL));
      }
    }
  }

  public static class TruncateDecimal extends TruncateBase<Decimal> {
    private final int precision;
    private final int scale;

    public TruncateDecimal(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    // magic method used in codegen
    public static Decimal invoke(int width, Decimal value) {
      if (value == null) {
        return null;
      }

      return Decimal.apply(
          TruncateUtil.truncateDecimal(BigInteger.valueOf(width), value.toJavaBigDecimal()));
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {DataTypes.IntegerType, DataTypes.createDecimalType(precision, scale)};
    }

    @Override
    public DataType resultType() {
      return DataTypes.createDecimalType(precision, scale);
    }

    @Override
    public String canonicalName() {
      return String.format("iceberg.truncate(decimal(%d,%d))", precision, scale);
    }

    @Override
    public Decimal produceResult(InternalRow input) {
      if (input.isNullAt(WIDTH_ORDINAL) || input.isNullAt(VALUE_ORDINAL)) {
        return null;
      } else {
        int width = input.getInt(WIDTH_ORDINAL);
        Decimal value = input.getDecimal(VALUE_ORDINAL, precision, scale);
        return invoke(width, value);
      }
    }
  }
}
