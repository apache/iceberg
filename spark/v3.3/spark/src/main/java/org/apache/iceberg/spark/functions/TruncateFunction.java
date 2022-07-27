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
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.TruncateUtil;
import org.apache.iceberg.util.UnicodeUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.CharType;
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
import org.apache.spark.sql.types.VarcharType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Implementation of {@link UnboundFunction} that matches the <b>truncate</b> transformation. This
 * unbound function is registered with the {@link org.apache.iceberg.spark.SparkCatalog} such that
 * the function can be used as {@code truncate(width, col)} or {@code truncate(2, col)}.
 *
 * <p>Specific {@link BoundFunction} implementations are resolved based on their input types. As
 * with transforms, the truncation width must be non-negative.
 *
 * <p>For efficiency in generated code, the {@code width} is not validated. <b>It is the
 * responsibility of calling code of these functions to not call truncate with a non-positive
 * width.</b>
 */
public class TruncateFunction implements UnboundFunction {
  private static final List<DataType> truncateableAtomicTypes =
      ImmutableList.of(
          DataTypes.ByteType,
          DataTypes.ShortType,
          DataTypes.IntegerType,
          DataTypes.LongType,
          DataTypes.StringType,
          DataTypes.BinaryType);

  private static void validateTruncationFieldType(DataType dt) {
    if (truncateableAtomicTypes.stream().noneMatch(type -> type.sameType(dt))
        && !(dt instanceof DecimalType)) {
      String expectedTypes =
          "[ByteType, ShortType, IntegerType, LongType, StringType, BinaryType, DecimalType]";
      throw new UnsupportedOperationException(
          String.format(
              "Invalid input type to truncate. Expected one of %s, but found %s",
              expectedTypes, dt));
    }
  }

  private static void validateTruncationWidthType(DataType widthType) {
    if (!DataTypes.IntegerType.sameType(widthType)
        && !DataTypes.ShortType.sameType(widthType)
        && !DataTypes.ByteType.sameType(widthType)) {
      throw new UnsupportedOperationException(
          "Expected truncation width to be one of [ByteType, ShortType, IntegerType], but found "
              + widthType);
    }
  }

  @Override
  public BoundFunction bind(StructType inputType) {
    if (inputType.fields().length != 2) {
      throw new UnsupportedOperationException(
          String.format(
              "Invalid input type. Expected 2 fields but found %s", inputType.fields().length));
    }

    StructField widthField = inputType.apply(0);
    StructField toTruncateField = inputType.apply(1);

    validateTruncationFieldType(toTruncateField.dataType());
    validateTruncationWidthType(widthField.dataType());

    DataType toTruncateDataType = toTruncateField.dataType();
    if (toTruncateDataType instanceof ByteType) {
      return new TruncateTinyInt();
    } else if (toTruncateDataType instanceof ShortType) {
      return new TruncateSmallInt();
    } else if (toTruncateDataType instanceof IntegerType) {
      return new TruncateInt();
    } else if (toTruncateDataType instanceof LongType) {
      return new TruncateBigInt();
    } else if (toTruncateDataType instanceof DecimalType) {
      return new TruncateDecimal(
          ((DecimalType) toTruncateDataType).precision(),
          ((DecimalType) toTruncateDataType).scale());
    } else if (toTruncateDataType instanceof StringType
        || toTruncateDataType instanceof VarcharType
        || toTruncateDataType instanceof CharType) {
      return new TruncateString();
    } else if (toTruncateDataType instanceof BinaryType) {
      return new TruncateBinary();
    } else {
      throw new UnsupportedOperationException("Cannot truncate type: " + toTruncateDataType);
    }
  }

  @Override
  public String description() {
    return "Truncate - The Iceberg truncate function used for truncate partition transformations.\n"
        + "\tCalled with the truncation width as the first argument: e.g. system.truncate(width, col)";
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
      return "org.apache.iceberg.spark.functions.truncate[width](tinyint)";
    }

    @Override
    public Byte produceResult(InternalRow input) {
      Integer width = readAndValidateWidth(input);

      Byte toTruncate = !input.isNullAt(1) ? input.getByte(1) : null;
      return toTruncate != null ? invoke(width, toTruncate) : null;
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
      return "org.apache.iceberg.spark.functions.truncate[width](smallint)";
    }

    @Override
    public Short produceResult(InternalRow input) {
      Integer width = readAndValidateWidth(input);

      Short toTruncate = !input.isNullAt(1) ? input.getShort(1) : null;
      return toTruncate != null ? invoke(width, toTruncate) : null;
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
      return "org.apache.iceberg.spark.functions.truncate[width](int)";
    }

    @Override
    public Integer produceResult(InternalRow input) {
      Integer width = readAndValidateWidth(input);

      Integer toTruncate = !input.isNullAt(1) ? input.getInt(1) : null;
      return toTruncate != null ? invoke(width, toTruncate) : null;
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
      return "org.apache.iceberg.spark.functions.truncate[width](bigint)";
    }

    @Override
    public Long produceResult(InternalRow input) {
      Integer width = readAndValidateWidth(input);

      Long toTruncate = !input.isNullAt(1) ? input.getLong(1) : null;
      return toTruncate != null ? invoke(width, toTruncate) : null;
    }
  }

  public static class TruncateString extends TruncateBase<String> {
    // magic function for usage with codegen
    // todo - this can be made more efficient but first keep the implementation the same.
    public static UTF8String invoke(int width, UTF8String value) {
      if (value == null) {
        return null;
      }

      ByteBuffer bb = value.getByteBuffer();
      CharSequence charSequence = StandardCharsets.UTF_8.decode(bb);
      CharSequence truncated = UnicodeUtil.truncateString(charSequence, width);
      ByteBuffer truncatedBytes = StandardCharsets.UTF_8.encode(CharBuffer.wrap(truncated));
      return UTF8String.fromBytes(ByteBuffers.toByteArray(truncatedBytes));
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
      return "org.apache.iceberg.spark.functions.truncate[width](string)";
    }

    @Override
    public String produceResult(InternalRow input) {
      Integer width = readAndValidateWidth(input);

      UTF8String toTruncate = !input.isNullAt(1) ? input.getUTF8String(1) : null;
      UTF8String result = toTruncate != null ? invoke(width, toTruncate) : null;
      return result != null ? result.toString() : null;
    }
  }

  public static class TruncateBinary extends TruncateBase<byte[]> {
    // magic method used in codegen
    public static byte[] invoke(int width, byte[] value) {
      if (value == null) {
        return null;
      }

      return ByteBuffers.toByteArray(
          TruncateUtil.truncateByteBuffer(width, ByteBuffer.wrap(value)));
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
      return "org.apache.iceberg.spark.functions.truncate[width](binary)";
    }

    @Override
    public byte[] produceResult(InternalRow input) {
      Integer width = readAndValidateWidth(input);

      byte[] toTruncate = !input.isNullAt(1) ? input.getBinary(1) : null;
      return toTruncate != null ? invoke(width, toTruncate) : null;
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
      return String.format(
          "org.apache.iceberg.spark.functions.truncate[width](decimal(%d,%d))", precision, scale);
    }

    @Override
    public Decimal produceResult(InternalRow input) {
      Integer width = readAndValidateWidth(input);

      Decimal toTruncate = !input.isNullAt(1) ? input.getDecimal(1, precision, scale) : null;
      return toTruncate != null ? invoke(width, toTruncate) : null;
    }
  }

  private static Integer readAndValidateWidth(InternalRow input) {
    Integer width = !input.isNullAt(0) ? input.getInt(0) : null;
    if (width == null) {
      throw new IllegalArgumentException("Invalid truncation width: null");
    }

    if (width <= 0) {
      throw new IllegalArgumentException(
          String.format("Invalid truncate width: %s (must be > 0)", width));
    }

    return width;
  }
}
