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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.util.BucketUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A Spark function implementation for the Iceberg bucket transform.
 *
 * <p>Example usage: {@code SELECT system.bucket(128, 'abc')}, which returns the bucket 122.
 *
 * <p>Note that for performance reasons, the given input number of buckets is not validated in the
 * implementations used in code-gen. The number of buckets must be positive to give meaningful
 * results.
 */
public class BucketFunction implements UnboundFunction {

  private static final int NUM_BUCKETS_ORDINAL = 0;
  private static final int VALUE_ORDINAL = 1;

  private static final Set<DataType> SUPPORTED_NUM_BUCKETS_TYPES =
      ImmutableSet.of(DataTypes.ByteType, DataTypes.ShortType, DataTypes.IntegerType);

  @Override
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public BoundFunction bind(StructType inputType) {
    if (inputType.size() != 2) {
      throw new UnsupportedOperationException(
          "Wrong number of inputs (expected numBuckets and value)");
    }

    StructField numBucketsField = inputType.fields()[NUM_BUCKETS_ORDINAL];
    StructField valueField = inputType.fields()[VALUE_ORDINAL];

    if (!SUPPORTED_NUM_BUCKETS_TYPES.contains(numBucketsField.dataType())) {
      throw new UnsupportedOperationException(
          "Expected number of buckets to be tinyint, shortint or int");
    }

    DataType type = valueField.dataType();
    if (type instanceof DateType) {
      return new BucketInt(type);
    } else if (type instanceof ByteType
        || type instanceof ShortType
        || type instanceof IntegerType) {
      return new BucketInt(DataTypes.IntegerType);
    } else if (type instanceof LongType) {
      return new BucketLong(type);
    } else if (type instanceof TimestampType) {
      return new BucketLong(type);
    } else if (type instanceof TimestampNTZType) {
      return new BucketLong(type);
    } else if (type instanceof DecimalType) {
      return new BucketDecimal(type);
    } else if (type instanceof StringType) {
      return new BucketString();
    } else if (type instanceof BinaryType) {
      return new BucketBinary();
    } else {
      throw new UnsupportedOperationException(
          "Expected column to be date, tinyint, smallint, int, bigint, decimal, timestamp, string, or binary");
    }
  }

  @Override
  public String description() {
    return name()
        + "(numBuckets, col) - Call Iceberg's bucket transform\n"
        + "  numBuckets :: number of buckets to divide the rows into, e.g. bucket(100, 34) -> 79 (must be a tinyint, smallint, or int)\n"
        + "  col :: column to bucket (must be a date, integer, long, timestamp, decimal, string, or binary)";
  }

  @Override
  public String name() {
    return "bucket";
  }

  public abstract static class BucketBase extends BaseScalarFunction<Integer> {
    public static int apply(int numBuckets, int hashedValue) {
      return (hashedValue & Integer.MAX_VALUE) % numBuckets;
    }

    @Override
    public String name() {
      return "bucket";
    }

    @Override
    public DataType resultType() {
      return DataTypes.IntegerType;
    }
  }

  // Used for both int and date - tinyint and smallint are upcasted to int by Spark.
  public static class BucketInt extends BucketBase {
    private final DataType sqlType;

    // magic method used in codegen
    public static int invoke(int numBuckets, int value) {
      return apply(numBuckets, hash(value));
    }

    // Visible for testing
    public static int hash(int value) {
      return BucketUtil.hash(value);
    }

    public BucketInt(DataType sqlType) {
      this.sqlType = sqlType;
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {DataTypes.IntegerType, sqlType};
    }

    @Override
    public String canonicalName() {
      return String.format("iceberg.bucket(%s)", sqlType.catalogString());
    }

    @Override
    public Integer produceResult(InternalRow input) {
      // return null for null input to match what Spark does in the code-generated versions.
      if (input.isNullAt(NUM_BUCKETS_ORDINAL) || input.isNullAt(VALUE_ORDINAL)) {
        return null;
      } else {
        return invoke(input.getInt(NUM_BUCKETS_ORDINAL), input.getInt(VALUE_ORDINAL));
      }
    }
  }

  // Used for both BigInt and Timestamp
  public static class BucketLong extends BucketBase {
    private final DataType sqlType;

    // magic function for usage with codegen - needs to be static
    public static int invoke(int numBuckets, long value) {
      return apply(numBuckets, hash(value));
    }

    // Visible for testing
    public static int hash(long value) {
      return BucketUtil.hash(value);
    }

    public BucketLong(DataType sqlType) {
      this.sqlType = sqlType;
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {DataTypes.IntegerType, sqlType};
    }

    @Override
    public String canonicalName() {
      return String.format("iceberg.bucket(%s)", sqlType.catalogString());
    }

    @Override
    public Integer produceResult(InternalRow input) {
      if (input.isNullAt(NUM_BUCKETS_ORDINAL) || input.isNullAt(VALUE_ORDINAL)) {
        return null;
      } else {
        return invoke(input.getInt(NUM_BUCKETS_ORDINAL), input.getLong(VALUE_ORDINAL));
      }
    }
  }

  public static class BucketString extends BucketBase {
    // magic function for usage with codegen
    public static Integer invoke(int numBuckets, UTF8String value) {
      if (value == null) {
        return null;
      }

      // TODO - We can probably hash the bytes directly given they're already UTF-8 input.
      return apply(numBuckets, hash(value.toString()));
    }

    // Visible for testing
    public static int hash(String value) {
      return BucketUtil.hash(value);
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {DataTypes.IntegerType, DataTypes.StringType};
    }

    @Override
    public String canonicalName() {
      return "iceberg.bucket(string)";
    }

    @Override
    public Integer produceResult(InternalRow input) {
      if (input.isNullAt(NUM_BUCKETS_ORDINAL) || input.isNullAt(VALUE_ORDINAL)) {
        return null;
      } else {
        return invoke(input.getInt(NUM_BUCKETS_ORDINAL), input.getUTF8String(VALUE_ORDINAL));
      }
    }
  }

  public static class BucketBinary extends BucketBase {
    public static Integer invoke(int numBuckets, byte[] value) {
      if (value == null) {
        return null;
      }

      return apply(numBuckets, hash(ByteBuffer.wrap(value)));
    }

    // Visible for testing
    public static int hash(ByteBuffer value) {
      return BucketUtil.hash(value);
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {DataTypes.IntegerType, DataTypes.BinaryType};
    }

    @Override
    public Integer produceResult(InternalRow input) {
      if (input.isNullAt(NUM_BUCKETS_ORDINAL) || input.isNullAt(VALUE_ORDINAL)) {
        return null;
      } else {
        return invoke(input.getInt(NUM_BUCKETS_ORDINAL), input.getBinary(VALUE_ORDINAL));
      }
    }

    @Override
    public String canonicalName() {
      return "iceberg.bucket(binary)";
    }
  }

  public static class BucketDecimal extends BucketBase {
    private final DataType sqlType;
    private final int precision;
    private final int scale;

    // magic method used in codegen
    public static Integer invoke(int numBuckets, Decimal value) {
      if (value == null) {
        return null;
      }

      return apply(numBuckets, hash(value.toJavaBigDecimal()));
    }

    // Visible for testing
    public static int hash(BigDecimal value) {
      return BucketUtil.hash(value);
    }

    public BucketDecimal(DataType sqlType) {
      this.sqlType = sqlType;
      this.precision = ((DecimalType) sqlType).precision();
      this.scale = ((DecimalType) sqlType).scale();
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {DataTypes.IntegerType, sqlType};
    }

    @Override
    public Integer produceResult(InternalRow input) {
      if (input.isNullAt(NUM_BUCKETS_ORDINAL) || input.isNullAt(VALUE_ORDINAL)) {
        return null;
      } else {
        int numBuckets = input.getInt(NUM_BUCKETS_ORDINAL);
        Decimal value = input.getDecimal(VALUE_ORDINAL, precision, scale);
        return invoke(numBuckets, value);
      }
    }

    @Override
    public String canonicalName() {
      return "iceberg.bucket(decimal)";
    }
  }
}
