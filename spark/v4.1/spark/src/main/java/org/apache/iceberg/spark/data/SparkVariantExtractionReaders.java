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
package org.apache.iceberg.spark.data;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.expressions.PathUtil;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetVariantExtractionReaders;
import org.apache.iceberg.parquet.ParquetVariantExtractionReaders.VariantExtractionField;
import org.apache.iceberg.parquet.ParquetVariantExtractionReaders.VariantExtractionRow;
import org.apache.iceberg.parquet.ParquetVariantReaders.DelegatingValueReader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.VariantValue;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.types.UTF8String;

/** Parquet readers that materialize Spark variant extraction struct rows from shredded variants. */
public class SparkVariantExtractionReaders {
  private SparkVariantExtractionReaders() {}

  public static ParquetValueReader<InternalRow> buildStructReader(
      MessageType fileSchema,
      GroupType variantGroup,
      List<String> variantColumnPath,
      StructType extractionStruct) {
    List<VariantExtractionField> fields = Lists.newArrayList();
    int numFields = 0;
    for (StructField field : extractionStruct.fields()) {
      numFields = Math.max(numFields, Integer.parseInt(field.name()) + 1);
    }

    DataType[] targetTypes = new DataType[numFields];
    for (StructField field : extractionStruct.fields()) {
      int ordinal = Integer.parseInt(field.name());
      targetTypes[ordinal] = field.dataType();
      fields.add(
          new VariantExtractionField(
              ordinal,
              SparkVariantExtractionUtil.isPlaceholderExtraction(field),
              PathUtil.parse(SparkVariantExtractionUtil.extractionPath(field))));
    }

    ParquetValueReader<VariantExtractionRow> parquetReader =
        ParquetVariantExtractionReaders.buildRowReader(
            fileSchema, variantGroup, variantColumnPath, fields);

    return new SparkVariantExtractionStructReader(parquetReader, targetTypes);
  }

  /** Counts leaf Parquet columns referenced by a reader tree. */
  public static int leafColumnCount(ParquetValueReader<?> reader) {
    return ParquetVariantExtractionReaders.leafColumnCount(reader);
  }

  /** Visible for unit tests in {@code org.apache.iceberg.spark.data}. */
  static Object toSparkValueForTests(VariantValue value, DataType targetType) {
    return toSparkValue(value, targetType);
  }

  private static class SparkVariantExtractionStructReader
      extends DelegatingValueReader<VariantExtractionRow, InternalRow> {
    private final DataType[] targetTypes;

    private SparkVariantExtractionStructReader(
        ParquetValueReader<VariantExtractionRow> reader, DataType[] targetTypes) {
      super(reader);
      this.targetTypes = targetTypes;
    }

    @Override
    public InternalRow read(InternalRow reuse) {
      VariantExtractionRow row = readFromDelegate(null);
      int numFields = row.numFields();
      GenericInternalRow result =
          reuse instanceof GenericInternalRow
              ? (GenericInternalRow) reuse
              : new GenericInternalRow(numFields);

      if (row.metadata() == null) {
        // SQL NULL variant: match Spark variant_get semantics by nulling every extraction slot,
        // including full-variant placeholder slots. Distinct from a non-null variant where only
        // missing or JSON-null paths produce per-field SQL NULLs.
        for (int i = 0; i < numFields; i += 1) {
          result.setNullAt(i);
        }
        return result;
      }

      for (int i = 0; i < numFields; i += 1) {
        if (row.placeholder(i)) {
          result.setBoolean(i, true);
        } else {
          Object sparkValue = toSparkValue(row.value(i), targetTypes[i]);
          if (sparkValue == null) {
            result.setNullAt(i);
          } else {
            result.update(i, sparkValue);
          }
        }
      }

      return result;
    }
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static Object toSparkValue(VariantValue value, DataType targetType) {
    if (value == null || value.type() == PhysicalType.NULL) {
      return null;
    }

    if (DataTypes.StringType.sameType(targetType)) {
      String stringValue = asString(value);
      return stringValue != null ? UTF8String.fromString(stringValue) : null;
    } else if (DataTypes.LongType.sameType(targetType)) {
      return asLong(value);
    } else if (DataTypes.IntegerType.sameType(targetType)) {
      Long longValue = asLong(value);
      if (longValue == null) {
        return null;
      }
      int intValue = (int) (long) longValue;
      return (long) intValue == longValue ? intValue : null;
    } else if (DataTypes.ByteType.sameType(targetType)) {
      Long longValue = asLong(value);
      if (longValue == null) {
        return null;
      }
      byte byteValue = (byte) (long) longValue;
      return (long) byteValue == longValue ? byteValue : null;
    } else if (DataTypes.ShortType.sameType(targetType)) {
      Long longValue = asLong(value);
      if (longValue == null) {
        return null;
      }
      short shortValue = (short) (long) longValue;
      return (long) shortValue == longValue ? shortValue : null;
    } else if (DataTypes.DateType.sameType(targetType)) {
      return asDateDays(value);
    } else if (targetType instanceof TimestampType) {
      return asTimestampMicros(value, false);
    } else if (targetType instanceof TimestampNTZType) {
      return asTimestampMicros(value, true);
    } else if (DataTypes.BinaryType.sameType(targetType)) {
      return asBinary(value);
    } else if (DataTypes.BooleanType.sameType(targetType)) {
      return asBoolean(value);
    } else if (DataTypes.DoubleType.sameType(targetType)) {
      return asDouble(value);
    } else if (DataTypes.FloatType.sameType(targetType)) {
      Double doubleValue = asDouble(value);
      if (doubleValue == null) {
        return null;
      }
      float floatValue = (float) (double) doubleValue;
      return Float.isInfinite(floatValue) && !Double.isInfinite(doubleValue) ? null : floatValue;
    } else if (targetType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) targetType;
      BigDecimal decimal = asDecimal(value);
      return decimal != null
          ? Decimal.apply(decimal, decimalType.precision(), decimalType.scale())
          : null;
    }

    // Unknown target type: return null (consistent with Spark variant_get type-mismatch behavior).
    return null;
  }

  private static String asString(VariantValue value) {
    switch (value.type()) {
      case STRING:
      case UUID:
        return (String) value.asPrimitive().get();
      default:
        // Variant physical type is not string-compatible; return null like variant_get does.
        return null;
    }
  }

  private static Long asLong(VariantValue value) {
    switch (value.type()) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case TIME:
        return ((Number) value.asPrimitive().get()).longValue();
      default:
        return null;
    }
  }

  private static Integer asDateDays(VariantValue value) {
    if (value.type() == PhysicalType.DATE) {
      return (Integer) value.asPrimitive().get();
    }

    return null;
  }

  private static Long asTimestampMicros(VariantValue value, boolean timestampNtz) {
    switch (value.type()) {
      case TIMESTAMPTZ:
      case TIMESTAMPTZ_NANOS:
        if (timestampNtz) {
          return null;
        }
        return toMicros(value);
      case TIMESTAMPNTZ:
      case TIMESTAMPNTZ_NANOS:
        if (!timestampNtz) {
          return null;
        }
        return toMicros(value);
      default:
        return null;
    }
  }

  private static Long toMicros(VariantValue value) {
    long raw = ((Number) value.asPrimitive().get()).longValue();
    switch (value.type()) {
      case TIMESTAMPTZ_NANOS:
      case TIMESTAMPNTZ_NANOS:
        return raw / 1000L;
      case TIMESTAMPTZ:
      case TIMESTAMPNTZ:
        return raw;
      default:
        return null;
    }
  }

  private static byte[] asBinary(VariantValue value) {
    if (value.type() != PhysicalType.BINARY) {
      return null;
    }

    ByteBuffer buffer = (ByteBuffer) value.asPrimitive().get();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }

  private static Double asDouble(VariantValue value) {
    switch (value.type()) {
      case FLOAT:
      case DOUBLE:
      case INT8:
      case INT16:
      case INT32:
      case INT64:
        return ((Number) value.asPrimitive().get()).doubleValue();
      default:
        return null;
    }
  }

  private static Boolean asBoolean(VariantValue value) {
    switch (value.type()) {
      case BOOLEAN_TRUE:
        return true;
      case BOOLEAN_FALSE:
        return false;
      default:
        return null;
    }
  }

  private static BigDecimal asDecimal(VariantValue value) {
    switch (value.type()) {
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        return (BigDecimal) value.asPrimitive().get();
      default:
        return null;
    }
  }
}
