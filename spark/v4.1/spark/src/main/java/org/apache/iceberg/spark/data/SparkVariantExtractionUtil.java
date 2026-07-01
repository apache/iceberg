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

import org.apache.iceberg.expressions.PathUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.VarcharType;
import org.apache.spark.sql.types.VariantType;

/**
 * Utilities for Spark variant extraction pushdown ({@code __VARIANT_METADATA_KEY}).
 *
 * <p>Path parsing ({@link #isSupportedExtractionPath}) delegates to {@link PathUtil}.
 */
public class SparkVariantExtractionUtil {
  public static final String VARIANT_METADATA_KEY = "__VARIANT_METADATA_KEY";
  public static final String PLACEHOLDER_PATH = "$.__placeholder_field__";

  private SparkVariantExtractionUtil() {}

  public static boolean isVariantExtractionStruct(DataType dataType) {
    if (!(dataType instanceof StructType)) {
      return false;
    }

    StructType structType = (StructType) dataType;
    if (structType.fields().length == 0) {
      return false;
    }

    for (StructField field : structType.fields()) {
      if (!field.metadata().contains(VARIANT_METADATA_KEY)) {
        return false;
      }
    }

    return true;
  }

  public static String extractionPath(StructField field) {
    Preconditions.checkArgument(
        field.metadata().contains(VARIANT_METADATA_KEY),
        "Missing %s on field %s",
        VARIANT_METADATA_KEY,
        field.name());
    Metadata variantMetadata = field.metadata().getMetadata(VARIANT_METADATA_KEY);
    return variantMetadata.getString("path");
  }

  public static boolean isPlaceholderExtraction(StructField field) {
    return PLACEHOLDER_PATH.equals(extractionPath(field));
  }

  /**
   * Returns the {@code failOnError} flag for the extraction (false when absent). Spark sets this to
   * true for {@code variant_get} and false for {@code try_variant_get}; the selective readers honor
   * it on numeric narrowing overflow.
   */
  public static boolean failOnError(StructField field) {
    Preconditions.checkArgument(
        field.metadata().contains(VARIANT_METADATA_KEY),
        "Missing %s on field %s",
        VARIANT_METADATA_KEY,
        field.name());
    Metadata variantMetadata = field.metadata().getMetadata(VARIANT_METADATA_KEY);
    return variantMetadata.contains("failOnError") && variantMetadata.getBoolean("failOnError");
  }

  /**
   * Returns the {@code timeZoneId} for the extraction, or {@code null} when absent. Spark sets this
   * from the session time zone; the selective readers pass it to Spark's {@code VariantGet.cast}
   * when delegating zone-sensitive casts (e.g. timestamp {@code TZ <-> NTZ}).
   */
  public static String timeZoneId(StructField field) {
    Preconditions.checkArgument(
        field.metadata().contains(VARIANT_METADATA_KEY),
        "Missing %s on field %s",
        VARIANT_METADATA_KEY,
        field.name());
    Metadata variantMetadata = field.metadata().getMetadata(VARIANT_METADATA_KEY);
    return variantMetadata.contains("timeZoneId") ? variantMetadata.getString("timeZoneId") : null;
  }

  /**
   * Returns true when the selective Parquet reader can handle the extraction path. Delegates to
   * {@link PathUtil#parse} for RFC 9535-compliant validation; any path that fails to parse is
   * declined.
   */
  public static boolean isSupportedExtractionPath(String jsonPath) {
    try {
      PathUtil.parse(jsonPath);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Returns true when Spark pushdown can materialize the extraction target type from shredded
   * variant columns. Must stay in sync with {@link SparkVariantExtractionReaders} cast support.
   */
  public static boolean isSupportedPushdownTargetType(DataType targetType) {
    if (targetType == null || targetType instanceof VariantType) {
      return false;
    }

    // @todo: add support for container types
    // Spark treats char/varchar as string
    if (isUnsupportedContainerType(targetType)) {
      return false;
    }

    return isSupportedPrimitiveType(targetType);
  }

  private static boolean isUnsupportedContainerType(DataType targetType) {
    return targetType instanceof StructType
        || targetType instanceof ArrayType
        || targetType instanceof MapType
        || targetType instanceof CharType
        || targetType instanceof VarcharType;
  }

  private static boolean isSupportedPrimitiveType(DataType targetType) {
    return isSupportedBasicType(targetType) || isSupportedTemporalOrDecimalType(targetType);
  }

  private static boolean isSupportedBasicType(DataType targetType) {
    return DataTypes.StringType.sameType(targetType)
        || DataTypes.IntegerType.sameType(targetType)
        || DataTypes.LongType.sameType(targetType)
        || DataTypes.ByteType.sameType(targetType)
        || DataTypes.ShortType.sameType(targetType)
        || DataTypes.BooleanType.sameType(targetType)
        || DataTypes.FloatType.sameType(targetType)
        || DataTypes.DoubleType.sameType(targetType)
        || DataTypes.DateType.sameType(targetType)
        || DataTypes.BinaryType.sameType(targetType);
  }

  private static boolean isSupportedTemporalOrDecimalType(DataType targetType) {
    return targetType instanceof DecimalType
        || targetType instanceof TimestampType
        || targetType instanceof TimestampNTZType;
  }

  public static String extractionPath(
      org.apache.spark.sql.connector.read.VariantExtraction extraction) {
    org.apache.spark.sql.types.Metadata metadata = extraction.metadata();
    if (metadata.contains(VARIANT_METADATA_KEY)) {
      return metadata.getMetadata(VARIANT_METADATA_KEY).getString("path");
    }

    return metadata.getString("path");
  }
}
