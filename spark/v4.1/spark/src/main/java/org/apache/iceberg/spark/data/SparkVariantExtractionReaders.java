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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.ZoneId;
import java.util.List;
import org.apache.iceberg.expressions.PathUtil;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetVariantExtractionReaders;
import org.apache.iceberg.parquet.ParquetVariantExtractionReaders.VariantExtractionField;
import org.apache.iceberg.parquet.ParquetVariantExtractionReaders.VariantExtractionRow;
import org.apache.iceberg.parquet.ParquetVariantReaders.DelegatingValueReader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.variant.VariantCastArgs;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.VariantVal;

/** Parquet readers that materialize Spark variant extraction struct rows from shredded variants. */
public class SparkVariantExtractionReaders {
  // Empty variant metadata dictionary, reused for delegating primitive-leaf casts (primitives do
  // not reference the dictionary, so the per-row dictionary need not be serialized).
  private static final byte[] EMPTY_VARIANT_METADATA = serializeMetadata(VariantMetadata.empty());

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
    boolean[] failOnError = new boolean[numFields];
    String[] timeZoneIds = new String[numFields];
    for (StructField field : extractionStruct.fields()) {
      int ordinal = Integer.parseInt(field.name());
      targetTypes[ordinal] = field.dataType();
      failOnError[ordinal] = SparkVariantExtractionUtil.failOnError(field);
      timeZoneIds[ordinal] = SparkVariantExtractionUtil.timeZoneId(field);
      fields.add(
          new VariantExtractionField(
              ordinal,
              SparkVariantExtractionUtil.isPlaceholderExtraction(field),
              PathUtil.parse(SparkVariantExtractionUtil.extractionPath(field))));
    }

    ParquetValueReader<VariantExtractionRow> parquetReader =
        ParquetVariantExtractionReaders.buildRowReader(
            fileSchema, variantGroup, variantColumnPath, fields);

    return new SparkVariantExtractionStructReader(
        parquetReader, targetTypes, failOnError, timeZoneIds);
  }

  /** Counts leaf Parquet columns referenced by a reader tree. */
  public static int leafColumnCount(ParquetValueReader<?> reader) {
    return ParquetVariantExtractionReaders.leafColumnCount(reader);
  }

  /** Visible for unit tests in {@code org.apache.iceberg.spark.data}. */
  static Object toSparkValueForTests(VariantValue value, DataType targetType) {
    return toSparkValueForTests(value, targetType, false);
  }

  /** Visible for unit tests in {@code org.apache.iceberg.spark.data}. */
  static Object toSparkValueForTests(VariantValue value, DataType targetType, boolean failOnError) {
    // Tests pass standalone primitive values, so an empty metadata dictionary and UTC suffice.
    return toSparkValue(value, targetType, failOnError, VariantMetadata.empty(), "UTC");
  }

  private static class SparkVariantExtractionStructReader
      extends DelegatingValueReader<VariantExtractionRow, InternalRow> {
    private final DataType[] targetTypes;
    private final boolean[] failOnError;
    private final String[] timeZoneIds;

    private SparkVariantExtractionStructReader(
        ParquetValueReader<VariantExtractionRow> reader,
        DataType[] targetTypes,
        boolean[] failOnError,
        String[] timeZoneIds) {
      super(reader);
      this.targetTypes = targetTypes;
      this.failOnError = failOnError;
      this.timeZoneIds = timeZoneIds;
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
          Object sparkValue =
              toSparkValue(
                  row.value(i), targetTypes[i], failOnError[i], row.metadata(), timeZoneIds[i]);
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

  /**
   * Materializes a single extracted shredded value as the Spark {@code targetType} by delegating to
   * Spark's own {@code VariantGet.cast}.
   *
   * <p>Delegation serializes the extracted leaf back into a Spark {@link VariantVal} and runs the
   * same cast the whole-variant {@code variant_get} path runs, so every conversion — including
   * cross-type and lossy casts, {@code failOnError} throwing, {@code timeZoneId} handling, and
   * physical types Spark's variant cannot represent ({@code TIME}, nanos timestamps) — matches the
   * non-pushdown path exactly. The selective reader's benefit is reading fewer Parquet columns; it
   * intentionally does not reimplement Spark's cast semantics. A faster inline conversion for the
   * hot primitive pairs can be layered on as a follow-up.
   */
  private static Object toSparkValue(
      VariantValue value,
      DataType targetType,
      boolean failOnError,
      VariantMetadata metadata,
      String timeZoneId) {
    if (value == null || value.type() == PhysicalType.NULL) {
      // Missing path or variant null: always SQL NULL, regardless of failOnError. This mirrors
      // Spark's VariantGet, which returns null for a missing path / variant null before any cast.
      return null;
    }

    return delegate(value, metadata, targetType, failOnError, timeZoneId);
  }

  /**
   * Delegates the cast to Spark's own {@code VariantGet.cast} by serializing the extracted leaf
   * value into a Spark {@link VariantVal}, so cross-type and lossy casts match the non-pushdown
   * {@code variant_get} path exactly (returning Spark's value, or throwing {@code
   * INVALID_VARIANT_CAST} when {@code failOnError = true}).
   */
  private static Object delegate(
      VariantValue value,
      VariantMetadata metadata,
      DataType targetType,
      boolean failOnError,
      String timeZoneId) {
    byte[] metadataBytes;
    if (value.type() == PhysicalType.OBJECT || value.type() == PhysicalType.ARRAY) {
      // Object/array values reference the dictionary (e.g. for cast-to-string JSON rendering).
      metadataBytes = serializeMetadata(metadata != null ? metadata : VariantMetadata.empty());
    } else {
      metadataBytes = EMPTY_VARIANT_METADATA;
    }

    byte[] valueBytes = new byte[value.sizeInBytes()];
    value.writeTo(ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN), 0);

    // Fall back to the session time zone (matching the non-pushdown variant_get path) when the
    // extraction metadata did not carry one.
    String zone = timeZoneId != null ? timeZoneId : SQLConf.get().sessionLocalTimeZone();
    VariantCastArgs castArgs =
        new VariantCastArgs(failOnError, scala.Option.apply(zone), ZoneId.of(zone));
    return org.apache.spark.sql.catalyst.expressions.variant.VariantGet$.MODULE$.cast(
        new VariantVal(valueBytes, metadataBytes), targetType, castArgs);
  }

  private static byte[] serializeMetadata(VariantMetadata metadata) {
    byte[] bytes = new byte[metadata.sizeInBytes()];
    metadata.writeTo(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN), 0);
    return bytes;
  }
}
