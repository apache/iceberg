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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.function.ToLongFunction;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UnicodeUtil;

class StatsUtil {
  private StatsUtil() {}

  private static final int NUM_RESERVED_FIELD_STATS_IDS = 200;
  private static final int METADATA_STATS_RANGE_START = 9_000;
  private static final int CONTENT_STATS_RANGE_START = 10_000;
  private static final int CONTENT_STATS_RANGE_END = 200_000_000; // exclusive

  private static final int DATA_FIELD_ID_START = 0;
  // this end ID corresponds to stats base ID 200_000_000, the end of the stats range
  private static final int DATA_FIELD_ID_END = 999_950; // exclusive

  private static final int LAST_UPDATED_SEQ_NUM_BASE_ID = 9_000;
  private static final int ROW_ID_BASE_ID = 9_200;

  // Offsets used for individual stats columns
  static final int LOWER_BOUND_OFFSET = 1;
  static final int UPPER_BOUND_OFFSET = 2;
  static final int TIGHT_BOUNDS_OFFSET = 3;
  static final int VALUE_COUNT_OFFSET = 4;
  static final int NULL_VALUE_COUNT_OFFSET = 5;
  static final int NAN_VALUE_COUNT_OFFSET = 6;
  static final int TOTAL_BYTES_OFFSET = 7;

  static final String LOWER_BOUND_NAME = "lower_bound";
  static final String UPPER_BOUND_NAME = "upper_bound";

  // Offsets used within geo_lower struct
  private static final int GEO_LOWER_X_OFFSET = 10;
  private static final int GEO_LOWER_Y_OFFSET = 11;
  private static final int GEO_LOWER_Z_OFFSET = 12;
  private static final int GEO_LOWER_M_OFFSET = 13;

  // Offsets used within geo_upper struct
  private static final int GEO_UPPER_X_OFFSET = 14;
  private static final int GEO_UPPER_Y_OFFSET = 15;
  private static final int GEO_UPPER_Z_OFFSET = 16;
  private static final int GEO_UPPER_M_OFFSET = 17;

  private static final Map<Integer, Integer> METADATA_ID_TO_BASE_ID =
      ImmutableMap.of(
          MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId(), LAST_UPDATED_SEQ_NUM_BASE_ID,
          MetadataColumns.ROW_ID.fieldId(), ROW_ID_BASE_ID);

  /**
   * Return the base ID of the stats struct for the given field ID, or -1 if the ID is out of range.
   *
   * @param fieldId a table field ID
   * @return the base ID for a field stats struct, or -1 if stats cannot be stored
   */
  @VisibleForTesting
  static int toBaseId(int fieldId) {
    if (fieldId >= DATA_FIELD_ID_START && fieldId < DATA_FIELD_ID_END) {
      return (fieldId * NUM_RESERVED_FIELD_STATS_IDS) + CONTENT_STATS_RANGE_START;
    }

    return METADATA_ID_TO_BASE_ID.getOrDefault(fieldId, -1);
  }

  /**
   * Return the field ID corresponding to the stats field ID.
   *
   * @param statId the field ID of a field stats struct or field within a stats struct
   * @return ID of the corresponding table field
   * @throws IllegalArgumentException if the stats ID is not valid
   * @throws UnsupportedOperationException if the stats ID is for an unsupported metadata field
   */
  static int toFieldId(int statId) {
    Preconditions.checkArgument(isValidStatId(statId), "Invalid stats field ID: %s", statId);

    if (statId < CONTENT_STATS_RANGE_START) {
      if (inBaseIdRange(LAST_UPDATED_SEQ_NUM_BASE_ID, statId)) {
        return MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId();
      } else if (inBaseIdRange(ROW_ID_BASE_ID, statId)) {
        return MetadataColumns.ROW_ID.fieldId();
      } else {
        throw new UnsupportedOperationException("Unsupported metadata stats field ID: " + statId);
      }
    }

    return (statId - CONTENT_STATS_RANGE_START) / NUM_RESERVED_FIELD_STATS_IDS;
  }

  /**
   * Return the stats offset of a stats field ID.
   *
   * @param statId the field ID of a field stats struct or field within a stats struct
   * @return offset that identifies the stored metric, or 0 for a stats struct's ID
   * @throws IllegalArgumentException if the stats ID is not valid
   * @throws UnsupportedOperationException if the stats ID is for an unsupported metadata field
   */
  static int statOffset(int statId) {
    Preconditions.checkArgument(isValidStatId(statId), "Invalid stats field ID: %s", statId);

    return statId % NUM_RESERVED_FIELD_STATS_IDS;
  }

  public static Types.NestedField contentStatsField(Types.StructType contentStats) {
    return optional(TrackedFile.CONTENT_STATS_ID, TrackedFile.CONTENT_STATS_NAME, contentStats);
  }

  public static Types.StructType statsWriteSchema(Schema tableSchema, MetricsConfig metricsConfig) {
    Map<Integer, String> idToStatsName = TypeUtil.indexStatsNames(tableSchema.asStruct());
    List<Types.NestedField> fieldStructs = Lists.newArrayList();
    Map<Integer, Integer> parentIndex = TypeUtil.indexParents(tableSchema.asStruct());

    for (int id : metricsConfig.metricsFieldIds()) {
      String fieldName = idToStatsName.get(id);
      Types.NestedField field = tableSchema.findField(id);
      Preconditions.checkArgument(
          field != null, "Cannot build content stats schema: missing field ID %s", id);

      if (isScalar(tableSchema, parentIndex, id)) {
        int baseId = toBaseId(id);
        Types.StructType fieldStruct =
            fieldStatsStruct(field.type(), baseId, metricsConfig.columnMode(id));

        if (fieldStruct != null) {
          fieldStructs.add(optional(baseId, fieldName, fieldStruct));
        }
      }
    }

    return Types.StructType.of(fieldStructs);
  }

  /**
   * Produce a schema to read content stats for the given table field IDs.
   *
   * @param tableSchema a schema
   * @param fieldIds an iterable of field IDs to project stats for
   * @return a content stats struct for a read
   */
  public static Types.StructType statsReadSchema(Schema tableSchema, Iterable<Integer> fieldIds) {
    Map<Integer, String> idToStatsName = TypeUtil.indexStatsNames(tableSchema.asStruct());
    List<Types.NestedField> fieldStructs = Lists.newArrayList();
    Map<Integer, Integer> parentIndex = TypeUtil.indexParents(tableSchema.asStruct());

    for (int id : fieldIds) {
      String fieldName = idToStatsName.get(id);
      Types.NestedField field = tableSchema.findField(id);

      // if a field is missing, stats are not projected or used but it is not a failure
      if (field != null && isScalar(tableSchema, parentIndex, id)) {
        int baseId = toBaseId(id);
        Types.StructType fieldStruct =
            fieldStatsStruct(field.type(), baseId, MetricsModes.Full.get());

        if (fieldStruct != null) {
          fieldStructs.add(optional(baseId, fieldName, fieldStruct));
        }
      }
    }

    return Types.StructType.of(fieldStructs);
  }

  /** Return whether the stat ID is valid for either a data or metadata column. */
  private static boolean isValidStatId(int statId) {
    return statId >= METADATA_STATS_RANGE_START && statId < CONTENT_STATS_RANGE_END;
  }

  /** Return whether the stat ID belongs to the base ID. */
  private static boolean inBaseIdRange(int baseId, int statId) {
    return statId >= baseId && statId < (baseId + NUM_RESERVED_FIELD_STATS_IDS);
  }

  private static boolean tracksTightBounds(Type type) {
    return !type.isVariantType() && !isGeoType(type);
  }

  private static boolean isFloatingPoint(Type type) {
    return type.typeId() == Type.TypeID.FLOAT || type.typeId() == Type.TypeID.DOUBLE;
  }

  private static boolean isVariableLength(Type type) {
    return type.isVariantType()
        || type.typeId() == Type.TypeID.STRING
        || type.typeId() == Type.TypeID.BINARY
        || isGeoType(type);
  }

  private static boolean isGeoType(Type type) {
    return type.typeId() == Type.TypeID.GEOMETRY || type.typeId() == Type.TypeID.GEOGRAPHY;
  }

  private static Types.StructType geoLowerBound(int baseId) {
    return Types.StructType.of(
        required(
            baseId + GEO_LOWER_X_OFFSET,
            "x",
            Types.DoubleType.get(),
            "Bounding box westernmost/xmin; [-180..180]"),
        required(
            baseId + GEO_LOWER_Y_OFFSET,
            "y",
            Types.DoubleType.get(),
            "Bounding box southernmost/ymin; [-90..90]"),
        optional(baseId + GEO_LOWER_Z_OFFSET, "z", Types.DoubleType.get(), "Bounding box zmin"),
        optional(baseId + GEO_LOWER_M_OFFSET, "m", Types.DoubleType.get(), "Bounding box mmin"));
  }

  private static Types.StructType geoUpperBound(int baseId) {
    return Types.StructType.of(
        required(
            baseId + GEO_UPPER_X_OFFSET,
            "x",
            Types.DoubleType.get(),
            "Bounding box easternmost/xmax; [-180..180]"),
        required(
            baseId + GEO_UPPER_Y_OFFSET,
            "y",
            Types.DoubleType.get(),
            "Bounding box northernmost/ymax; [-90..90]"),
        optional(baseId + GEO_UPPER_Z_OFFSET, "z", Types.DoubleType.get(), "Bounding box zmax"),
        optional(baseId + GEO_UPPER_M_OFFSET, "m", Types.DoubleType.get(), "Bounding box mmax"));
  }

  private static Types.NestedField lowerBoundField(Type type, int baseId) {
    Type boundType = isGeoType(type) ? geoLowerBound(baseId) : type;
    return optional(baseId + LOWER_BOUND_OFFSET, LOWER_BOUND_NAME, boundType);
  }

  private static Types.NestedField upperBoundField(Type type, int baseId) {
    Type boundType = isGeoType(type) ? geoUpperBound(baseId) : type;
    return optional(baseId + UPPER_BOUND_OFFSET, UPPER_BOUND_NAME, boundType);
  }

  @VisibleForTesting
  static Types.StructType fieldStatsStruct(Type type, int baseId, MetricsModes.MetricsMode mode) {
    if (null == mode || mode == MetricsModes.None.get() || type.isNestedType() || baseId < 0) {
      return null;
    }

    List<Types.NestedField> fields = Lists.newArrayList();

    if (mode.hasBounds()) {
      fields.add(lowerBoundField(type, baseId));
      fields.add(upperBoundField(type, baseId));

      if (tracksTightBounds(type)) {
        fields.add(
            optional(
                baseId + TIGHT_BOUNDS_OFFSET,
                "tight_bounds",
                Types.BooleanType.get(),
                "True if lower=min and upper=max; false otherwise"));
      }
    }

    fields.add(
        optional(
            baseId + VALUE_COUNT_OFFSET,
            "value_count",
            Types.LongType.get(),
            "Number of values (including null and NaN)"));

    fields.add(
        optional(
            baseId + NULL_VALUE_COUNT_OFFSET,
            "null_value_count",
            Types.LongType.get(),
            "Number of null values"));

    if (isFloatingPoint(type)) {
      fields.add(
          optional(
              baseId + NAN_VALUE_COUNT_OFFSET,
              "nan_value_count",
              Types.LongType.get(),
              "Number of NaN values"));
    }

    if (isVariableLength(type)) {
      fields.add(optional(baseId + TOTAL_BYTES_OFFSET, "total_bytes", Types.LongType.get()));
    }

    return Types.StructType.of(fields);
  }

  /** Return whether a field has one value or may be repeated in map or list. */
  private static boolean isScalar(Schema schema, Map<Integer, Integer> parentIndex, int id) {
    Integer currentId = id;
    while (currentId != null) {
      Type type = schema.findField(currentId).type();
      if (type.isMapType() || type.isListType()) {
        return false;
      }

      currentId = parentIndex.get(currentId);
    }

    return true;
  }

  /** Inclusive source column bounds for a partition value. Upper is null when unbounded. */
  record PartitionSourceBounds(Object lower, Object upper) {}

  /**
   * Returns the source column bounds for a partition value, or null for a NaN identity value.
   *
   * @throws IllegalArgumentException if the transform does not preserve order or the value is null
   */
  static PartitionSourceBounds sourceBoundsFor(
      Schema schema, PartitionField field, Object partitionValue) {
    Preconditions.checkArgument(
        field.transform().preservesOrder(),
        "Cannot derive source column bounds from partition field that does not preserve order: %s",
        field);
    Preconditions.checkArgument(
        partitionValue != null, "Cannot derive source column bounds from null partition value");
    Type sourceType = schema.findType(field.sourceId());
    Preconditions.checkArgument(
        sourceType != null, "Cannot find source column for partition field: %s", field);
    return PartitionSpecVisitor.visit(
        schema, field, new PartitionSourceBoundsVisitor(sourceType, partitionValue));
  }

  private static class PartitionSourceBoundsVisitor
      implements PartitionSpecVisitor<PartitionSourceBounds> {
    private final Type sourceType;
    private final Object value;

    private PartitionSourceBoundsVisitor(Type sourceType, Object value) {
      this.sourceType = sourceType;
      this.value = value;
    }

    @Override
    public PartitionSourceBounds identity(int fieldId, String sourceName, int sourceId) {
      // convert to the source type, which may have been promoted after the value was written
      Object bound =
          switch (sourceType.typeId()) {
            case LONG -> ((Number) value).longValue();
            case DOUBLE -> ((Number) value).doubleValue();
            default -> value;
          };

      if (bound instanceof ByteBuffer buffer) {
        bound = ByteBuffers.copy(buffer);
      }

      // NaN cannot be used as a bound
      if (bound instanceof Double d && d.isNaN() || bound instanceof Float f && f.isNaN()) {
        return null;
      }

      return new PartitionSourceBounds(bound, bound);
    }

    @Override
    public PartitionSourceBounds truncate(int fieldId, String sourceName, int sourceId, int width) {
      // source values are at most width - 1 larger than the partition value, unscaled for decimals
      return switch (sourceType.typeId()) {
        case INTEGER -> {
          int lower = (Integer) value;
          yield new PartitionSourceBounds(
              lower, (int) Math.min(Integer.MAX_VALUE, (long) lower + width - 1));
        }
        case LONG -> {
          long lower = ((Number) value).longValue();
          yield new PartitionSourceBounds(
              lower, lower > Long.MAX_VALUE - (width - 1) ? Long.MAX_VALUE : lower + width - 1);
        }
        case DECIMAL -> {
          BigDecimal lower = (BigDecimal) value;
          int precision = ((Types.DecimalType) sourceType).precision();
          BigInteger maxUnscaled = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
          BigInteger unscaledUpper =
              lower.unscaledValue().add(BigInteger.valueOf(width - 1)).min(maxUnscaled);
          yield new PartitionSourceBounds(lower, new BigDecimal(unscaledUpper, lower.scale()));
        }
        case STRING -> truncatedString(value.toString(), width);
        case BINARY -> truncatedBinary((ByteBuffer) value, width);
        default ->
            throw new UnsupportedOperationException("Unsupported truncate source: " + sourceType);
      };
    }

    private static PartitionSourceBounds truncatedString(String prefix, int width) {
      if (prefix.codePointCount(0, prefix.length()) < width) {
        // a prefix shorter than the width was not truncated, so every value equals it
        return new PartitionSourceBounds(prefix, prefix);
      }

      // add a character so truncateStringMax returns the next higher prefix
      return new PartitionSourceBounds(
          prefix, UnicodeUtil.truncateStringMax(prefix + "\u0000", width));
    }

    private static PartitionSourceBounds truncatedBinary(ByteBuffer value, int width) {
      ByteBuffer prefix = ByteBuffers.copy(value);
      if (prefix.remaining() < width) {
        // a prefix shorter than the width was not truncated, so every value equals it
        return new PartitionSourceBounds(prefix, prefix);
      }

      // add a byte so truncateBinaryMax returns the next higher prefix
      ByteBuffer extended = ByteBuffer.allocate(prefix.remaining() + 1);
      extended.put(prefix.duplicate()).put((byte) 0).flip();
      return new PartitionSourceBounds(prefix, BinaryUtil.truncateBinaryMax(extended, width));
    }

    @Override
    public PartitionSourceBounds year(int fieldId, String sourceName, int sourceId) {
      return timeBoundsFor(ChronoUnit.YEARS);
    }

    @Override
    public PartitionSourceBounds month(int fieldId, String sourceName, int sourceId) {
      return timeBoundsFor(ChronoUnit.MONTHS);
    }

    @Override
    public PartitionSourceBounds day(int fieldId, String sourceName, int sourceId) {
      return timeBoundsFor(ChronoUnit.DAYS);
    }

    @Override
    public PartitionSourceBounds hour(int fieldId, String sourceName, int sourceId) {
      return timeBoundsFor(ChronoUnit.HOURS);
    }

    private PartitionSourceBounds timeBoundsFor(ChronoUnit unit) {
      long periods = (Integer) value;
      // the lower bound starts one unit earlier for 0 or less since older writers had a bug that
      // rounded pre-1970 values toward zero, except for day transforms on dates
      long lowerPeriods = periods <= 0 ? periods - 1 : periods;
      return switch (sourceType.typeId()) {
        case DATE -> {
          long dateLowerPeriods = unit == ChronoUnit.DAYS ? periods : lowerPeriods;
          yield new PartitionSourceBounds(
              (int) Math.max(Integer.MIN_VALUE, daysFromEpoch(dateLowerPeriods, unit)),
              (int) Math.min(Integer.MAX_VALUE, daysFromEpoch(periods + 1, unit) - 1));
        }
        case TIMESTAMP ->
            new PartitionSourceBounds(
                unitsFromEpoch(DateTimeUtil::microsFromTimestamptz, lowerPeriods, unit),
                before(unitsFromEpoch(DateTimeUtil::microsFromTimestamptz, periods + 1, unit)));
        case TIMESTAMP_NANO ->
            new PartitionSourceBounds(
                unitsFromEpoch(DateTimeUtil::nanosFromTimestamptz, lowerPeriods, unit),
                before(unitsFromEpoch(DateTimeUtil::nanosFromTimestamptz, periods + 1, unit)));
        default ->
            throw new UnsupportedOperationException("Unsupported time source: " + sourceType);
      };
    }

    private static long daysFromEpoch(long periods, ChronoUnit unit) {
      return ChronoUnit.DAYS.between(
          DateTimeUtil.EPOCH_DAY, DateTimeUtil.EPOCH_DAY.plus(periods, unit));
    }

    private static long unitsFromEpoch(
        ToLongFunction<OffsetDateTime> fromEpoch, long periods, ChronoUnit unit) {
      OffsetDateTime time = DateTimeUtil.EPOCH.plus(periods, unit);
      try {
        return fromEpoch.applyAsLong(time);
      } catch (ArithmeticException e) {
        return time.isBefore(DateTimeUtil.EPOCH) ? Long.MIN_VALUE : Long.MAX_VALUE;
      }
    }

    private static long before(long exclusiveUpper) {
      if (exclusiveUpper == Long.MAX_VALUE || exclusiveUpper == Long.MIN_VALUE) {
        return exclusiveUpper;
      }

      return exclusiveUpper - 1;
    }
  }
}
