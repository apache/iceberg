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
package org.apache.iceberg.transforms;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import org.apache.iceberg.expressions.BoundLiteralPredicate;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundSetPredicate;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.SerializableFunction;

/**
 * A transform that casts values from one type to another.
 *
 * <p>This transform performs explicit type conversions, similar to SQL CAST operations. It supports
 * casting between compatible types with appropriate handling of overflow, precision loss, and null
 * values.
 *
 * @param <S> the source type
 * @param <T> the target type
 */
class Cast<S, T> implements Transform<S, T> {
  private static final long MICROS_PER_DAY = 86_400_000_000L;
  private static final long NANOS_PER_MICRO = 1_000L;
  private static final long NANOS_PER_DAY = 86_400_000_000_000L;

  private final Type targetType;

  private Cast(Type targetType) {
    Preconditions.checkNotNull(targetType, "Target type cannot be null");
    this.targetType = targetType;
  }

  @SuppressWarnings("unchecked")
  static <S, T> Cast<S, T> get(Type targetType) {
    return new Cast<>(targetType);
  }

  @Override
  public SerializableFunction<S, T> bind(Type sourceType) {
    return new CastFunction<>(sourceType, targetType);
  }

  @Override
  public boolean canTransform(Type sourceType) {
    return isValidCast(sourceType, targetType);
  }

  @Override
  public Type getResultType(Type sourceType) {
    Preconditions.checkArgument(
        canTransform(sourceType), "Cannot cast from %s to %s", sourceType, targetType);
    return targetType;
  }

  @Override
  public boolean preservesOrder() {
    // Cast order preservation depends on source and target types.
    return false;
  }

  @Override
  public boolean preservesOrder(Type sourceType) {
    Type.TypeID source = sourceType.typeId();
    Type.TypeID target = targetType.typeId();

    if (source == target) {
      return true;
    }

    if (isNumeric(source) && isNumeric(target)) {
      return true;
    }

    if (isDateTimeOrderPreserving(source, target)) {
      return true;
    }

    return false;
  }

  private static boolean isNumeric(Type.TypeID type) {
    switch (type) {
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        return true;
      default:
        return false;
    }
  }

  private static boolean isDateTimeOrderPreserving(Type.TypeID source, Type.TypeID target) {
    switch (source) {
      case INTEGER:
        return target == Type.TypeID.DATE;

      case LONG:
        return target == Type.TypeID.TIME
            || target == Type.TypeID.TIMESTAMP
            || target == Type.TypeID.TIMESTAMP_NANO
            || target == Type.TypeID.DATE;

      case DATE:
        return target == Type.TypeID.TIMESTAMP
            || target == Type.TypeID.TIMESTAMP_NANO
            || target == Type.TypeID.STRING;

      case TIME:
        return target == Type.TypeID.LONG || target == Type.TypeID.STRING;

      case TIMESTAMP:
        return target == Type.TypeID.DATE
            || target == Type.TypeID.TIMESTAMP_NANO
            || target == Type.TypeID.LONG;

      case TIMESTAMP_NANO:
        return target == Type.TypeID.DATE
            || target == Type.TypeID.TIMESTAMP
            || target == Type.TypeID.LONG;

      case STRING:
        return target == Type.TypeID.DATE;

      default:
        return false;
    }
  }

  @Override
  public UnboundPredicate<T> project(String name, BoundPredicate<S> predicate) {
    // For cast expressions like cast(col, targetType) = value,
    // we can project by inverting the cast: col = cast(value, sourceType)
    // This allows pushdown when the table is partitioned by the source column

    if (predicate == null) {
      return null;
    }

    // Get the source type from the predicate's term
    Type sourceType = predicate.term().ref().type();

    if (predicate.isUnaryPredicate()) {
      // IS NULL, NOT NULL, IS NAN, NOT NAN can be projected directly
      return Expressions.predicate(predicate.op(), name);
    } else if (predicate.isLiteralPredicate()) {
      // Try to inverse cast the literal from target type back to source type
      BoundLiteralPredicate<S> literalPred = predicate.asLiteralPredicate();
      Object inverseCastValue = inverseCast(literalPred.literal().value(), targetType, sourceType);
      if (inverseCastValue != null) {
        @SuppressWarnings("unchecked")
        UnboundPredicate<T> result =
            (UnboundPredicate<T>) Expressions.predicate(predicate.op(), name, inverseCastValue);
        return result;
      }
    } else if (predicate.isSetPredicate()) {
      // For IN/NOT IN predicates, inverse cast all literals
      BoundSetPredicate<S> setPred = predicate.asSetPredicate();
      List<Object> inverseCastValues = new java.util.ArrayList<>();
      for (S value : setPred.literalSet()) {
        Object inverseCastValue = inverseCast(value, targetType, sourceType);
        if (inverseCastValue == null) {
          // If any literal cannot be inverse cast, cannot project
          return null;
        }
        inverseCastValues.add(inverseCastValue);
      }
      @SuppressWarnings("unchecked")
      UnboundPredicate<T> result =
          (UnboundPredicate<T>) Expressions.predicate(predicate.op(), name, inverseCastValues);
      return result;
    }

    return null;
  }

  @Override
  public UnboundPredicate<T> projectStrict(String name, BoundPredicate<S> predicate) {
    if (predicate == null) {
      return null;
    }

    Type sourceType = predicate.term().ref().type();

    if (!hasExactInverseCast(predicate, sourceType, targetType)) {
      return null;
    }

    return project(name, predicate);
  }

  /** Checks if an inverse cast from targetType back to sourceType is exact (no precision loss). */
  private boolean hasExactInverseCast(
      BoundPredicate<S> predicate, Type sourceType, Type targetType) {
    if (!isExactInverseCast(sourceType, targetType)) {
      return false;
    }

    if (predicate.isUnaryPredicate()) {
      return true;
    } else if (predicate.isLiteralPredicate()) {
      return hasExactInverseValue(
          predicate.asLiteralPredicate().literal().value(), targetType, sourceType);
    } else if (predicate.isSetPredicate()) {
      for (S value : predicate.asSetPredicate().literalSet()) {
        if (!hasExactInverseValue(value, targetType, sourceType)) {
          return false;
        }
      }
      return true;
    }

    return false;
  }

  private boolean hasExactInverseValue(Object value, Type fromType, Type toType) {
    Object inverseValue = inverseCast(value, fromType, toType);
    if (inverseValue == null) {
      return false;
    }

    Object roundTripValue = new CastFunction<>(toType, fromType).apply(inverseValue);
    return Objects.equals(value, roundTripValue);
  }

  private boolean isExactInverseCast(Type sourceType, Type targetType) {
    Type.TypeID source = sourceType.typeId();
    Type.TypeID target = targetType.typeId();

    if (source == target) {
      return true;
    }

    switch (source) {
      case INTEGER:
        return target == Type.TypeID.LONG || target == Type.TypeID.DATE;
      case LONG:
        return target == Type.TypeID.DATE
            || target == Type.TypeID.TIME
            || target == Type.TypeID.TIMESTAMP
            || target == Type.TypeID.TIMESTAMP_NANO;
      case DATE:
        return false;
      case TIME:
        return target == Type.TypeID.LONG;
      case TIMESTAMP:
        return target == Type.TypeID.TIMESTAMP_NANO || target == Type.TypeID.LONG;
      case TIMESTAMP_NANO:
        return target == Type.TypeID.LONG;
      case STRING:
        return false;
      default:
        return false;
    }
  }

  /**
   * Inverse cast a value from target type back to source type. This is used for predicate pushdown
   * when the partition is on the source column.
   */
  @SuppressWarnings("unchecked")
  private Object inverseCast(Object value, Type fromType, Type toType) {
    if (value == null) {
      return null;
    }

    Type.TypeID from = fromType.typeId();
    Type.TypeID to = toType.typeId();

    // If types are the same, no cast needed
    if (from == to) {
      return value;
    }

    try {
      switch (from) {
        case STRING:
          // Inverse cast from string - parse the string
          return inverseCastFromString((CharSequence) value, to, toType);
        case INTEGER:
          return inverseCastFromInteger((Integer) value, to);
        case LONG:
          return inverseCastFromLong((Long) value, to);
        case FLOAT:
          return inverseCastFromFloat((Float) value, to);
        case DOUBLE:
          return inverseCastFromDouble((Double) value, to);
        case DATE:
          return inverseCastFromDate((Integer) value, to);
        case TIME:
          return inverseCastFromTime((Long) value, to);
        case TIMESTAMP:
          return inverseCastFromTimestamp((Long) value, to, (Types.TimestampType) fromType);
        case TIMESTAMP_NANO:
          return inverseCastFromTimestampNano((Long) value, to, (Types.TimestampNanoType) fromType);
        default:
          // Cannot inverse cast from other types
          return null;
      }
    } catch (Exception e) {
      // If inverse cast fails, return null to indicate projection is not possible
      return null;
    }
  }

  private Object inverseCastFromString(CharSequence value, Type.TypeID target, Type targetType) {
    String str = value.toString().trim();
    switch (target) {
      case BOOLEAN:
        return Boolean.valueOf(str);
      case INTEGER:
        return Integer.valueOf(str);
      case LONG:
        return Long.valueOf(str);
      case FLOAT:
        return Float.valueOf(str);
      case DOUBLE:
        return Double.valueOf(str);
      case DECIMAL:
        return new BigDecimal(str);
      case DATE:
        return Integer.valueOf(DateTimeUtil.isoDateToDays(str));
      case TIME:
        return Long.valueOf(DateTimeUtil.isoTimeToMicros(str));
      case TIMESTAMP:
        Types.TimestampType timestampType = (Types.TimestampType) targetType;
        if (timestampType.shouldAdjustToUTC()) {
          return Long.valueOf(DateTimeUtil.isoTimestamptzToMicros(str));
        } else {
          return Long.valueOf(DateTimeUtil.isoTimestampToMicros(str));
        }
      case TIMESTAMP_NANO:
        Types.TimestampNanoType timestampNanoType = (Types.TimestampNanoType) targetType;
        if (timestampNanoType.shouldAdjustToUTC()) {
          return Long.valueOf(DateTimeUtil.isoTimestamptzToNanos(str));
        } else {
          return Long.valueOf(DateTimeUtil.isoTimestampToNanos(str));
        }
      case UUID:
        return java.util.UUID.fromString(str);
      default:
        return null;
    }
  }

  private Object inverseCastFromInteger(Integer value, Type.TypeID target) {
    switch (target) {
      case INTEGER:
        return value;
      case LONG:
        return value.longValue();
      case FLOAT:
        return value.floatValue();
      case DOUBLE:
        return value.doubleValue();
      case DATE:
        return value; // Integer days since epoch
      case DECIMAL:
        return new BigDecimal(value);
      default:
        return null;
    }
  }

  private Object inverseCastFromLong(Long value, Type.TypeID target) {
    switch (target) {
      case INTEGER:
        if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
          return value.intValue();
        }
        return null;
      case LONG:
        return value;
      case FLOAT:
        return value.floatValue();
      case DOUBLE:
        return value.doubleValue();
      case TIME:
        return value; // Long microseconds
      case TIMESTAMP:
        return value; // Long microseconds
      case TIMESTAMP_NANO:
        return value; // Long nanoseconds
      case DATE:
        if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
          return value.intValue();
        }
        return null;
      case DECIMAL:
        return new BigDecimal(value);
      default:
        return null;
    }
  }

  private Object inverseCastFromFloat(Float value, Type.TypeID target) {
    switch (target) {
      case FLOAT:
        return value;
      case DOUBLE:
        return value.doubleValue();
      case DECIMAL:
        return BigDecimal.valueOf(value);
      default:
        return null;
    }
  }

  private Object inverseCastFromDouble(Double value, Type.TypeID target) {
    switch (target) {
      case FLOAT:
        return value.floatValue();
      case DOUBLE:
        return value;
      case DECIMAL:
        return BigDecimal.valueOf(value);
      default:
        return null;
    }
  }

  private Object inverseCastFromDate(Integer value, Type.TypeID target) {
    switch (target) {
      case DATE:
        return value;
      case INTEGER:
        return value;
      case LONG:
        return value.longValue();
      case STRING:
        return DateTimeUtil.daysToIsoDate(value);
      case TIMESTAMP:
        return null;
      case TIMESTAMP_NANO:
        return null;
      default:
        return null;
    }
  }

  private Object inverseCastFromTime(Long value, Type.TypeID target) {
    switch (target) {
      case TIME:
        return value;
      case LONG:
        return value;
      default:
        return null;
    }
  }

  @SuppressWarnings("UnusedVariable")
  private Object inverseCastFromTimestamp(
      Long value, Type.TypeID target, Types.TimestampType timestampType) {
    switch (target) {
      case TIMESTAMP:
        return value;
      case LONG:
        return value;
      case DATE:
        int days = DateTimeUtil.microsToDays(value);
        if (value.equals(Math.multiplyExact(days, MICROS_PER_DAY))) {
          return days;
        }
        return null;
      case TIMESTAMP_NANO:
        // Convert timestamp (microseconds) to timestamp nano (nanoseconds)
        return Math.multiplyExact(value, NANOS_PER_MICRO);
      default:
        return null;
    }
  }

  @SuppressWarnings("UnusedVariable")
  private Object inverseCastFromTimestampNano(
      Long value, Type.TypeID target, Types.TimestampNanoType timestampNanoType) {
    switch (target) {
      case TIMESTAMP_NANO:
        return value;
      case LONG:
        if (value % NANOS_PER_MICRO == 0) {
          return DateTimeUtil.nanosToMicros(value);
        }
        return null;
      case TIMESTAMP:
        return DateTimeUtil.nanosToMicros(value);
      case DATE:
        int days = DateTimeUtil.nanosToDays(value);
        if (value.equals(Math.multiplyExact(days, NANOS_PER_DAY))) {
          return days;
        }
        return null;
      default:
        return null;
    }
  }

  @Override
  public String toString() {
    return "cast[" + targetType + "]";
  }

  @Override
  public String dedupName() {
    return "cast[" + targetType + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Cast<?, ?> cast = (Cast<?, ?>) o;
    return targetType.equals(cast.targetType);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(targetType);
  }

  /**
   * Checks if a cast from sourceType to targetType is valid.
   *
   * @param sourceType the source type
   * @param targetType the target type
   * @return true if the cast is valid, false otherwise
   */
  private static boolean isValidCast(Type sourceType, Type targetType) {
    Type.TypeID source = sourceType.typeId();
    Type.TypeID target = targetType.typeId();

    // Same type - always valid
    if (source == target) {
      return true;
    }

    switch (source) {
      case BOOLEAN:
        // Boolean can only cast to boolean or string
        return target == Type.TypeID.STRING;

      case INTEGER:
        // Integer can cast to wider numeric types, date, decimal, or string
        return target == Type.TypeID.LONG
            || target == Type.TypeID.FLOAT
            || target == Type.TypeID.DOUBLE
            || target == Type.TypeID.DATE
            || target == Type.TypeID.DECIMAL
            || target == Type.TypeID.STRING;

      case LONG:
        // Long can cast to numeric types, temporal types, decimal, or string
        return target == Type.TypeID.INTEGER
            || target == Type.TypeID.FLOAT
            || target == Type.TypeID.DOUBLE
            || target == Type.TypeID.TIME
            || target == Type.TypeID.TIMESTAMP
            || target == Type.TypeID.TIMESTAMP_NANO
            || target == Type.TypeID.DATE
            || target == Type.TypeID.DECIMAL
            || target == Type.TypeID.STRING;

      case FLOAT:
        // Float can cast to numeric types, decimal, or string
        return target == Type.TypeID.DOUBLE
            || target == Type.TypeID.DECIMAL
            || target == Type.TypeID.STRING;

      case DOUBLE:
        // Double can cast to numeric types, decimal, or string
        return target == Type.TypeID.FLOAT
            || target == Type.TypeID.DECIMAL
            || target == Type.TypeID.STRING;

      case DECIMAL:
        // Decimal can cast to numeric types or string
        return target == Type.TypeID.INTEGER
            || target == Type.TypeID.LONG
            || target == Type.TypeID.FLOAT
            || target == Type.TypeID.DOUBLE
            || target == Type.TypeID.DECIMAL
            || target == Type.TypeID.STRING;

      case DATE:
        // Date can cast to string, timestamp
        return target == Type.TypeID.STRING
            || target == Type.TypeID.TIMESTAMP
            || target == Type.TypeID.TIMESTAMP_NANO;

      case TIME:
        // Time can cast to string or long
        return target == Type.TypeID.STRING || target == Type.TypeID.LONG;

      case TIMESTAMP:
        // Timestamp can cast to date, timestamp_nano, or string
        return target == Type.TypeID.DATE
            || target == Type.TypeID.TIMESTAMP_NANO
            || target == Type.TypeID.STRING
            || target == Type.TypeID.LONG;

      case TIMESTAMP_NANO:
        // Timestamp nano can cast to date, timestamp, or string
        return target == Type.TypeID.DATE
            || target == Type.TypeID.TIMESTAMP
            || target == Type.TypeID.STRING
            || target == Type.TypeID.LONG;

      case STRING:
        // String can cast to many types (parsing)
        return target == Type.TypeID.BOOLEAN
            || target == Type.TypeID.INTEGER
            || target == Type.TypeID.LONG
            || target == Type.TypeID.FLOAT
            || target == Type.TypeID.DOUBLE
            || target == Type.TypeID.DECIMAL
            || target == Type.TypeID.DATE
            || target == Type.TypeID.TIME
            || target == Type.TypeID.TIMESTAMP
            || target == Type.TypeID.TIMESTAMP_NANO
            || target == Type.TypeID.UUID;

      case UUID:
        // UUID can cast to string
        return target == Type.TypeID.STRING;

      case FIXED:
        // Fixed can cast to binary of same or larger size
        return target == Type.TypeID.BINARY;

      case BINARY:
        // Binary can cast to fixed of exact size
        return target == Type.TypeID.FIXED;

      default:
        return false;
    }
  }

  /**
   * Function that performs the actual cast operation.
   *
   * @param <S> source type
   * @param <T> target type
   */
  private static class CastFunction<S, T> implements SerializableFunction<S, T> {
    private final Type sourceType;
    private final Type targetType;

    CastFunction(Type sourceType, Type targetType) {
      this.sourceType = sourceType;
      this.targetType = targetType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T apply(S value) {
      if (value == null) {
        return null;
      }

      Type.TypeID source = sourceType.typeId();
      Type.TypeID target = targetType.typeId();

      // Same type - return as-is (except for decimal which may have different scale)
      if (source == target && source != Type.TypeID.DECIMAL) {
        return (T) value;
      }

      try {
        switch (source) {
          case BOOLEAN:
            return castFromBoolean((Boolean) value, target);

          case INTEGER:
            return castFromInteger((Integer) value, target, targetType);

          case LONG:
            return castFromLong((Long) value, target, targetType);

          case FLOAT:
            return castFromFloat((Float) value, target, targetType);

          case DOUBLE:
            return castFromDouble((Double) value, target, targetType);

          case DECIMAL:
            return castFromDecimal((BigDecimal) value, target, targetType);

          case DATE:
            return castFromDate((Integer) value, target);

          case TIME:
            return castFromTime((Long) value, target);

          case TIMESTAMP:
            return castFromTimestamp((Long) value, target, targetType);

          case TIMESTAMP_NANO:
            return castFromTimestampNano((Long) value, target, targetType);

          case STRING:
            return castFromString((CharSequence) value, target, targetType);

          case UUID:
            return castFromUUID((java.util.UUID) value, target);

          case FIXED:
          case BINARY:
            return castFromBinary((ByteBuffer) value, target, targetType);

          default:
            throw new UnsupportedOperationException(
                "Cast from " + source + " to " + target + " is not supported");
        }
      } catch (Exception e) {
        // Cast failed - return null (similar to SQL CAST behavior)
        return null;
      }
    }

    @SuppressWarnings("unchecked")
    private T castFromBoolean(Boolean value, Type.TypeID target) {
      switch (target) {
        case STRING:
          return (T) String.valueOf(value);
        default:
          throw new UnsupportedOperationException("Cannot cast boolean to " + target);
      }
    }

    @SuppressWarnings("unchecked")
    private T castFromInteger(Integer value, Type.TypeID target, Type targetType) {
      switch (target) {
        case LONG:
          return (T) Long.valueOf(value.longValue());
        case FLOAT:
          return (T) Float.valueOf(value.floatValue());
        case DOUBLE:
          return (T) Double.valueOf(value.doubleValue());
        case DATE:
          return (T) value;
        case DECIMAL:
          Types.DecimalType decimalType = (Types.DecimalType) targetType;
          return (T) BigDecimal.valueOf(value).setScale(decimalType.scale(), RoundingMode.HALF_UP);
        case STRING:
          return (T) String.valueOf(value);
        default:
          throw new UnsupportedOperationException("Cannot cast integer to " + target);
      }
    }

    @SuppressWarnings("unchecked")
    private T castFromLong(Long value, Type.TypeID target, Type targetType) {
      switch (target) {
        case INTEGER:
          if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
            throw new ArithmeticException("Long value out of integer range");
          }
          return (T) Integer.valueOf(value.intValue());
        case FLOAT:
          return (T) Float.valueOf(value.floatValue());
        case DOUBLE:
          return (T) Double.valueOf(value.doubleValue());
        case TIME:
          return (T) value;
        case TIMESTAMP:
          return (T) value;
        case TIMESTAMP_NANO:
          return (T) Long.valueOf(DateTimeUtil.microsToNanos(value));
        case DATE:
          if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
            throw new ArithmeticException("Long value out of date range");
          }
          return (T) Integer.valueOf(value.intValue());
        case DECIMAL:
          Types.DecimalType decimalType = (Types.DecimalType) targetType;
          return (T) BigDecimal.valueOf(value).setScale(decimalType.scale(), RoundingMode.HALF_UP);
        case STRING:
          return (T) String.valueOf(value);
        default:
          throw new UnsupportedOperationException("Cannot cast long to " + target);
      }
    }

    @SuppressWarnings("unchecked")
    private T castFromFloat(Float value, Type.TypeID target, Type targetType) {
      switch (target) {
        case DOUBLE:
          return (T) Double.valueOf(value.doubleValue());
        case DECIMAL:
          Types.DecimalType decimalType = (Types.DecimalType) targetType;
          return (T) BigDecimal.valueOf(value).setScale(decimalType.scale(), RoundingMode.HALF_UP);
        case STRING:
          return (T) String.valueOf(value);
        default:
          throw new UnsupportedOperationException("Cannot cast float to " + target);
      }
    }

    @SuppressWarnings("unchecked")
    private T castFromDouble(Double value, Type.TypeID target, Type targetType) {
      switch (target) {
        case FLOAT:
          if (value > Float.MAX_VALUE || value < -Float.MAX_VALUE) {
            throw new ArithmeticException("Double value out of float range");
          }
          return (T) Float.valueOf(value.floatValue());
        case DECIMAL:
          Types.DecimalType decimalType = (Types.DecimalType) targetType;
          return (T) BigDecimal.valueOf(value).setScale(decimalType.scale(), RoundingMode.HALF_UP);
        case STRING:
          return (T) String.valueOf(value);
        default:
          throw new UnsupportedOperationException("Cannot cast double to " + target);
      }
    }

    @SuppressWarnings("unchecked")
    private T castFromDecimal(BigDecimal value, Type.TypeID target, Type targetType) {
      switch (target) {
        case INTEGER:
          return (T) Integer.valueOf(value.intValue());
        case LONG:
          return (T) Long.valueOf(value.longValue());
        case FLOAT:
          return (T) Float.valueOf(value.floatValue());
        case DOUBLE:
          return (T) Double.valueOf(value.doubleValue());
        case DECIMAL:
          Types.DecimalType decimalType = (Types.DecimalType) targetType;
          return (T) value.setScale(decimalType.scale(), RoundingMode.HALF_UP);
        case STRING:
          return (T) value.toString();
        default:
          throw new UnsupportedOperationException("Cannot cast decimal to " + target);
      }
    }

    @SuppressWarnings("unchecked")
    private T castFromDate(Integer value, Type.TypeID target) {
      switch (target) {
        case STRING:
          return (T) DateTimeUtil.daysToIsoDate(value);
        case TIMESTAMP:
          // Convert days to microseconds: days * 24 * 60 * 60 * 1_000_000
          return (T) Long.valueOf(value * 86400L * 1_000_000L);
        case TIMESTAMP_NANO:
          // Convert days to nanoseconds: days * 24 * 60 * 60 * 1_000_000_000
          return (T) Long.valueOf(value * 86400L * 1_000_000_000L);
        default:
          throw new UnsupportedOperationException("Cannot cast date to " + target);
      }
    }

    @SuppressWarnings("unchecked")
    private T castFromTime(Long value, Type.TypeID target) {
      switch (target) {
        case STRING:
          return (T) DateTimeUtil.microsToIsoTime(value);
        case LONG:
          return (T) value;
        default:
          throw new UnsupportedOperationException("Cannot cast time to " + target);
      }
    }

    @SuppressWarnings({"unchecked", "UnusedVariable"})
    private T castFromTimestamp(Long value, Type.TypeID target, Type targetType) {
      switch (target) {
        case DATE:
          return (T) Integer.valueOf(DateTimeUtil.microsToDays(value));
        case TIMESTAMP_NANO:
          return (T) Long.valueOf(DateTimeUtil.microsToNanos(value));
        case LONG:
          return (T) value;
        case STRING:
          Types.TimestampType timestampType = (Types.TimestampType) sourceType;
          if (timestampType.shouldAdjustToUTC()) {
            return (T) DateTimeUtil.microsToIsoTimestamptz(value);
          } else {
            return (T) DateTimeUtil.microsToIsoTimestamp(value);
          }
        default:
          throw new UnsupportedOperationException("Cannot cast timestamp to " + target);
      }
    }

    @SuppressWarnings({"unchecked", "UnusedVariable"})
    private T castFromTimestampNano(Long value, Type.TypeID target, Type targetType) {
      switch (target) {
        case DATE:
          return (T) Integer.valueOf(DateTimeUtil.nanosToDays(value));
        case TIMESTAMP:
          return (T) Long.valueOf(DateTimeUtil.nanosToMicros(value));
        case LONG:
          return (T) value;
        case STRING:
          Types.TimestampNanoType timestampType = (Types.TimestampNanoType) sourceType;
          if (timestampType.shouldAdjustToUTC()) {
            return (T) DateTimeUtil.nanosToIsoTimestamptz(value);
          } else {
            return (T) DateTimeUtil.nanosToIsoTimestamp(value);
          }
        default:
          throw new UnsupportedOperationException("Cannot cast timestamp_nano to " + target);
      }
    }

    @SuppressWarnings("unchecked")
    private T castFromString(CharSequence value, Type.TypeID target, Type targetType) {
      String str = value.toString().trim();
      switch (target) {
        case BOOLEAN:
          return (T) Boolean.valueOf(str);
        case INTEGER:
          return (T) Integer.valueOf(str);
        case LONG:
          return (T) Long.valueOf(str);
        case FLOAT:
          return (T) Float.valueOf(str);
        case DOUBLE:
          return (T) Double.valueOf(str);
        case DECIMAL:
          return (T) new BigDecimal(str);
        case DATE:
          return (T) Integer.valueOf(DateTimeUtil.isoDateToDays(str));
        case TIME:
          return (T) Long.valueOf(DateTimeUtil.isoTimeToMicros(str));
        case TIMESTAMP:
          Types.TimestampType timestampType = (Types.TimestampType) targetType;
          if (timestampType.shouldAdjustToUTC()) {
            return (T) Long.valueOf(DateTimeUtil.isoTimestamptzToMicros(str));
          } else {
            return (T) Long.valueOf(DateTimeUtil.isoTimestampToMicros(str));
          }
        case TIMESTAMP_NANO:
          Types.TimestampNanoType timestampNanoType = (Types.TimestampNanoType) targetType;
          if (timestampNanoType.shouldAdjustToUTC()) {
            return (T) Long.valueOf(DateTimeUtil.isoTimestamptzToNanos(str));
          } else {
            return (T) Long.valueOf(DateTimeUtil.isoTimestampToNanos(str));
          }
        case UUID:
          return (T) java.util.UUID.fromString(str);
        default:
          throw new UnsupportedOperationException("Cannot cast string to " + target);
      }
    }

    @SuppressWarnings("unchecked")
    private T castFromUUID(java.util.UUID value, Type.TypeID target) {
      switch (target) {
        case STRING:
          return (T) value.toString();
        default:
          throw new UnsupportedOperationException("Cannot cast uuid to " + target);
      }
    }

    @SuppressWarnings("unchecked")
    private T castFromBinary(ByteBuffer value, Type.TypeID target, Type targetType) {
      switch (target) {
        case BINARY:
          return (T) value;
        case FIXED:
          Types.FixedType fixedType = (Types.FixedType) targetType;
          if (value.remaining() != fixedType.length()) {
            throw new IllegalArgumentException(
                "Binary length "
                    + value.remaining()
                    + " does not match fixed length "
                    + fixedType.length());
          }
          return (T) value;
        default:
          throw new UnsupportedOperationException("Cannot cast binary to " + target);
      }
    }
  }
}
