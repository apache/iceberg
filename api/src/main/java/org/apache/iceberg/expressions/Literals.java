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

package org.apache.iceberg.expressions;

import com.google.common.base.Preconditions;
import java.io.ObjectStreamException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.UUID;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

class Literals {
  private Literals() {
  }

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  /**
   * Create a {@link Literal} from an Object.
   *
   * @param value a value
   * @param <T> Java type of value
   * @return a Literal for the given value
   */
  @SuppressWarnings("unchecked")
  static <T> Literal<T> from(T value) {
    Preconditions.checkNotNull(value, "Cannot create expression literal from null");

    if (value instanceof Boolean) {
      return (Literal<T>) new Literals.BooleanLiteral((Boolean) value);
    } else if (value instanceof Integer) {
      return (Literal<T>) new Literals.IntegerLiteral((Integer) value);
    } else if (value instanceof Long) {
      return (Literal<T>) new Literals.LongLiteral((Long) value);
    } else if (value instanceof Float) {
      return (Literal<T>) new Literals.FloatLiteral((Float) value);
    } else if (value instanceof Double) {
      return (Literal<T>) new Literals.DoubleLiteral((Double) value);
    } else if (value instanceof CharSequence) {
      return (Literal<T>) new Literals.StringLiteral((CharSequence) value);
    } else if (value instanceof UUID) {
      return (Literal<T>) new Literals.UUIDLiteral((UUID) value);
    } else if (value instanceof byte[]) {
      return (Literal<T>) new Literals.FixedLiteral(ByteBuffer.wrap((byte[]) value));
    } else if (value instanceof ByteBuffer) {
      return (Literal<T>) new Literals.BinaryLiteral((ByteBuffer) value);
    } else if (value instanceof BigDecimal) {
      return (Literal<T>) new Literals.DecimalLiteral((BigDecimal) value);
    }

    throw new IllegalArgumentException(String.format(
        "Cannot create expression literal from %s: %s", value.getClass().getName(), value));
  }

  @SuppressWarnings("unchecked")
  static <T> AboveMax<T> aboveMax() {
    return AboveMax.INSTANCE;
  }

  @SuppressWarnings("unchecked")
  static <T> BelowMin<T> belowMin() {
    return BelowMin.INSTANCE;
  }

  private abstract static class BaseLiteral<T> implements Literal<T> {
    private final T value;

    BaseLiteral(T value) {
      Preconditions.checkNotNull(value, "Literal values cannot be null");
      this.value = value;
    }

    @Override
    public T value() {
      return value;
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }
  }

  private abstract static class ComparableLiteral<C extends Comparable<C>> extends BaseLiteral<C> {
    @SuppressWarnings("unchecked")
    private static final Comparator<? extends Comparable> CMP =
        Comparators.<Comparable>nullsFirst().thenComparing(Comparator.naturalOrder());

    ComparableLiteral(C value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Comparator<C> comparator() {
      return (Comparator<C>) CMP;
    }
  }

  static class AboveMax<T> implements Literal<T> {
    private static final AboveMax INSTANCE = new AboveMax();

    private AboveMax() {
    }

    @Override
    public T value() {
      throw new UnsupportedOperationException("AboveMax has no value");
    }

    @Override
    public <X> Literal<X> to(Type type) {
      throw new UnsupportedOperationException("Cannot change the type of AboveMax");
    }

    @Override
    public Comparator<T> comparator() {
      throw new UnsupportedOperationException("AboveMax has no comparator");
    }

    @Override
    public String toString() {
      return "aboveMax";
    }
  }

  static class BelowMin<T> implements Literal<T> {
    private static final BelowMin INSTANCE = new BelowMin();

    private BelowMin() {
    }

    @Override
    public T value() {
      throw new UnsupportedOperationException("BelowMin has no value");
    }

    @Override
    public <X> Literal<X> to(Type type) {
      throw new UnsupportedOperationException("Cannot change the type of BelowMin");
    }

    @Override
    public Comparator<T> comparator() {
      throw new UnsupportedOperationException("BelowMin has no comparator");
    }

    @Override
    public String toString() {
      return "belowMin";
    }
  }

  static class BooleanLiteral extends ComparableLiteral<Boolean> {
    BooleanLiteral(Boolean value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Literal<T> to(Type type) {
      if (type.typeId() == Type.TypeID.BOOLEAN) {
        return (Literal<T>) this;
      }
      return null;
    }
  }

  static class IntegerLiteral extends ComparableLiteral<Integer> {
    IntegerLiteral(Integer value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Literal<T> to(Type type) {
      switch (type.typeId()) {
        case INTEGER:
          return (Literal<T>) this;
        case LONG:
          return (Literal<T>) new LongLiteral(value().longValue());
        case FLOAT:
          return (Literal<T>) new FloatLiteral(value().floatValue());
        case DOUBLE:
          return (Literal<T>) new DoubleLiteral(value().doubleValue());
        case DATE:
          return (Literal<T>) new DateLiteral(value());
        case DECIMAL:
          int scale = ((Types.DecimalType) type).scale();
          // rounding mode isn't necessary, but pass one to avoid warnings
          return (Literal<T>) new DecimalLiteral(
              BigDecimal.valueOf(value()).setScale(scale, RoundingMode.HALF_UP));
        default:
          return null;
      }
    }
  }

  static class LongLiteral extends ComparableLiteral<Long> {
    LongLiteral(Long value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Literal<T> to(Type type) {
      switch (type.typeId()) {
        case INTEGER:
          if ((long) Integer.MAX_VALUE < value()) {
            return aboveMax();
          } else if ((long) Integer.MIN_VALUE > value()) {
            return belowMin();
          }
          return (Literal<T>) new IntegerLiteral(value().intValue());
        case LONG:
          return (Literal<T>) this;
        case FLOAT:
          return (Literal<T>) new FloatLiteral(value().floatValue());
        case DOUBLE:
          return (Literal<T>) new DoubleLiteral(value().doubleValue());
        case TIME:
          return (Literal<T>) new TimeLiteral(value());
        case TIMESTAMP:
          return (Literal<T>) new TimestampLiteral(value());
        case DECIMAL:
          int scale = ((Types.DecimalType) type).scale();
          // rounding mode isn't necessary, but pass one to avoid warnings
          return (Literal<T>) new DecimalLiteral(
              BigDecimal.valueOf(value()).setScale(scale, RoundingMode.HALF_UP));
        default:
          return null;
      }
    }
  }

  static class FloatLiteral extends ComparableLiteral<Float> {
    FloatLiteral(Float value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Literal<T> to(Type type) {
      switch (type.typeId()) {
        case FLOAT:
          return (Literal<T>) this;
        case DOUBLE:
          return (Literal<T>) new DoubleLiteral(value().doubleValue());
        case DECIMAL:
          int scale = ((Types.DecimalType) type).scale();
          return (Literal<T>) new DecimalLiteral(
              BigDecimal.valueOf(value()).setScale(scale, RoundingMode.HALF_UP));
        default:
          return null;
      }
    }
  }

  static class DoubleLiteral extends ComparableLiteral<Double> {
    DoubleLiteral(Double value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Literal<T> to(Type type) {
      switch (type.typeId()) {
        case FLOAT:
          if ((double) Float.MAX_VALUE < value()) {
            return aboveMax();
          } else if ((double) -Float.MAX_VALUE > value()) {
            // Compare with -Float.MAX_VALUE because it is the most negative float value.
            // Float.MIN_VALUE is the smallest non-negative floating point value.
            return belowMin();
          }
          return (Literal<T>) new FloatLiteral(value().floatValue());
        case DOUBLE:
          return (Literal<T>) this;
        case DECIMAL:
          int scale = ((Types.DecimalType) type).scale();
          return (Literal<T>) new DecimalLiteral(
              BigDecimal.valueOf(value()).setScale(scale, RoundingMode.HALF_UP));
        default:
          return null;
      }
    }
  }

  static class DateLiteral extends ComparableLiteral<Integer> {
    DateLiteral(Integer value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Literal<T> to(Type type) {
      if (type.typeId() == Type.TypeID.DATE) {
        return (Literal<T>) this;
      }
      return null;
    }
  }

  static class TimeLiteral extends ComparableLiteral<Long> {
    TimeLiteral(Long value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Literal<T> to(Type type) {
      if (type.typeId() == Type.TypeID.TIME) {
        return (Literal<T>) this;
      }
      return null;
    }
  }

  static class TimestampLiteral extends ComparableLiteral<Long> {
    TimestampLiteral(Long value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Literal<T> to(Type type) {
      switch (type.typeId()) {
        case TIMESTAMP:
          return (Literal<T>) this;
        case DATE:
          return (Literal<T>) new DateLiteral((int) ChronoUnit.DAYS.between(
              EPOCH_DAY, EPOCH.plus(value(), ChronoUnit.MICROS).toLocalDate()));
        default:
      }
      return null;
    }
  }

  static class DecimalLiteral extends ComparableLiteral<BigDecimal> {
    DecimalLiteral(BigDecimal value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Literal<T> to(Type type) {
      switch (type.typeId()) {
        case DECIMAL:
          // do not change decimal scale
          if (value().scale() == ((Types.DecimalType) type).scale()) {
            return (Literal<T>) this;
          }
          return null;
        default:
          return null;
      }
    }
  }

  static class StringLiteral extends BaseLiteral<CharSequence> {
    private static final Comparator<CharSequence> CMP =
        Comparators.<CharSequence>nullsFirst().thenComparing(Comparators.charSequences());

    StringLiteral(CharSequence value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Literal<T> to(Type type) {
      switch (type.typeId()) {
        case DATE:
          int date = (int) ChronoUnit.DAYS.between(EPOCH_DAY,
              LocalDate.parse(value(), DateTimeFormatter.ISO_LOCAL_DATE));
          return (Literal<T>) new DateLiteral(date);

        case TIME:
          long timeMicros = LocalTime.parse(value(), DateTimeFormatter.ISO_LOCAL_TIME)
              .toNanoOfDay() / 1000;
          return (Literal<T>) new TimeLiteral(timeMicros);

        case TIMESTAMP:
          if (((Types.TimestampType) type).shouldAdjustToUTC()) {
            long timestampMicros = ChronoUnit.MICROS.between(EPOCH,
                OffsetDateTime.parse(value(), DateTimeFormatter.ISO_DATE_TIME));
            return (Literal<T>) new TimestampLiteral(timestampMicros);
          } else {
            long timestampMicros = ChronoUnit.MICROS.between(EPOCH,
                LocalDateTime.parse(value(), DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                    .atOffset(ZoneOffset.UTC));
            return (Literal<T>) new TimestampLiteral(timestampMicros);
          }

        case STRING:
          return (Literal<T>) this;

        case UUID:
          return (Literal<T>) new UUIDLiteral(UUID.fromString(value().toString()));

        case DECIMAL:
          int scale = ((Types.DecimalType) type).scale();
          BigDecimal decimal = new BigDecimal(value().toString());
          if (scale == decimal.scale()) {
            return (Literal<T>) new DecimalLiteral(decimal);
          }
          return null;

        default:
          return null;
      }
    }

    @Override
    public Comparator<CharSequence> comparator() {
      return CMP;
    }

    @Override
    public String toString() {
      return "\"" + value() + "\"";
    }
  }

  static class UUIDLiteral extends ComparableLiteral<UUID> {
    UUIDLiteral(UUID value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Literal<T> to(Type type) {
      if (type.typeId() == Type.TypeID.UUID) {
        return (Literal<T>) this;
      }
      return null;
    }
  }

  static class FixedLiteral extends BaseLiteral<ByteBuffer> {
    private static final Comparator<ByteBuffer> CMP =
        Comparators.<ByteBuffer>nullsFirst().thenComparing(Comparators.unsignedBytes());

    FixedLiteral(ByteBuffer value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Literal<T> to(Type type) {
      switch (type.typeId()) {
        case FIXED:
          Types.FixedType fixed = (Types.FixedType) type;
          if (value().remaining() == fixed.length()) {
            return (Literal<T>) this;
          }
          return null;
        case BINARY:
          return (Literal<T>) new BinaryLiteral(value());
        default:
          return null;
      }
    }

    @Override
    public Comparator<ByteBuffer> comparator() {
      return CMP;
    }

    Object writeReplace() throws ObjectStreamException {
      return new SerializationProxies.FixedLiteralProxy(value());
    }
  }

  static class BinaryLiteral extends BaseLiteral<ByteBuffer> {
    private static final Comparator<ByteBuffer> CMP =
        Comparators.<ByteBuffer>nullsFirst().thenComparing(Comparators.unsignedBytes());

    BinaryLiteral(ByteBuffer value) {
      super(value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Literal<T> to(Type type) {
      switch (type.typeId()) {
        case FIXED:
          Types.FixedType fixed = (Types.FixedType) type;
          if (value().remaining() == fixed.length()) {
            return (Literal<T>) new FixedLiteral(value());
          }
          return null;
        case BINARY:
          return (Literal<T>) this;
        default:
          return null;
      }
    }

    @Override
    public Comparator<ByteBuffer> comparator() {
      return CMP;
    }

    Object writeReplace() throws ObjectStreamException {
      return new SerializationProxies.BinaryLiteralProxy(value());
    }
  }
}
