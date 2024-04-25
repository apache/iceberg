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

import com.google.errorprone.annotations.Immutable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundTransform;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.SerializableFunction;

class Timestamps implements Transform<Long, Integer> {

  static final Timestamps YEAR_FROM_MICROS =
      new Timestamps(ChronoUnit.MICROS, ResultTypeUnit.YEARS, "year");
  static final Timestamps MONTH_FROM_MICROS =
      new Timestamps(ChronoUnit.MICROS, ResultTypeUnit.MONTHS, "month");
  static final Timestamps DAY_FROM_MICROS =
      new Timestamps(ChronoUnit.MICROS, ResultTypeUnit.DAYS, "day");
  static final Timestamps HOUR_FROM_MICROS =
      new Timestamps(ChronoUnit.MICROS, ResultTypeUnit.HOURS, "hour");
  static final Timestamps YEAR_FROM_NANOS =
      new Timestamps(ChronoUnit.NANOS, ResultTypeUnit.YEARS, "year");
  static final Timestamps MONTH_FROM_NANOS =
      new Timestamps(ChronoUnit.NANOS, ResultTypeUnit.MONTHS, "month");
  static final Timestamps DAY_FROM_NANOS =
      new Timestamps(ChronoUnit.NANOS, ResultTypeUnit.DAYS, "day");
  static final Timestamps HOUR_FROM_NANOS =
      new Timestamps(ChronoUnit.NANOS, ResultTypeUnit.HOURS, "hour");

  static Timestamps get(Types.TimestampType type, String resultTypeUnit) {
    switch (resultTypeUnit.toLowerCase(Locale.ENGLISH)) {
      case "year":
        return get(type, ChronoUnit.YEARS);
      case "month":
        return get(type, ChronoUnit.MONTHS);
      case "day":
        return get(type, ChronoUnit.DAYS);
      case "hour":
        return get(type, ChronoUnit.HOURS);
      default:
        throw new IllegalArgumentException(
            "Unsupported source/result type units: " + type + "->" + resultTypeUnit);
    }
  }

  static Timestamps get(Types.TimestampNanoType type, String resultTypeUnit) {
    switch (resultTypeUnit.toLowerCase(Locale.ENGLISH)) {
      case "year":
        return get(type, ChronoUnit.YEARS);
      case "month":
        return get(type, ChronoUnit.MONTHS);
      case "day":
        return get(type, ChronoUnit.DAYS);
      case "hour":
        return get(type, ChronoUnit.HOURS);
      default:
        throw new IllegalArgumentException(
            "Unsupported source/result type units: " + type + "->" + resultTypeUnit);
    }
  }

  static Timestamps get(Types.TimestampType type, ChronoUnit resultTypeUnit) {
    if (type.typeId() != Type.TypeID.TIMESTAMP) {
      throw new UnsupportedOperationException("Unsupported timestamp unit: " + type);
    }
    switch (resultTypeUnit) {
      case YEARS:
        return YEAR_FROM_MICROS;
      case MONTHS:
        return MONTH_FROM_MICROS;
      case DAYS:
        return DAY_FROM_MICROS;
      case HOURS:
        return HOUR_FROM_MICROS;
      default:
        throw new IllegalArgumentException(
            "Unsupported source/result type units: " + type + "->" + resultTypeUnit);
    }
  }

  static Timestamps get(Types.TimestampNanoType type, ChronoUnit resultTypeUnit) {
    if (type.typeId() != Type.TypeID.TIMESTAMP_NANO) {
      throw new UnsupportedOperationException("Unsupported timestamp unit: " + type);
    }
    switch (resultTypeUnit) {
      case YEARS:
        return YEAR_FROM_NANOS;
      case MONTHS:
        return MONTH_FROM_NANOS;
      case DAYS:
        return DAY_FROM_NANOS;
      case HOURS:
        return HOUR_FROM_NANOS;
      default:
        throw new IllegalArgumentException(
            "Unsupported source/result type units: " + type + "->" + resultTypeUnit);
    }
  }

  enum ResultTypeUnit {
    YEARS(ChronoUnit.YEARS),
    MONTHS(ChronoUnit.MONTHS),
    DAYS(ChronoUnit.DAYS),
    HOURS(ChronoUnit.HOURS),
    MICROS(ChronoUnit.MICROS),
    NANOS(ChronoUnit.NANOS),
    ;

    private final ChronoUnit unit;

    ResultTypeUnit(final ChronoUnit unit) {
      this.unit = unit;
    }

    Duration getDuration() {
      return unit.getDuration();
    }
  }

  @Immutable
  static class Apply implements SerializableFunction<Long, Integer> {
    private final ChronoUnit sourceTypeUnit;
    private final ResultTypeUnit resultTypeUnit;

    Apply(ChronoUnit sourceTypeUnit, ResultTypeUnit resultTypeUnit) {
      this.sourceTypeUnit = sourceTypeUnit;
      this.resultTypeUnit = resultTypeUnit;
    }

    @Override
    public Integer apply(Long timestampUnits) {
      if (timestampUnits == null) {
        return null;
      }

      switch (sourceTypeUnit) {
        case MICROS:
          switch (resultTypeUnit) {
            case YEARS:
              return DateTimeUtil.microsToYears(timestampUnits);
            case MONTHS:
              return DateTimeUtil.microsToMonths(timestampUnits);
            case DAYS:
              return DateTimeUtil.microsToDays(timestampUnits);
            case HOURS:
              return DateTimeUtil.microsToHours(timestampUnits);
            default:
              throw new UnsupportedOperationException(
                  "Unsupported result type unit: " + resultTypeUnit);
          }
        case NANOS:
          return DateTimeUtil.convertNanos(timestampUnits, resultTypeUnit.unit);
        default:
          throw new UnsupportedOperationException(
              "Unsupported source type unit: " + sourceTypeUnit);
      }
    }
  }

  private final String name;
  private final Apply apply;

  Timestamps(ChronoUnit sourceTypeUnit, ResultTypeUnit resultTypeUnit, String name) {
    this.name = name;
    this.apply = new Apply(sourceTypeUnit, resultTypeUnit);
  }

  @Override
  public Integer apply(Long timestampUnits) {
    return apply.apply(timestampUnits);
  }

  @Override
  public SerializableFunction<Long, Integer> bind(Type type) {
    Preconditions.checkArgument(canTransform(type), "Cannot bind to unsupported type: %s", type);
    return apply;
  }

  @Override
  public boolean canTransform(Type type) {
    return type.typeId() == Type.TypeID.TIMESTAMP || type.typeId() == Type.TypeID.TIMESTAMP_NANO;
  }

  @Override
  public Type getResultType(Type sourceType) {
    if (apply.resultTypeUnit == ResultTypeUnit.DAYS) {
      return Types.DateType.get();
    }
    return Types.IntegerType.get();
  }

  ResultTypeUnit resultTypeUnit() {
    return apply.resultTypeUnit;
  }

  @Override
  public boolean preservesOrder() {
    return true;
  }

  @Override
  public boolean satisfiesOrderOf(Transform<?, ?> other) {
    if (this == other) {
      return true;
    }

    if (other instanceof Timestamps) {
      // test the granularity, in hours. hour(ts) => 1 hour, day(ts) => 24 hours, and hour satisfies
      // the order of day
      Timestamps otherTransform = (Timestamps) other;
      return apply.resultTypeUnit.getDuration().toHours()
          <= otherTransform.apply.resultTypeUnit.getDuration().toHours();
    }

    return false;
  }

  @Override
  public UnboundPredicate<Integer> project(String fieldName, BoundPredicate<Long> pred) {
    if (pred.term() instanceof BoundTransform) {
      return ProjectionUtil.projectTransformPredicate(this, fieldName, pred);
    }

    if (pred.isUnaryPredicate()) {
      return Expressions.predicate(pred.op(), fieldName);

    } else if (pred.isLiteralPredicate()) {
      UnboundPredicate<Integer> projected =
          ProjectionUtil.truncateLong(fieldName, pred.asLiteralPredicate(), apply);
      return ProjectionUtil.fixInclusiveTimeProjection(projected);

    } else if (pred.isSetPredicate() && pred.op() == Expression.Operation.IN) {
      UnboundPredicate<Integer> projected =
          ProjectionUtil.transformSet(fieldName, pred.asSetPredicate(), apply);
      return ProjectionUtil.fixInclusiveTimeProjection(projected);
    }

    return null;
  }

  @Override
  public UnboundPredicate<Integer> projectStrict(String fieldName, BoundPredicate<Long> pred) {
    if (pred.term() instanceof BoundTransform) {
      return ProjectionUtil.projectTransformPredicate(this, fieldName, pred);
    }

    if (pred.isUnaryPredicate()) {
      return Expressions.predicate(pred.op(), fieldName);

    } else if (pred.isLiteralPredicate()) {
      UnboundPredicate<Integer> projected =
          ProjectionUtil.truncateLongStrict(fieldName, pred.asLiteralPredicate(), apply);
      return ProjectionUtil.fixStrictTimeProjection(projected);

    } else if (pred.isSetPredicate() && pred.op() == Expression.Operation.NOT_IN) {
      UnboundPredicate<Integer> projected =
          ProjectionUtil.transformSet(fieldName, pred.asSetPredicate(), apply);
      return ProjectionUtil.fixStrictTimeProjection(projected);
    }

    return null;
  }

  @Override
  public String toHumanString(Type outputType, Integer value) {
    if (value == null) {
      return "null";
    }

    switch (apply.resultTypeUnit) {
      case YEARS:
        return TransformUtil.humanYear(value);
      case MONTHS:
        return TransformUtil.humanMonth(value);
      case DAYS:
        return TransformUtil.humanDay(value);
      case HOURS:
        return TransformUtil.humanHour(value);
      default:
        throw new UnsupportedOperationException("Unsupported time unit: " + apply.resultTypeUnit);
    }
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public String dedupName() {
    return "time";
  }
}
