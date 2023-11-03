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
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.SerializableFunction;

class Timestamps implements Transform<Long, Integer> {

  static final Timestamps YEAR_FROM_MICROS = new Timestamps(ChronoUnit.MICROS, ChronoUnit.YEARS);
  static final Timestamps MONTH_FROM_MICROS = new Timestamps(ChronoUnit.MICROS, ChronoUnit.MONTHS);
  static final Timestamps DAY_FROM_MICROS = new Timestamps(ChronoUnit.MICROS, ChronoUnit.DAYS);
  static final Timestamps HOUR_FROM_MICROS = new Timestamps(ChronoUnit.MICROS, ChronoUnit.HOURS);
  static final Timestamps YEAR_FROM_NANOS = new Timestamps(ChronoUnit.NANOS, ChronoUnit.YEARS);
  static final Timestamps MONTH_FROM_NANOS = new Timestamps(ChronoUnit.NANOS, ChronoUnit.MONTHS);
  static final Timestamps DAY_FROM_NANOS = new Timestamps(ChronoUnit.NANOS, ChronoUnit.DAYS);
  static final Timestamps HOUR_FROM_NANOS = new Timestamps(ChronoUnit.NANOS, ChronoUnit.HOURS);

  static Timestamps get(TimestampType type, String resultTypeUnit) {
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

  static Timestamps get(TimestampType type, ChronoUnit resultTypeUnit) {
    switch (type.unit()) {
      case MICROS:
        switch (resultTypeUnit) {
          case YEARS:
            return YEAR_FROM_MICROS;
          case MONTHS:
            return MONTH_FROM_MICROS;
          case DAYS:
            return DAY_FROM_MICROS;
          case HOURS:
            return HOUR_FROM_MICROS;
        }
        break;
      case NANOS:
        switch (resultTypeUnit) {
          case YEARS:
            return YEAR_FROM_NANOS;
          case MONTHS:
            return MONTH_FROM_NANOS;
          case DAYS:
            return DAY_FROM_NANOS;
          case HOURS:
            return HOUR_FROM_NANOS;
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported timestamp unit: " + type.unit());
    }
    throw new IllegalArgumentException(
        "Unsupported source/result type units: " + type + "->" + resultTypeUnit);
  }

  @Immutable
  static class Apply implements SerializableFunction<Long, Integer> {
    private final ChronoUnit sourceTypeUnit;
    private final ChronoUnit resultTypeUnit;

    Apply(ChronoUnit sourceTypeUnit, ChronoUnit resultTypeUnit) {
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
          switch (resultTypeUnit) {
            case YEARS:
              return DateTimeUtil.nanosToYears(timestampUnits);
            case MONTHS:
              return DateTimeUtil.nanosToMonths(timestampUnits);
            case DAYS:
              return DateTimeUtil.nanosToDays(timestampUnits);
            case HOURS:
              return DateTimeUtil.nanosToHours(timestampUnits);
            default:
              throw new UnsupportedOperationException(
                  "Unsupported result type unit: " + resultTypeUnit);
          }
        default:
          throw new UnsupportedOperationException(
              "Unsupported source type unit: " + sourceTypeUnit);
      }
    }
  }

  private final Apply apply;

  Timestamps(ChronoUnit sourceTypeUnit, ChronoUnit resultTypeUnit) {
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
    return type.typeId() == Type.TypeID.TIMESTAMP;
  }

  @Override
  public Type getResultType(Type sourceType) {
    if (apply.resultTypeUnit == ChronoUnit.DAYS) {
      return Types.DateType.get();
    }
    return Types.IntegerType.get();
  }

  public ChronoUnit getResultTypeUnit() {
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
      // test the granularity, in hours. hour(ts) => 1 hour, day(ts) => 24 hours, and
      // hour satisfies the order of day
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
    switch (apply.resultTypeUnit) {
      case YEARS:
        return "year";
      case MONTHS:
        return "month";
      case DAYS:
        return "day";
      case HOURS:
        return "hour";
      default:
        throw new UnsupportedOperationException(
            "Unsupported result time unit: " + apply.resultTypeUnit);
    }
  }

  @Override
  public String dedupName() {
    return "time";
  }
}
