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

enum Timestamps implements Transform<Long, Integer> {
  MICROS_TO_YEAR(ChronoUnit.YEARS, "year", TimestampUnit.MICROS),
  MICROS_TO_MONTH(ChronoUnit.MONTHS, "month", TimestampUnit.MICROS),
  MICROS_TO_DAY(ChronoUnit.DAYS, "day", TimestampUnit.MICROS),
  MICROS_TO_HOUR(ChronoUnit.HOURS, "hour", TimestampUnit.MICROS),

  NANOS_TO_YEAR(ChronoUnit.YEARS, "year", TimestampUnit.NANOS),
  NANOS_TO_MONTH(ChronoUnit.MONTHS, "month", TimestampUnit.NANOS),
  NANOS_TO_DAY(ChronoUnit.DAYS, "day", TimestampUnit.NANOS),
  NANOS_TO_HOUR(ChronoUnit.HOURS, "hour", TimestampUnit.NANOS);

  enum TimestampUnit {
    MICROS,
    NANOS
  }

  @Immutable
  static class Apply implements SerializableFunction<Long, Integer> {
    private final ChronoUnit granularity;
    private final TimestampUnit timestampUnit;

    Apply(ChronoUnit granularity, TimestampUnit timestampUnit) {
      this.granularity = granularity;
      this.timestampUnit = timestampUnit;
    }

    @Override
    public Integer apply(Long timestamp) {
      if (timestamp == null) {
        return null;
      }

      switch (timestampUnit) {
        case MICROS:
          switch (granularity) {
            case YEARS:
              return DateTimeUtil.microsToYears(timestamp);
            case MONTHS:
              return DateTimeUtil.microsToMonths(timestamp);
            case DAYS:
              return DateTimeUtil.microsToDays(timestamp);
            case HOURS:
              return DateTimeUtil.microsToHours(timestamp);
            default:
              throw new UnsupportedOperationException("Unsupported time unit: " + granularity);
          }
        case NANOS:
          switch (granularity) {
            case YEARS:
              return DateTimeUtil.nanosToYears(timestamp);
            case MONTHS:
              return DateTimeUtil.nanosToMonths(timestamp);
            case DAYS:
              return DateTimeUtil.nanosToDays(timestamp);
            case HOURS:
              return DateTimeUtil.nanosToHours(timestamp);
            default:
              throw new UnsupportedOperationException("Unsupported time unit: " + granularity);
          }
        default:
          throw new UnsupportedOperationException("Unsupported time unit: " + timestampUnit);
      }
    }
  }

  private final ChronoUnit granularity;
  private final String name;
  private final Apply apply;

  Timestamps(ChronoUnit granularity, String name, TimestampUnit timestampUnit) {
    this.name = name;
    this.granularity = granularity;
    this.apply = new Apply(granularity, timestampUnit);
  }

  /**
   * Transforms a value to its corresponding partition value.
   *
   * @param timestamp a source value
   * @return a transformed partition value
   * @deprecated will be removed in 2.0.0; use {@link #bind(Type)} instead
   */
  @Deprecated
  @Override
  public Integer apply(Long timestamp) {
    return apply.apply(timestamp);
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
    if (granularity == ChronoUnit.DAYS) {
      return Types.DateType.get();
    }
    return Types.IntegerType.get();
  }

  ChronoUnit granularity() {
    return granularity;
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

    if (other instanceof Dates) {
      return TransformUtil.satisfiesOrderOf(granularity, ((Dates) other).granularity());
    } else if (other instanceof Timestamps) {
      return TransformUtil.satisfiesOrderOf(granularity, ((Timestamps) other).granularity());
    } else if (other instanceof TimeTransform) {
      return TransformUtil.satisfiesOrderOf(granularity, ((TimeTransform<?>) other).granularity());
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

    switch (granularity) {
      case YEARS:
        return TransformUtil.humanYear(value);
      case MONTHS:
        return TransformUtil.humanMonth(value);
      case DAYS:
        return TransformUtil.humanDay(value);
      case HOURS:
        return TransformUtil.humanHour(value);
      default:
        throw new UnsupportedOperationException("Unsupported time unit: " + granularity);
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
