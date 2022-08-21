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

import java.io.Serializable;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.function.Function;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundTransform;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

enum Timestamps implements Transform<Long, Integer> {
  YEAR(ChronoUnit.YEARS, "year"),
  MONTH(ChronoUnit.MONTHS, "month"),
  DAY(ChronoUnit.DAYS, "day"),
  HOUR(ChronoUnit.HOURS, "hour");

  static class Apply implements Function<Long, Integer>, Serializable {
    private final ChronoUnit granularity;

    Apply(ChronoUnit granularity) {
      this.granularity = granularity;
    }

    @Override
    public Integer apply(Long timestampMicros) {
      if (timestampMicros == null) {
        return null;
      }

      if (timestampMicros >= 0) {
        OffsetDateTime timestamp =
            Instant.ofEpochSecond(
                    Math.floorDiv(timestampMicros, 1_000_000),
                    Math.floorMod(timestampMicros, 1_000_000) * 1000)
                .atOffset(ZoneOffset.UTC);
        return (int) granularity.between(EPOCH, timestamp);
      } else {
        // add 1 micro to the value to account for the case where there is exactly 1 unit between
        // the
        // timestamp and epoch
        // because the result will always be decremented.
        OffsetDateTime timestamp =
            Instant.ofEpochSecond(
                    Math.floorDiv(timestampMicros, 1_000_000),
                    Math.floorMod(timestampMicros + 1, 1_000_000) * 1000)
                .atOffset(ZoneOffset.UTC);
        return (int) granularity.between(EPOCH, timestamp) - 1;
      }
    }
  }

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private final ChronoUnit granularity;
  private final String name;
  private final Function<Long, Integer> apply;

  Timestamps(ChronoUnit granularity, String name) {
    this.granularity = granularity;
    this.name = name;
    this.apply = new Apply(granularity);
  }

  @Override
  public Integer apply(Long timestampMicros) {
    return apply.apply(timestampMicros);
  }

  @Override
  public Function<Long, Integer> bind(Type type) {
    return apply;
  }

  @Override
  public boolean canTransform(Type type) {
    return type.typeId() == Type.TypeID.TIMESTAMP;
  }

  @Override
  public Type getResultType(Type sourceType) {
    if (granularity == ChronoUnit.DAYS) {
      return Types.DateType.get();
    }
    return Types.IntegerType.get();
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
      return granularity.getDuration().toHours()
          <= otherTransform.granularity.getDuration().toHours();
    }

    return false;
  }

  @Override
  public UnboundPredicate<Integer> project(String fieldName, BoundPredicate<Long> pred) {
    if (pred.term() instanceof BoundTransform) {
      return ProjectionUtil.projectTransformPredicate(apply, fieldName, pred);
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
      return ProjectionUtil.projectTransformPredicate(apply, fieldName, pred);
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
  public String toHumanString(Type alwaysTimestamp, Integer value) {
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
