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

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

enum Dates implements Transform<Integer, Integer> {
  YEAR(ChronoUnit.YEARS, "year"),
  MONTH(ChronoUnit.MONTHS, "month"),
  DAY(ChronoUnit.DAYS, "day");

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private final ChronoUnit granularity;
  private final String name;

  Dates(ChronoUnit granularity, String name) {
    this.granularity = granularity;
    this.name = name;
  }

  @Override
  public Integer apply(Integer days) {
    if (days == null) {
      return null;
    }

    if (granularity == ChronoUnit.DAYS) {
      return days;
    }

    return (int) granularity.between(EPOCH, EPOCH.plusDays(days));
  }

  @Override
  public boolean canTransform(Type type) {
    return type.typeId() == Type.TypeID.DATE;
  }

  @Override
  public Type getResultType(Type sourceType) {
    if (granularity == ChronoUnit.DAYS) {
      return Types.DateType.get();
    }
    return Types.IntegerType.get();
  }

  @Override
  public UnboundPredicate<Integer> project(String fieldName, BoundPredicate<Integer> pred) {
    if (pred.isUnaryPredicate()) {
      return Expressions.predicate(pred.op(), fieldName);
    } else if (pred.isLiteralPredicate()) {
      return ProjectionUtil.truncateInteger(fieldName, pred.asLiteralPredicate(), this);
    }
    return null;
  }

  @Override
  public UnboundPredicate<Integer> projectStrict(String fieldName, BoundPredicate<Integer> pred) {
    if (pred.isUnaryPredicate()) {
      return Expressions.predicate(pred.op(), fieldName);
    } else if (pred.isLiteralPredicate()) {
      return ProjectionUtil.truncateIntegerStrict(fieldName, pred.asLiteralPredicate(), this);
    }
    return null;
  }

  @Override
  public String toHumanString(Integer value) {
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
      default:
        throw new UnsupportedOperationException("Unsupported time unit: " + granularity);
    }
  }

  @Override
  public String toString() {
    return name;
  }
}
