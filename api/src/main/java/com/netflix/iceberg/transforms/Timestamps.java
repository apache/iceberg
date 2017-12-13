/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.transforms;

import com.netflix.iceberg.expressions.BoundPredicate;
import com.netflix.iceberg.expressions.UnboundPredicate;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

enum Timestamps implements Transform<Long, Integer> {
  YEAR(ChronoUnit.YEARS, "year"),
  MONTH(ChronoUnit.MONTHS, "month"),
  DAY(ChronoUnit.DAYS, "day"),
  HOUR(ChronoUnit.HOURS, "hour");

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private final ChronoUnit granularity;
  private final String name;

  Timestamps(ChronoUnit granularity, String name) {
    this.granularity = granularity;
    this.name = name;
  }

  @Override
  public Integer apply(Long timestampMicros) {
    // discards fractional seconds, not needed for calculation
    OffsetDateTime timestamp = Instant
        .ofEpochSecond(timestampMicros / 1_000_000)
        .atOffset(ZoneOffset.UTC);
    return (int) granularity.between(EPOCH, timestamp);
  }

  @Override
  public boolean canTransform(Type type) {
    return type.typeId() == Type.TypeID.TIMESTAMP;
  }

  @Override
  public Type getResultType(Type sourceType) {
    return Types.IntegerType.get();
  }

  @Override
  public UnboundPredicate<Integer> project(String name, BoundPredicate<Long> pred) {
    return ProjectionUtil.truncateLong(name, pred, this);
  }

  @Override
  public UnboundPredicate<Integer> projectStrict(String name, BoundPredicate<Long> predicate) {
    return null;
  }

  @Override
  public String toString() {
    return name;
  }
}
