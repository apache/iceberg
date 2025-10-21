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

import java.io.ObjectStreamException;
import java.time.temporal.ChronoUnit;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

class Years<T> extends TimeTransform<T> {
  private static final Years<?> INSTANCE = new Years<>();

  @SuppressWarnings("unchecked")
  static <T> Years<T> get() {
    return (Years<T>) INSTANCE;
  }

  @Override
  protected ChronoUnit granularity() {
    return ChronoUnit.YEARS;
  }

  @Override
  protected Transform<T, Integer> toEnum(Type type) {
    return (Transform<T, Integer>)
        fromSourceType(type, Dates.YEAR, Timestamps.MICROS_TO_YEAR, Timestamps.NANOS_TO_YEAR);
  }

  @Override
  public Type getResultType(Type sourceType) {
    return Types.IntegerType.get();
  }

  @Override
  public String toHumanString(Type alwaysInt, Integer value) {
    return value != null ? TransformUtil.humanYear(value) : "null";
  }

  @Override
  public String toString() {
    return "year";
  }

  @Override
  @SuppressWarnings("unchecked")
  public T toSourceTypeValue(Type sourceType, Integer years) {
    if (years == null) {
      return null;
    }
    // For DATE type, convert to days
    if (sourceType.typeId() == Type.TypeID.DATE) {
      return (T)
          (Integer)
              (int)
                  ChronoUnit.DAYS.between(
                      org.apache.iceberg.util.DateTimeUtil.EPOCH_DAY,
                      org.apache.iceberg.util.DateTimeUtil.EPOCH_DAY.plusYears(years));
    }
    // Convert years since epoch to micros/nanos since epoch (start of the year)
    long micros =
        ChronoUnit.MICROS.between(
            org.apache.iceberg.util.DateTimeUtil.EPOCH,
            org.apache.iceberg.util.DateTimeUtil.EPOCH.plusYears(years));
    if (sourceType.typeId() == Type.TypeID.TIMESTAMP_NANO) {
      return (T) (Long) (micros * 1000L);
    }
    return (T) (Long) micros;
  }

  Object writeReplace() throws ObjectStreamException {
    return SerializationProxies.YearsTransformProxy.get();
  }
}
