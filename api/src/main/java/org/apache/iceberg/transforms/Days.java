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

public class Days<T> extends TimeTransform<T> {
  private static final Days<?> INSTANCE = new Days<>();

  @SuppressWarnings("unchecked")
  static <T> Days<T> get() {
    return (Days<T>) INSTANCE;
  }

  @Override
  protected ChronoUnit granularity() {
    return ChronoUnit.DAYS;
  }

  @Override
  protected Transform<T, Integer> toEnum(Type type) {
    return (Transform<T, Integer>)
        fromSourceType(type, Dates.DAY, Timestamps.MICROS_TO_DAY, Timestamps.NANOS_TO_DAY);
  }

  @Override
  public Type getResultType(Type sourceType) {
    return Types.DateType.get();
  }

  @Override
  public String toHumanString(Type alwaysDate, Integer value) {
    return value != null ? TransformUtil.humanDay(value) : "null";
  }

  @Override
  public String toString() {
    return "day";
  }

  @Override
  @SuppressWarnings("unchecked")
  public T toSourceTypeValue(Type sourceType, Integer days) {
    if (days == null) {
      return null;
    }
    // For DATE type, return the days value as-is
    if (sourceType.typeId() == Type.TypeID.DATE) {
      return (T) days;
    }
    // Convert days since epoch to micros/nanos since epoch (start of the day)
    long micros = days * 24L * 3600L * 1_000_000L;
    if (sourceType.typeId() == Type.TypeID.TIMESTAMP_NANO) {
      return (T) (Long) (micros * 1000L);
    }
    return (T) (Long) micros;
  }

  Object writeReplace() throws ObjectStreamException {
    return SerializationProxies.DaysTransformProxy.get();
  }
}
