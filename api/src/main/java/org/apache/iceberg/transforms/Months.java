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

public class Months<T> extends TimeTransform<T> {
  private static final Months<?> INSTANCE = new Months<>();

  @SuppressWarnings("unchecked")
  static <T> Months<T> get() {
    return (Months<T>) INSTANCE;
  }

  @Override
  protected ChronoUnit granularity() {
    return ChronoUnit.MONTHS;
  }

  @Override
  protected Transform<T, Integer> toEnum(Type type) {
    return (Transform<T, Integer>)
        fromSourceType(type, Dates.MONTH, Timestamps.MICROS_TO_MONTH, Timestamps.NANOS_TO_MONTH);
  }

  @Override
  public Type getResultType(Type sourceType) {
    return Types.IntegerType.get();
  }

  @Override
  public String toHumanString(Type alwaysInt, Integer value) {
    return value != null ? TransformUtil.humanMonth(value) : "null";
  }

  @Override
  public String toString() {
    return "month";
  }

  Object writeReplace() throws ObjectStreamException {
    return SerializationProxies.MonthsTransformProxy.get();
  }
}
