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
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class Hours<T> extends TimeTransform<T> {
  private static final Hours<?> INSTANCE = new Hours<>();

  @SuppressWarnings("unchecked")
  static <T> Hours<T> get() {
    return (Hours<T>) INSTANCE;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Transform<T, Integer> toEnum(Type type) {
    switch (type.typeId()) {
      case TIMESTAMP:
        return (Transform<T, Integer>) Timestamps.HOUR_FROM_MICROS;
      case TIMESTAMP_NANO:
        return (Transform<T, Integer>) Timestamps.HOUR_FROM_NANOS;
      default:
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }

  @Override
  public boolean canTransform(Type type) {
    return type.typeId() == Type.TypeID.TIMESTAMP || type.typeId() == Type.TypeID.TIMESTAMP_NANO;
  }

  @Override
  public Type getResultType(Type sourceType) {
    return Types.IntegerType.get();
  }

  @Override
  public boolean satisfiesOrderOf(Transform<?, ?> other) {
    if (this == other) {
      return true;
    }

    if (other instanceof Timestamps) {
      return other == Timestamps.HOUR_FROM_MICROS || other == Timestamps.HOUR_FROM_NANOS;
    } else if (other instanceof Hours
        || other instanceof Days
        || other instanceof Months
        || other instanceof Years) {
      return true;
    }

    return false;
  }

  @Override
  public String toHumanString(Type alwaysInt, Integer value) {
    return value != null ? TransformUtil.humanHour(value) : "null";
  }

  @Override
  public String toString() {
    return "hour";
  }

  Object writeReplace() throws ObjectStreamException {
    return SerializationProxies.HoursTransformProxy.get();
  }
}
