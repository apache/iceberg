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

import java.time.temporal.ChronoUnit;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundTransform;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.SerializableFunction;

abstract class TimeTransform<S> implements Transform<S, Integer> {
  protected static <R> R fromSourceType(Type type, R dateResult, R microsResult, R nanosResult) {
    switch (type.typeId()) {
      case DATE:
        if (dateResult != null) {
          return dateResult;
        }
        break;
      case TIMESTAMP:
        return microsResult;
      case TIMESTAMP_NANO:
        return nanosResult;
    }

    throw new IllegalArgumentException("Unsupported type: " + type);
  }

  protected abstract ChronoUnit granularity();

  protected abstract Transform<S, Integer> toEnum(Type type);

  @Override
  public SerializableFunction<S, Integer> bind(Type type) {
    return toEnum(type).bind(type);
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
      return TransformUtil.satisfiesOrderOf(granularity(), ((Dates) other).granularity());
    } else if (other instanceof Timestamps) {
      return TransformUtil.satisfiesOrderOf(granularity(), ((Timestamps) other).granularity());
    } else if (other instanceof TimeTransform) {
      return TransformUtil.satisfiesOrderOf(
          granularity(), ((TimeTransform<?>) other).granularity());
    }

    return false;
  }

  @Override
  public boolean canTransform(Type type) {
    return type.typeId() == Type.TypeID.DATE
        || type.typeId() == Type.TypeID.TIMESTAMP
        || type.typeId() == Type.TypeID.TIMESTAMP_NANO;
  }

  @Override
  public UnboundPredicate<Integer> project(String name, BoundPredicate<S> predicate) {
    if (predicate.term() instanceof BoundTransform) {
      return ProjectionUtil.projectTransformPredicate(this, name, predicate);
    }

    return toEnum(predicate.term().type()).project(name, predicate);
  }

  @Override
  public UnboundPredicate<Integer> projectStrict(String name, BoundPredicate<S> predicate) {
    if (predicate.term() instanceof BoundTransform) {
      return ProjectionUtil.projectTransformPredicate(this, name, predicate);
    }

    return toEnum(predicate.term().type()).projectStrict(name, predicate);
  }

  @Override
  public String dedupName() {
    return "time";
  }
}
