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

import com.google.common.base.Objects;
import com.netflix.iceberg.expressions.BoundPredicate;
import com.netflix.iceberg.expressions.Expressions;
import com.netflix.iceberg.expressions.UnboundPredicate;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types;
import java.nio.ByteBuffer;

class Identity<T> implements Transform<T, T> {
  @SuppressWarnings("unchecked")
  public static <I> Identity<I> get(Type type) {
    return new Identity<>(type);
  }

  private final Type type;

  private Identity(Type type) {
    this.type = type;
  }

  @Override
  public T apply(T value) {
    return value;
  }

  @Override
  public boolean canTransform(Type type) {
    return type.isPrimitiveType();
  }

  @Override
  public Type getResultType(Type sourceType) {
    return sourceType;
  }

  @Override
  public UnboundPredicate<T> project(String name, BoundPredicate<T> predicate) {
    return projectStrict(name, predicate);
  }

  @Override
  public UnboundPredicate<T> projectStrict(String name, BoundPredicate<T> predicate) {
    if (predicate.literal() != null) {
      return Expressions.predicate(predicate.op(), name, predicate.literal().value());
    } else {
      return Expressions.predicate(predicate.op(), name);
    }
  }

  @Override
  public String toHumanString(T value) {
    if (value == null) {
      return "null";
    }

    switch (type.typeId()) {
      case DATE:
        return TransformUtil.humanDay((Integer) value);
      case TIME:
        return TransformUtil.humanTime((Long) value);
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          return TransformUtil.humanTimestampWithZone((Long) value);
        } else {
          return TransformUtil.humanTimestampWithoutZone((Long) value);
        }
      case FIXED:
      case BINARY:
        if (value instanceof ByteBuffer) {
          return TransformUtil.base64encode(((ByteBuffer) value).duplicate());
        } else if (value instanceof byte[]) {
          return TransformUtil.base64encode(ByteBuffer.wrap((byte[]) value));
        } else {
          throw new UnsupportedOperationException("Unsupported binary type: " + value.getClass());
        }
      default:
        return value.toString();
    }
  }

  @Override
  public String toString() {
    return "identity";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Identity<?> that = (Identity<?>) o;
    return type.equals(that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(type);
  }
}
