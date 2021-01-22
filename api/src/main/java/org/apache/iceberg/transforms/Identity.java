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

import java.nio.ByteBuffer;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

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
  public boolean canTransform(Type maybePrimitive) {
    return maybePrimitive.isPrimitiveType();
  }

  @Override
  public Type getResultType(Type sourceType) {
    return sourceType;
  }

  @Override
  public boolean preservesOrder() {
    return true;
  }

  @Override
  public boolean satisfiesOrderOf(Transform<?, ?> other) {
    // ordering by value is the same as long as the other preserves order
    return other.preservesOrder();
  }

  @Override
  public UnboundPredicate<T> project(String name, BoundPredicate<T> predicate) {
    return projectStrict(name, predicate);
  }

  @Override
  public UnboundPredicate<T> projectStrict(String name, BoundPredicate<T> predicate) {
    if (predicate.isUnaryPredicate()) {
      return Expressions.predicate(predicate.op(), name);
    } else if (predicate.isLiteralPredicate()) {
      return Expressions.predicate(predicate.op(), name, predicate.asLiteralPredicate().literal().value());
    } else if (predicate.isSetPredicate()) {
      return Expressions.predicate(predicate.op(), name, predicate.asSetPredicate().literalSet());
    }
    return null;
  }

  @Override
  public boolean isIdentity() {
    return true;
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
    } else if (!(o instanceof Identity)) {
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
