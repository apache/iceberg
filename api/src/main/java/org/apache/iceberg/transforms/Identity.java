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

import com.google.common.base.Objects;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

abstract class Identity<T> implements Transform<T, T> {
  private final Type type;

  private Identity(Type type) {
    this.type = type;
  }

  @SuppressWarnings("unchecked")
  public static <T> Identity<T> get(Type type) {
    switch (type.typeId()) {
      case DATE:
      case INTEGER:
        return (Identity<T>) new IdentityInteger(type);
      case TIME:
      case TIMESTAMP:
      case LONG:
        return (Identity<T>) new IdentityLong(type);
      case DECIMAL:
        return (Identity<T>) new IdentityDecimal(type);
      case STRING:
        return (Identity<T>) new IdentityString(type);
      case FIXED:
      case BINARY:
        return (Identity<T>) new IdentityByteBuffer(type);
      case UUID:
        return (Identity<T>) new IdentityUUID(type);
      case BOOLEAN:
        return (Identity<T>) new IdentityBoolean(type);
      case FLOAT:
        return (Identity<T>) new IdentityFloat(type);
      case DOUBLE:
        return (Identity<T>) new IdentityDouble(type);
      default:
        throw new IllegalArgumentException("Cannot use identity with type: " + type);
    }
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

  private static class IdentityUUID extends Identity<UUID> {

    private IdentityUUID(Type type) {
      super(type);
    }

    @Override
    public UUID fromHumanString(String value) {
      return UUID.fromString(value);
    }
  }

  private static class IdentityBoolean extends Identity<Boolean> {

    private IdentityBoolean(Type type) {
      super(type);
    }

    @Override
    public Boolean fromHumanString(String value) {
      return Boolean.valueOf(value);
    }
  }

  private static class IdentityFloat extends Identity<Float> {

    private IdentityFloat(Type type) {
      super(type);
    }

    @Override
    public Float fromHumanString(String value) {
      return Float.valueOf(value);
    }
  }

  private static class IdentityDouble extends Identity<Double> {

    private IdentityDouble(Type type) {
      super(type);
    }

    @Override
    public Double fromHumanString(String value) {
      return Double.valueOf(value);
    }
  }

  private static class IdentityByteBuffer extends Identity<ByteBuffer> {

    private IdentityByteBuffer(Type type) {
      super(type);
    }

    @Override
    public ByteBuffer fromHumanString(String value) {
      return ByteBuffer.wrap(
          TransformUtil.base64decode(value.getBytes(StandardCharsets.ISO_8859_1)));
    }
  }

  private static class IdentityString extends Identity<String> {

    private IdentityString(Type type) {
      super(type);
    }

    @Override
    public String fromHumanString(String value) {
      return value;
    }
  }

  private static class IdentityDecimal extends Identity<BigDecimal> {

    private IdentityDecimal(Type type) {
      super(type);
    }

    @Override
    public BigDecimal fromHumanString(String value) {
      return new BigDecimal(value);
    }
  }

  private static class IdentityInteger extends Identity<Integer> {

    private IdentityInteger(Type type) {
      super(type);
    }

    @Override
    public Integer fromHumanString(String value) {
      return Integer.valueOf(value);
    }
  }

  private static class IdentityLong extends Identity<Long> {

    private IdentityLong(Type type) {
      super(type);
    }

    @Override
    public Long fromHumanString(String value) {
      return Long.valueOf(value);
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
