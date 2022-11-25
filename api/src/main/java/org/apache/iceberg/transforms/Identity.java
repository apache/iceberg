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
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.SerializableFunction;

class Identity<T> implements Transform<T, T> {
  private static final Identity<?> INSTANCE = new Identity<>();

  private final Type type;

  /**
   * Instantiates a new Identity Transform
   *
   * @deprecated use {@link #get()} instead; will be removed in 2.0.0
   */
  @Deprecated
  public static <I> Identity<I> get(Type type) {
    return new Identity<>(type);
  }

  @SuppressWarnings("unchecked")
  public static <I> Identity<I> get() {
    return (Identity<I>) INSTANCE;
  }

  private static class Apply<T> implements SerializableFunction<T, T> {
    private static final Apply<?> APPLY_INSTANCE = new Apply<>();

    @SuppressWarnings("unchecked")
    private static <T> Apply<T> get() {
      return (Apply<T>) APPLY_INSTANCE;
    }

    @Override
    public T apply(T t) {
      return t;
    }
  }

  private Identity() {
    this(null);
  }

  private Identity(Type type) {
    this.type = type;
  }

  @Override
  public T apply(T value) {
    return value;
  }

  @Override
  @SuppressWarnings("checkstyle:HiddenField")
  public SerializableFunction<T, T> bind(Type type) {
    Preconditions.checkArgument(canTransform(type), "Cannot bind to unsupported type: %s", type);
    return Apply.get();
  }

  @Override
  public boolean canTransform(Type maybePrimitive) {
    return maybePrimitive.isPrimitiveType();
  }

  /**
   * Returns a human-readable String representation of a transformed value.
   *
   * <p>null values will return "null"
   *
   * @param value a transformed value
   * @return a human-readable String representation of the value
   * @deprecated use {@link #toHumanString(Type, Object)} instead; will be removed in 2.0.0
   */
  @Deprecated
  @Override
  public String toHumanString(T value) {
    if (this.type != null) {
      return toHumanString(this.type, value);
    }
    return Transform.super.toHumanString(value);
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
      return Expressions.predicate(
          predicate.op(), name, predicate.asLiteralPredicate().literal().value());
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
  public boolean equals(Object o) {
    // Can be removed with 2.0.0 deprecation of get(Type)
    if (this == o) {
      return true;
    } else if (o instanceof Identity) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    // Can be removed with 2.0.0 deprecation of get(Type)
    return 0;
  }

  @Override
  public String toString() {
    return "identity";
  }

  Object writeReplace() throws ObjectStreamException {
    return SerializationProxies.IdentityTransformProxy.get();
  }
}
