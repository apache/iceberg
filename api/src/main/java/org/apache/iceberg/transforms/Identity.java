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
import java.util.function.Function;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;

class Identity<T> implements Transform<T, T> {
  private static final Identity<?> INSTANCE = new Identity<>();

  @SuppressWarnings("unchecked")
  public static <I> Identity<I> get() {
    return (Identity<I>) INSTANCE;
  }

  private Identity() {
  }

  @Override
  public T apply(T value) {
    return value;
  }

  @Override
  public Function<T, T> bind(Type type) {
    return Function.identity();
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
  public String toString() {
    return "identity";
  }

  Object writeReplace() throws ObjectStreamException {
    return SerializationProxies.IdentityTransformProxy.get();
  }
}
