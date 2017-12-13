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

import com.netflix.iceberg.expressions.BoundPredicate;
import com.netflix.iceberg.expressions.Expressions;
import com.netflix.iceberg.expressions.UnboundPredicate;
import com.netflix.iceberg.types.Type;
import java.io.ObjectStreamException;

class Identity<T> implements Transform<T, T> {
  private static final Identity INSTANCE = new Identity();

  @SuppressWarnings("unchecked")
  public static <I> Identity<I> get() {
    return (Identity<I>) INSTANCE;
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
  public String toString() {
    return "identity";
  }

  Object writeReplace() throws ObjectStreamException {
    return SerializationProxies.IdentityProxy.get();
  }
}
