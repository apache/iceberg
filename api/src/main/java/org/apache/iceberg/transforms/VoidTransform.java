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
import java.io.Serializable;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.SerializableFunction;

class VoidTransform<S> implements Transform<S, Void> {
  private static final VoidTransform<Object> INSTANCE = new VoidTransform<>();

  @SuppressWarnings("unchecked")
  static <T> VoidTransform<T> get() {
    return (VoidTransform<T>) INSTANCE;
  }

  private static class Apply<S> implements SerializableFunction<S, Void>, Serializable {
    private static final Apply<?> APPLY_INSTANCE = new Apply<>();

    @SuppressWarnings("unchecked")
    private static <S> Apply<S> get() {
      return (Apply<S>) APPLY_INSTANCE;
    }

    @Override
    public Void apply(S t) {
      return null;
    }
  }

  private VoidTransform() {}

  @Override
  public Void apply(Object value) {
    return null;
  }

  @Override
  public SerializableFunction<S, Void> bind(Type type) {
    return Apply.get();
  }

  @Override
  public boolean canTransform(Type type) {
    return true;
  }

  @Override
  public Type getResultType(Type sourceType) {
    return sourceType;
  }

  @Override
  public UnboundPredicate<Void> projectStrict(String name, BoundPredicate<S> predicate) {
    return null;
  }

  @Override
  public UnboundPredicate<Void> project(String name, BoundPredicate<S> predicate) {
    return null;
  }

  @Override
  public boolean isVoid() {
    return true;
  }

  @Override
  public String toHumanString(Void value) {
    return "null";
  }

  @Override
  public String toString() {
    return "void";
  }

  Object writeReplace() throws ObjectStreamException {
    return SerializationProxies.VoidTransformProxy.get();
  }
}
