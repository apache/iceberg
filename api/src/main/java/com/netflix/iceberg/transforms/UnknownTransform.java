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

package com.netflix.iceberg.transforms;

import com.netflix.iceberg.expressions.BoundPredicate;
import com.netflix.iceberg.expressions.Expressions;
import com.netflix.iceberg.expressions.UnboundPredicate;
import com.netflix.iceberg.types.Type;

import java.util.Objects;

public class UnknownTransform<S, T> implements Transform<S, T> {

  private final Type type;
  private final String transform;

  public UnknownTransform(Type type, String transform) {
    this.type = type;
    this.transform = transform;
  }

  @Override
  public T apply(S value) {
    throw new IllegalArgumentException(
        "Unknown transform: " + transform + " for type: " + type.typeId().name());
  }

  @Override
  public boolean canTransform(Type type) {
    // Assuming the transform function can be applied for this type because unknown transform is only used when parsing
    // a transform in an existing table (a previous Iceberg version has already validated this)
    return true;
  }

  @Override
  public Type getResultType(Type sourceType) {
    return type;
  }

  @Override
  public UnboundPredicate<T> project(String name, BoundPredicate<S> predicate) {
    // Assuming all partition values may contain matching data
    return Expressions.alwaysTrueUnboundPredicate();
  }

  @Override
  public UnboundPredicate<T> projectStrict(String name, BoundPredicate<S> predicate) {
    // Can not provide guarantees of matching a predicate
    return null;
  }


  @Override
  public String toString() {
    return String.format("unknown:%s:%s", type.toString(), transform);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UnknownTransform<?, ?> that = (UnknownTransform<?, ?>) o;
    return type.equals(that.type) &&
            transform.equals(that.transform);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, transform);
  }
}
