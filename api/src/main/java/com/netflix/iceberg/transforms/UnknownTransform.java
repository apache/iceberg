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

class UnknownTransform<S, T> implements Transform<S, T> {

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
    // Assuming the transform function can be applied for this type
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
}
