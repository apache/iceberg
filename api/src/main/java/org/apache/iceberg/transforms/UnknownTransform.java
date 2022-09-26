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

import java.util.Objects;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;

public class UnknownTransform<S, T> implements Transform<S, T> {

  private final String transform;

  UnknownTransform(String transform) {
    this.transform = transform;
  }

  @Override
  public T apply(S value) {
    throw new UnsupportedOperationException(
        String.format("Cannot apply unsupported transform: %s", transform));
  }

  @Override
  public SerializableFunction<S, T> bind(Type type) {
    throw new UnsupportedOperationException(
        String.format("Cannot bind unsupported transform: %s", transform));
  }

  @Override
  public boolean canTransform(Type type) {
    // assume the transform function can be applied for any type
    return true;
  }

  @Override
  public Type getResultType(Type type) {
    // the actual result type is not known
    return Types.StringType.get();
  }

  @Override
  public UnboundPredicate<T> project(String name, BoundPredicate<S> predicate) {
    return null;
  }

  @Override
  public UnboundPredicate<T> projectStrict(String name, BoundPredicate<S> predicate) {
    return null;
  }

  @Override
  public String toString() {
    return transform;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof UnknownTransform)) {
      return false;
    }

    UnknownTransform<?, ?> that = (UnknownTransform<?, ?>) other;
    return transform.equals(that.transform);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(transform);
  }
}
