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
package org.apache.iceberg.expressions;

import org.apache.iceberg.StructLike;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Type;

/**
 * A transform expression.
 *
 * @param <S> the Java type of values transformed by this function.
 * @param <T> the Java type of values returned by the function.
 */
public class BoundTransform<S, T> implements BoundTerm<T> {
  private final BoundReference<S> ref;
  private final Transform<S, T> transform;

  BoundTransform(BoundReference<S> ref, Transform<S, T> transform) {
    this.ref = ref;
    this.transform = transform;
  }

  @Override
  public T eval(StructLike struct) {
    return transform.apply(ref.eval(struct));
  }

  @Override
  public BoundReference<S> ref() {
    return ref;
  }

  public Transform<S, T> transform() {
    return transform;
  }

  @Override
  public Type type() {
    return transform.getResultType(ref.type());
  }

  @Override
  public boolean isEquivalentTo(BoundTerm<?> other) {
    if (other instanceof BoundTransform) {
      BoundTransform<?, ?> bound = (BoundTransform<?, ?>) other;
      return ref.isEquivalentTo(bound.ref()) && transform.equals(bound.transform());
    } else if (transform.isIdentity() && other instanceof BoundReference) {
      return ref.isEquivalentTo(other);
    }

    return false;
  }

  @Override
  public String toString() {
    return transform + "(" + ref + ")";
  }
}
