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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;
import org.apache.iceberg.util.StructProjection;

/**
 * A transform expression.
 *
 * @param <S> the Java type of values transformed by this function.
 * @param <T> the Java type of values returned by the function.
 */
public class BoundTransform<S, T> implements BoundTerm<T> {
  private final BoundReference<?>[] refs;
  private final StructProjection projection;
  private Type inputType;
  private final Transform<?, T> transform;
  private final SerializableFunction<S, T> func;

  BoundTransform(BoundReference<?> ref, Transform<S, T> transform) {
    this.refs = Collections.singletonList(ref).toArray(new BoundReference<?>[0]);
    this.transform = transform;
    this.projection = null;
    this.func = transform.bind(ref.type());
  }

  BoundTransform(
      List<BoundReference<?>> refs, StructProjection projection, Transform<S, T> transform) {
    Preconditions.checkArgument(
        refs != null && !refs.isEmpty(), "At least one reference should be provided");
    if (refs.size() == 1) {
      Preconditions.checkArgument(
          projection == null, "For singe arg transform, projection should be null");
    }
    this.refs = refs.toArray(new BoundReference<?>[0]);
    this.transform = transform;
    this.projection = projection;
    this.func = transform.bind(lazyInputType());
  }

  private Type lazyInputType() {
    if (inputType == null) {
      if (refs.length == 1) {
        this.inputType = refs[0].type();
      } else {
        List<Types.NestedField> fields =
            Arrays.stream(refs).map(BoundReference::field).collect(Collectors.toList());
        this.inputType = Types.StructType.of(fields);
      }
    }
    return inputType;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T eval(StructLike struct) {
    if (projection == null) {
      return func.apply(ref().eval(struct));
    } else {
      return func.apply((S) projection.wrap(struct));
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public BoundReference<S> ref() {
    return (BoundReference<S>) refs[0];
  }

  @Override
  public List<BoundReference<?>> refs() {
    return Arrays.asList(this.refs);
  }

  public Transform<S, T> transform() {
    return (Transform<S, T>) transform;
  }

  @Override
  public Type type() {
    return transform.getResultType(lazyInputType());
  }

  private boolean areRefsEquivalent(List<BoundReference<?>> left, List<BoundReference<?>> right) {
    if (left.size() != right.size()) {
      return false;
    }
    Iterator<BoundReference<?>> leftIter = left.iterator();
    Iterator<BoundReference<?>> rightIter = right.iterator();
    while (leftIter.hasNext() && rightIter.hasNext()) {
      if (!leftIter.next().isEquivalentTo(rightIter.next())) {
        return false;
      }
    }
    return !leftIter.hasNext() && !rightIter.hasNext();
  }

  @Override
  public boolean isEquivalentTo(BoundTerm<?> other) {
    if (other instanceof BoundTransform) {
      BoundTransform<?, ?> bound = (BoundTransform<?, ?>) other;
      return areRefsEquivalent(this.refs(), other.refs()) && transform.equals(bound.transform());
    } else if (transform.isIdentity() && other instanceof BoundReference) {
      return refs.length == 1 && ref().isEquivalentTo(other);
    }

    return false;
  }

  @Override
  public String toString() {
    if (refs.length == 1) {
      return transform + "(" + ref() + ")";
    } else {
      return transform + "(" + Arrays.toString(refs) + ")";
    }
  }
}
