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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.StructProjection;

public class UnboundTransform<S, T> implements UnboundTerm<T>, Term {
  private final NamedReference<?>[] refs;
  private final Transform<?, T> transform;

  UnboundTransform(NamedReference<?> ref, Transform<?, T> transform) {
    this.refs = Collections.singletonList(ref).toArray(new NamedReference[0]);
    this.transform = transform;
  }

  UnboundTransform(List<NamedReference<?>> refs, Transform<?, T> transform) {
    Preconditions.checkArgument(
        refs != null && !refs.isEmpty(), "At least one reference should be provided");
    this.refs = refs.toArray(new NamedReference[0]);
    this.transform = transform;
  }

  @SuppressWarnings("unchecked")
  @Override
  public NamedReference<S> ref() {
    return (NamedReference<S>) refs[0];
  }

  @Override
  public List<NamedReference<?>> refs() {
    return Arrays.asList(refs);
  }

  public Transform<S, T> transform() {
    return (Transform<S, T>) transform;
  }

  @Override
  public BoundTransform<S, T> bind(Types.StructType struct, boolean caseSensitive) {
    if (refs.length == 1) {
      BoundReference<?> boundRef = ref().bind(struct, caseSensitive);
      return bindRef(boundRef);
    } else {
      List<BoundReference<?>> boundRefs =
          Arrays.stream(refs).map(x -> x.bind(struct, caseSensitive)).collect(Collectors.toList());
      return bindRefs(struct, boundRefs);
    }
  }

  private BoundTransform<S, T> bindRef(BoundReference<?> boundRef) {
    try {
      ValidationException.check(
          transform.canTransform(boundRef.type()),
          "Cannot bind: %s cannot transform %s values from '%s'",
          transform,
          boundRef.type(),
          ref().name());
    } catch (IllegalArgumentException e) {
      throw new ValidationException(
          "Cannot bind: %s cannot transform %s values from '%s'",
          transform, boundRef.type(), ref().name());
    }

    return new BoundTransform<>(boundRef, (Transform<S, T>) transform);
  }

  private BoundTransform<S, T> bindRefs(
      Types.StructType structType, List<BoundReference<?>> boundedRefs) {
    StructType projectedType =
        StructType.of(boundedRefs.stream().map(BoundReference::field).collect(Collectors.toList()));
    StructProjection projection = StructProjection.create(structType, projectedType);
    String refNames =
        boundedRefs.stream().map(BoundReference::name).collect(Collectors.joining("(", ",", ")"));
    try {
      ValidationException.check(
          transform.canTransform(projectedType),
          "Cannot bind: %s cannot transform %s values from '%s'",
          transform,
          projectedType,
          refNames);
    } catch (IllegalArgumentException e) {
      throw new ValidationException(
          "Cannot bind: %s cannot transform %s values from '%s'",
          transform, projectedType, refNames);
    }
    return new BoundTransform<>(boundedRefs, projection, (Transform<S, T>) transform);
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
