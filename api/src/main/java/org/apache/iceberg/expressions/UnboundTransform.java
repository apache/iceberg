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

import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Types;

public class UnboundTransform<S, T> implements UnboundTerm<T>, Term {
  private final NamedReference<S> ref;
  private final Transform<S, T> transform;

  UnboundTransform(NamedReference<S> ref, Transform<S, T> transform) {
    this.ref = ref;
    this.transform = transform;
  }

  @Override
  public NamedReference<S> ref() {
    return ref;
  }

  public Transform<S, T> transform() {
    return transform;
  }

  @Override
  public BoundTransform<S, T> bind(Types.StructType struct, boolean caseSensitive) {
    BoundReference<S> boundRef = ref.bind(struct, caseSensitive);

    try {
      ValidationException.check(
          transform.canTransform(boundRef.type()),
          "Cannot bind: %s cannot transform %s values from '%s'",
          transform,
          boundRef.type(),
          ref.name());
    } catch (IllegalArgumentException e) {
      throw new ValidationException(
          "Cannot bind: %s cannot transform %s values from '%s'",
          transform, boundRef.type(), ref.name());
    }

    return new BoundTransform<>(boundRef, transform);
  }

  @Override
  public String toString() {
    return transform + "(" + ref + ")";
  }
}
