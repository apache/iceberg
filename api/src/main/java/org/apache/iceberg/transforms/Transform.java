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

import java.io.Serializable;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;

/**
 * A transform function used for partitioning.
 * <p>
 * Implementations of this interface can be used to transform values, check or types, and project
 * {@link BoundPredicate predicates} to predicates on partition values.
 *
 * @param <S> Java class of source values
 * @param <T> Java class of transformed values
 */
public interface Transform<S, T> extends Serializable {
  /**
   * Transforms a value to its corresponding partition value.
   *
   * @param value a source value
   * @return a transformed partition value
   */
  T apply(S value);

  /**
   * Checks whether this function can be applied to the given {@link Type}.
   *
   * @param type a type
   * @return true if this transform can be applied to the type, false otherwise
   */
  boolean canTransform(Type type);

  /**
   * Returns the {@link Type} produced by this transform given a source type.
   *
   * @param sourceType a type
   * @return the result type created by the apply method for the given type
   */
  Type getResultType(Type sourceType);

  /**
   * Transforms a {@link BoundPredicate predicate} to an inclusive predicate on the partition
   * values produced by {@link #apply(Object)}.
   * <p>
   * This inclusive transform guarantees that if pred(v) is true, then projected(apply(v)) is true.
   *
   * @param name the field name for partition values
   * @param predicate a predicate for source values
   * @return an inclusive predicate on partition values
   */
  UnboundPredicate<T> project(String name, BoundPredicate<S> predicate);

  /**
   * Transforms a {@link BoundPredicate predicate} to a strict predicate on the partition values
   * produced by {@link #apply(Object)}.
   * <p>
   * This strict transform guarantees that if strict(apply(v)) is true, then pred(v) is also true.
   *
   * @param name the field name for partition values
   * @param predicate a predicate for source values
   * @return an inclusive predicate on partition values
   */
  UnboundPredicate<T> projectStrict(String name, BoundPredicate<S> predicate);

  /**
   * Return whether this transform is the identity transform.
   *
   * @return true if this is an identity transform, false otherwise
   */
  default boolean isIdentity() {
    return false;
  }

  /**
   * Returns a human-readable String representation of a transformed value.
   * <p>
   * null values will return "null"
   *
   * @param value a transformed value
   * @return a human-readable String representation of the value
   */
  default String toHumanString(T value) {
    return String.valueOf(value);
  }
}
