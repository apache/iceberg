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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;

/**
 * A transform function used for partitioning.
 *
 * <p>Implementations of this interface can be used to transform values, check or types, and project
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
   * @deprecated use {@link #bind(Type)} instead; will be removed in 2.0.0
   */
  @Deprecated
  default T apply(S value) {
    throw new UnsupportedOperationException(
        "apply(value) is deprecated, use bind(Type).apply(value)");
  }

  /**
   * Returns a function that applies this transform to values of the given {@link Type type}.
   *
   * @param type an Iceberg {@link Type}
   * @return a {@link Function} that applies this transform to values of the given type.
   */
  default SerializableFunction<S, T> bind(Type type) {
    throw new UnsupportedOperationException("bind is not implemented");
  }

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
   * Whether the transform preserves the order of values (is monotonic).
   *
   * <p>A transform preserves order for values when for any given a and b, if a &lt; b then apply(a)
   * &lt;= apply(b).
   *
   * @return true if the transform preserves the order of values
   */
  default boolean preservesOrder() {
    return false;
  }

  /**
   * Whether ordering by this transform's result satisfies the ordering of another transform's
   * result.
   *
   * <p>For example, sorting by day(ts) will produce an ordering that is also by month(ts) or
   * year(ts). However, sorting by day(ts) will not satisfy the order of hour(ts) or identity(ts).
   *
   * @return true if ordering by this transform is equivalent to ordering by the other transform
   */
  default boolean satisfiesOrderOf(Transform<?, ?> other) {
    return equals(other);
  }

  /**
   * Transforms a {@link BoundPredicate predicate} to an inclusive predicate on the partition values
   * produced by the transform.
   *
   * <p>This inclusive transform guarantees that if pred(v) is true, then projected(apply(v)) is
   * true.
   *
   * @param name the field name for partition values
   * @param predicate a predicate for source values
   * @return an inclusive predicate on partition values
   */
  UnboundPredicate<T> project(String name, BoundPredicate<S> predicate);

  /**
   * Transforms a {@link BoundPredicate predicate} to a strict predicate on the partition values
   * produced by the transform.
   *
   * <p>This strict transform guarantees that if strict(apply(v)) is true, then pred(v) is also
   * true.
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
   * Return whether this transform is the void transform.
   *
   * @return true if this is a void transform, false otherwise
   */
  default boolean isVoid() {
    return false;
  }

  /**
   * Returns a human-readable String representation of a transformed value.
   *
   * <p>null values will return "null"
   *
   * @param value a transformed value
   * @return a human-readable String representation of the value
   * @deprecated use {@link #toHumanString(Type, Object)} instead; will be removed in 2.0.0
   */
  @Deprecated
  default String toHumanString(T value) {
    if (value instanceof ByteBuffer) {
      return TransformUtil.base64encode(((ByteBuffer) value).duplicate());
    } else if (value instanceof byte[]) {
      return TransformUtil.base64encode(ByteBuffer.wrap((byte[]) value));
    } else {
      return String.valueOf(value);
    }
  }

  default String toHumanString(Type type, T value) {
    if (value == null) {
      return "null";
    }

    switch (type.typeId()) {
      case DATE:
        return TransformUtil.humanDay((Integer) value);
      case TIME:
        return TransformUtil.humanTime((Long) value);
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          return TransformUtil.humanTimestampWithZone((Long) value);
        } else {
          return TransformUtil.humanTimestampWithoutZone((Long) value);
        }
      case FIXED:
      case BINARY:
        if (value instanceof ByteBuffer) {
          return TransformUtil.base64encode(((ByteBuffer) value).duplicate());
        } else if (value instanceof byte[]) {
          return TransformUtil.base64encode(ByteBuffer.wrap((byte[]) value));
        } else {
          throw new UnsupportedOperationException("Unsupported binary type: " + value.getClass());
        }
      default:
        return value.toString();
    }
  }

  /**
   * Return the unique transform name to check if similar transforms for the same source field are
   * added multiple times in partition spec builder.
   *
   * @return a name used for dedup
   */
  default String dedupName() {
    return toString();
  }

  Function<List<Object>, Collection<Object>> dateFixer =
      input -> {
        boolean hasNegativeValue = false;
        Set<Object> fixedSet = Sets.newHashSet();
        for (Object val : input) {
          if (val != null) {
            Integer value = (Integer) val;
            fixedSet.add(value);
            if (value < 0) {
              hasNegativeValue = true;
              fixedSet.add(value + 1);
            }
          }
        }
        if (hasNegativeValue) {
          return fixedSet;
        } else {
          return input;
        }
      };
}
