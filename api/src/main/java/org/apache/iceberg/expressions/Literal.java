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

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.UUID;
import org.apache.iceberg.types.Type;
import org.locationtech.jts.geom.Geometry;

/**
 * Represents a literal fixed value in an expression predicate
 *
 * @param <T> The Java type of the value wrapped by a {@link Literal}
 */
public interface Literal<T> extends Serializable {
  static Literal<Boolean> of(boolean value) {
    return new Literals.BooleanLiteral(value);
  }

  static Literal<Integer> of(int value) {
    return new Literals.IntegerLiteral(value);
  }

  static Literal<Long> of(long value) {
    return new Literals.LongLiteral(value);
  }

  static Literal<Float> of(float value) {
    return new Literals.FloatLiteral(value);
  }

  static Literal<Double> of(double value) {
    return new Literals.DoubleLiteral(value);
  }

  static Literal<CharSequence> of(CharSequence value) {
    return new Literals.StringLiteral(value);
  }

  static Literal<UUID> of(UUID value) {
    return new Literals.UUIDLiteral(value);
  }

  static Literal<ByteBuffer> of(byte[] value) {
    return new Literals.FixedLiteral(ByteBuffer.wrap(value));
  }

  static Literal<ByteBuffer> of(ByteBuffer value) {
    return new Literals.BinaryLiteral(value);
  }

  static Literal<BigDecimal> of(BigDecimal value) {
    return new Literals.DecimalLiteral(value);
  }

  static Literal<Geometry> of(Geometry value) {
    return new Literals.GeometryLiteral(value);
  }

  /** Returns the value wrapped by this literal. */
  T value();

  /**
   * Converts this literal to a literal of the given type.
   *
   * <p>When a predicate is bound to a concrete data column, literals are converted to match the
   * bound column's type. This conversion process is more narrow than a cast and is only intended
   * for cases where substituting one type is a common mistake (e.g. 34 instead of 34L) or where
   * this API avoids requiring a concrete class (e.g., dates).
   *
   * <p>If conversion to a target type is not supported, this method returns null.
   *
   * <p>This method may return {@link Literals#aboveMax} or {@link Literals#belowMin} when the
   * target type is not as wide as the original type. These values indicate that the containing
   * predicate can be simplified. For example, Integer.MAX_VALUE+1 converted to an int will result
   * in {@code aboveMax} and can simplify a &lt; Integer.MAX_VALUE+1 to {@link
   * Expressions#alwaysTrue}
   *
   * @param type A primitive {@link Type}
   * @param <X> The Java type of value the new literal contains
   * @return A literal of the given type or null if conversion was not valid
   */
  <X> Literal<X> to(Type type);

  /**
   * Return a {@link Comparator} for values.
   *
   * @return a comparator for T objects
   */
  Comparator<T> comparator();

  /**
   * Serializes the value wrapped by this literal to binary using the single-value serialization
   * format described in the Iceberg table specification.
   *
   * @return a ByteBuffer that contains the serialized literal value.
   */
  default ByteBuffer toByteBuffer() {
    throw new UnsupportedOperationException("toByteBuffer is not supported");
  }
}
