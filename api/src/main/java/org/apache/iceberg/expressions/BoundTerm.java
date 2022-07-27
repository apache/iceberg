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

import java.util.Comparator;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;

/**
 * Represents a bound term.
 *
 * @param <T> the Java type of values produced by this term
 */
public interface BoundTerm<T> extends Bound<T>, Term {
  /** Returns the type produced by this expression. */
  Type type();

  /** Returns a {@link Comparator} for values produced by this term. */
  default Comparator<T> comparator() {
    return Comparators.forType(type().asPrimitiveType());
  }

  /**
   * Returns whether this term is equivalent to another.
   *
   * @param other a term
   * @return true if this term returns the same values as the other, false otherwise
   */
  boolean isEquivalentTo(BoundTerm<?> other);
}
