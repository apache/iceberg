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

import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Sorted copies of {@code IN} predicate literal sets, for testing literals against bounds.
 *
 * <p>Evaluators create one instance and share it across the files or manifests they evaluate, so
 * each literal set is sorted once. {@link #anyInRange(List, Comparator, Object, Object)} then tests
 * bounds with a binary search instead of a scan of every literal.
 *
 * <p>This class is thread-safe.
 */
class SortedInLiterals {
  // Keyed by identity: each bound IN predicate holds one literal set instance, which visitors
  // receive in in(), and two predicates with equal sets may use different comparators.
  @SuppressWarnings("IdentityHashMapUsage")
  private final Map<Set<?>, List<?>> sortedLiterals =
      Collections.synchronizedMap(new IdentityHashMap<>());

  /**
   * Return the literals of an {@code IN} predicate sorted by the given comparator.
   *
   * <p>The result is cached by the identity of {@code literalSet}, so callers must always pass the
   * same comparator for a given set, the comparator of the predicate's term.
   *
   * @param literalSet the literal set of a bound IN predicate
   * @param comparator the comparator of the predicate's term
   * @return the sorted literals
   */
  @SuppressWarnings("unchecked")
  <T> List<T> sorted(Set<T> literalSet, Comparator<T> comparator) {
    return (List<T>)
        sortedLiterals.computeIfAbsent(literalSet, set -> sort(literalSet, comparator));
  }

  /**
   * Return whether any of the sorted literals falls within {@code [lower, upper]}.
   *
   * @param sortedLiterals literals sorted by {@code comparator}
   * @param comparator the comparator used to sort the literals
   * @param lower the inclusive lower bound
   * @param upper the inclusive upper bound, or null if there is no upper bound
   * @return true if a literal is within the bounds, false otherwise
   */
  static <T> boolean anyInRange(
      List<T> sortedLiterals, Comparator<T> comparator, T lower, T upper) {
    int pos = Collections.binarySearch(sortedLiterals, lower, comparator);
    if (pos >= 0) {
      return true;
    }

    // the first literal greater than lower is in range if it is not greater than upper
    int next = -(pos + 1);
    return next < sortedLiterals.size()
        && (upper == null || comparator.compare(upper, sortedLiterals.get(next)) >= 0);
  }

  private static <T> List<T> sort(Set<T> literals, Comparator<T> comparator) {
    List<T> sorted = Lists.newArrayList(literals);
    sorted.sort(comparator);
    return sorted;
  }
}
