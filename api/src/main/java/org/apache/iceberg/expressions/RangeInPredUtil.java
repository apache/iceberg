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
import java.util.NavigableSet;
import java.util.function.Supplier;

public final class RangeInPredUtil {
  private static final boolean KEEP = true;
  private static final boolean DISCARD = false;

  private RangeInPredUtil() {}

  public static <T> boolean isInRange(
      Supplier<T> lowerBoundSupplier,
      Supplier<T> upperBoundSupplier,
      NavigableSet<T> rangeSet,
      boolean keepIfBoundsNull) {
    if (rangeSet.isEmpty()) {
      return DISCARD;
    }

    T lower = lowerBoundSupplier.get();
    T upper = upperBoundSupplier.get();
    Comparator<? super T> comparator = rangeSet.comparator();
    if (lower != null && upper != null) {
      T leastElementGELower = rangeSet.ceiling(lower);
      if (leastElementGELower == null) {
        return DISCARD;
      } else if (comparator.compare(leastElementGELower, lower) == 0) {
        return KEEP;
      } else if (comparator.compare(leastElementGELower, upper) > 0) {
        return DISCARD;
      } else {
        return KEEP;
      }
    } else if (upper != null) {
      return rangeSet.floor(upper) != null ? KEEP : DISCARD;
    } else if (lower != null) {
      return rangeSet.ceiling(lower) != null ? KEEP : DISCARD;
    } else {
      return keepIfBoundsNull ? KEEP : DISCARD;
    }
  }

  public static <T> boolean isStrictlyInRange(
      Supplier<T> lowerBoundSupplier,
      Supplier<T> upperBoundSupplier,
      NavigableSet<T> rangeSet,
      boolean keepIfBoundsNull) {
    if (rangeSet.isEmpty()) {
      return DISCARD;
    }

    T lower = lowerBoundSupplier.get();
    T upper = upperBoundSupplier.get();
    Comparator<? super T> comparator = rangeSet.comparator();
    if (rangeSet.contains(lower)
        && rangeSet.contains(upper)
        && comparator.compare(lower, upper) == 0) {
      return KEEP;
    } else {
      return DISCARD;
    }
  }
}
