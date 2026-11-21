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
import java.util.Set;

final class PredicateComparisons {
  private PredicateComparisons() {}

  static <T> int compare(T left, T right, Comparator<T> comparator) {
    if (left instanceof Double l
        && right instanceof Double r
        && l.doubleValue() == 0.0D
        && r.doubleValue() == 0.0D) {
      return 0;
    }

    if (left instanceof Float l
        && right instanceof Float r
        && l.floatValue() == 0.0F
        && r.floatValue() == 0.0F) {
      return 0;
    }

    return comparator.compare(left, right);
  }

  static boolean contains(Set<?> values, Object value) {
    if (value == null) {
      return false;
    }

    if (values.contains(value)) {
      return true;
    }

    if (value instanceof Double d && d.doubleValue() == 0.0D) {
      return values.contains(Double.valueOf(-d.doubleValue()));
    }

    if (value instanceof Float f && f.floatValue() == 0.0F) {
      return values.contains(Float.valueOf(-f.floatValue()));
    }

    return false;
  }
}
