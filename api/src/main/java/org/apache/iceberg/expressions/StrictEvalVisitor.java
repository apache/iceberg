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

import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.NaNUtil;

abstract class StrictEvalVisitor extends ExpressionVisitors.BoundExpressionVisitor<Boolean> {
  protected static final boolean ROWS_MUST_MATCH = true;
  protected static final boolean ROWS_MIGHT_NOT_MATCH = false;

  private final StructType struct;

  StrictEvalVisitor(StructType struct) {
    this.struct = struct;
  }

  /** Return true if null count is non-zero or is unknown, false if null count is 0. */
  protected abstract boolean mayContainNull(int id);

  /** Return true if null count is known and equal to value count, false otherwise. */
  protected abstract boolean containsNullsOnly(int id);

  /**
   * Return true if null counts are unavailable or if the null count is non-zero, false otherwise.
   *
   * <p>Unlike {@link #mayContainNull(int)}, this returns false when a null count is unavailable for
   * the field but is available for other fields.
   */
  protected abstract boolean canContainNulls(int id);

  /** Return true if NaN count is non-zero or is unknown, false if NaN count is 0. */
  protected abstract boolean mayContainNaN(int id);

  /** Return true if NaN count is known and equal to value count, false otherwise. */
  protected abstract boolean containsNaNsOnly(int id);

  /**
   * Return true if the NaN count is known and non-zero, false otherwise.
   *
   * <p>Unlike {@link #mayContainNaN(int)}, this returns false when the NaN count is unavailable.
   * NaN counts are not tracked for non-floating point fields or by early writers.
   */
  protected abstract boolean canContainNaNs(int id);

  /** Return the lower bound if it is known, or null otherwise. */
  protected abstract <T> T lowerBound(BoundReference<T> ref);

  /** Return the upper bound if it is known, or null otherwise. */
  protected abstract <T> T upperBound(BoundReference<T> ref);

  @Override
  public <T> Boolean handleNonReference(Bound<T> term) {
    // If the term in any expression is not a direct reference, assume that rows may not match. This
    // happens when transforms or other expressions are passed to this evaluator. For example,
    // bucket16(x) = 0 can't be determined because this visitor operates on data metrics and not
    // partition values. It may be possible to un-transform expressions for order preserving
    // transforms in the future, but this is not currently supported.
    return ROWS_MIGHT_NOT_MATCH;
  }

  @Override
  public Boolean alwaysTrue() {
    return ROWS_MUST_MATCH; // all rows match
  }

  @Override
  public Boolean alwaysFalse() {
    return ROWS_MIGHT_NOT_MATCH; // no rows match
  }

  @Override
  public Boolean not(Boolean result) {
    return !result;
  }

  @Override
  public Boolean and(Boolean leftResult, Boolean rightResult) {
    return leftResult && rightResult;
  }

  @Override
  public Boolean or(Boolean leftResult, Boolean rightResult) {
    return leftResult || rightResult;
  }

  @Override
  public <T> Boolean isNull(BoundReference<T> ref) {
    // no need to check whether the field is required because binding evaluates that case
    // if the column has any non-null values, the expression does not match
    int id = ref.fieldId();
    if (isNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (containsNullsOnly(id)) {
      return ROWS_MUST_MATCH;
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  @Override
  public <T> Boolean notNull(BoundReference<T> ref) {
    // no need to check whether the field is required because binding evaluates that case
    // if the column has any null values, the expression does not match
    int id = ref.fieldId();
    if (isNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (!mayContainNull(id)) {
      return ROWS_MUST_MATCH;
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  @Override
  public <T> Boolean isNaN(BoundReference<T> ref) {
    if (containsNaNsOnly(ref.fieldId())) {
      return ROWS_MUST_MATCH;
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  @Override
  public <T> Boolean notNaN(BoundReference<T> ref) {
    int id = ref.fieldId();

    if (!mayContainNaN(id)) {
      return ROWS_MUST_MATCH;
    }

    if (containsNullsOnly(id)) {
      return ROWS_MUST_MATCH;
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  @Override
  public <T> Boolean lt(BoundReference<T> ref, Literal<T> lit) {
    // Rows must match when: <----------Min----Max---X------->
    int id = ref.fieldId();
    if (isNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (canContainNulls(id) || canContainNaNs(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    T upper = upperBound(ref);
    if (null == upper) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    int cmp = lit.comparator().compare(upper, lit.value());
    if (cmp < 0) {
      return ROWS_MUST_MATCH;
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  @Override
  public <T> Boolean ltEq(BoundReference<T> ref, Literal<T> lit) {
    // Rows must match when: <----------Min----Max---X------->
    int id = ref.fieldId();
    if (isNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (canContainNulls(id) || canContainNaNs(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    T upper = upperBound(ref);
    if (null == upper) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    int cmp = lit.comparator().compare(upper, lit.value());
    if (cmp <= 0) {
      return ROWS_MUST_MATCH;
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  @Override
  public <T> Boolean gt(BoundReference<T> ref, Literal<T> lit) {
    // Rows must match when: <-------X---Min----Max---------->
    int id = ref.fieldId();
    if (isNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (canContainNulls(id) || canContainNaNs(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    T lower = lowerBound(ref);
    if (null == lower || NaNUtil.isNaN(lower)) {
      // NaN indicates unreliable bounds. See the StrictMetricsEvaluator docs for more.
      return ROWS_MIGHT_NOT_MATCH;
    }

    int cmp = lit.comparator().compare(lower, lit.value());
    if (cmp > 0) {
      return ROWS_MUST_MATCH;
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  @Override
  public <T> Boolean gtEq(BoundReference<T> ref, Literal<T> lit) {
    // Rows must match when: <-------X---Min----Max---------->
    int id = ref.fieldId();
    if (isNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (canContainNulls(id) || canContainNaNs(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    T lower = lowerBound(ref);
    if (null == lower || NaNUtil.isNaN(lower)) {
      // NaN indicates unreliable bounds. See the StrictMetricsEvaluator docs for more.
      return ROWS_MIGHT_NOT_MATCH;
    }

    int cmp = lit.comparator().compare(lower, lit.value());
    if (cmp >= 0) {
      return ROWS_MUST_MATCH;
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  @Override
  public <T> Boolean eq(BoundReference<T> ref, Literal<T> lit) {
    // Rows must match when Min == X == Max
    int id = ref.fieldId();
    if (isNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (canContainNulls(id) || canContainNaNs(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    T lower = lowerBound(ref);
    T upper = upperBound(ref);
    if (null == lower || null == upper) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (lit.comparator().compare(lower, lit.value()) != 0) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (lit.comparator().compare(upper, lit.value()) != 0) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    return ROWS_MUST_MATCH;
  }

  @Override
  public <T> Boolean notEq(BoundReference<T> ref, Literal<T> lit) {
    // Rows must match when X < Min or Max < X because it is not in the range
    int id = ref.fieldId();
    if (isNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (containsNullsOnly(id) || containsNaNsOnly(id)) {
      return ROWS_MUST_MATCH;
    }

    T lower = lowerBound(ref);
    if (lower != null) {
      if (NaNUtil.isNaN(lower)) {
        // NaN indicates unreliable bounds. See the StrictMetricsEvaluator docs for more.
        return ROWS_MIGHT_NOT_MATCH;
      }

      int cmp = lit.comparator().compare(lower, lit.value());
      if (cmp > 0) {
        return ROWS_MUST_MATCH;
      }
    }

    T upper = upperBound(ref);
    if (upper != null) {
      int cmp = lit.comparator().compare(upper, lit.value());
      if (cmp < 0) {
        return ROWS_MUST_MATCH;
      }
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  @Override
  public <T> Boolean in(BoundReference<T> ref, Set<T> literalSet) {
    int id = ref.fieldId();
    if (isNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (canContainNulls(id) || canContainNaNs(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    T lower = lowerBound(ref);
    T upper = upperBound(ref);
    if (null == lower || null == upper) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    // similar to the implementation in eq, first check if the lower bound is in the set
    if (!literalSet.contains(lower)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    // check if the upper bound is in the set
    if (!literalSet.contains(upper)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    // finally check if the lower bound and the upper bound are equal
    if (ref.comparator().compare(lower, upper) != 0) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    // all values must be in the set if the lower bound and the upper bound are in the set and are
    // equal
    return ROWS_MUST_MATCH;
  }

  @Override
  public <T> Boolean notIn(BoundReference<T> ref, Set<T> literalSet) {
    int id = ref.fieldId();
    if (isNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (containsNullsOnly(id) || containsNaNsOnly(id)) {
      return ROWS_MUST_MATCH;
    }

    Collection<T> literals = literalSet;

    T lower = lowerBound(ref);
    if (lower != null) {
      if (NaNUtil.isNaN(lower)) {
        // NaN indicates unreliable bounds. See the StrictMetricsEvaluator docs for more.
        return ROWS_MIGHT_NOT_MATCH;
      }

      literals =
          literals.stream()
              .filter(v -> ref.comparator().compare(lower, v) <= 0)
              .collect(Collectors.toList());
      // if all values are less than lower bound, rows must match (notIn)
      if (literals.isEmpty()) {
        return ROWS_MUST_MATCH;
      }
    }

    T upper = upperBound(ref);
    if (upper != null) {
      literals =
          literals.stream()
              .filter(v -> ref.comparator().compare(upper, v) >= 0)
              .collect(Collectors.toList());
      // if all remaining values are greater than upper bound, rows must match (notIn)
      if (literals.isEmpty()) {
        return ROWS_MUST_MATCH;
      }
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  @Override
  public <T> Boolean startsWith(BoundReference<T> ref, Literal<T> lit) {
    int id = ref.fieldId();
    if (isNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (canContainNulls(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    CharSequence lower = (CharSequence) lowerBound(ref);
    CharSequence upper = (CharSequence) upperBound(ref);
    if (null == lower || null == upper) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    String prefix = (String) lit.value();
    Comparator<CharSequence> comparator = Comparators.charSequences();

    // if lower is shorter than the prefix then lower doesn't start with the prefix
    if (lower.length() < prefix.length()) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (comparator.compare(lower.subSequence(0, prefix.length()), prefix) == 0) {
      // if upper is shorter than the prefix then upper can't start with the prefix
      if (upper.length() < prefix.length()) {
        return ROWS_MIGHT_NOT_MATCH;
      }

      if (comparator.compare(upper.subSequence(0, prefix.length()), prefix) == 0) {
        // both bounds start with the prefix, so all rows must start with the prefix
        return ROWS_MUST_MATCH;
      }
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  @Override
  public <T> Boolean notStartsWith(BoundReference<T> ref, Literal<T> lit) {
    int id = ref.fieldId();
    if (isNestedColumn(id)) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    if (containsNullsOnly(id)) {
      return ROWS_MUST_MATCH;
    }

    String prefix = (String) lit.value();
    Comparator<CharSequence> comparator = Comparators.charSequences();

    CharSequence lower = (CharSequence) lowerBound(ref);
    if (lower != null) {
      // truncate lower bound so that its length is not greater than the length of prefix
      int length = Math.min(prefix.length(), lower.length());
      if (comparator.compare(lower.subSequence(0, length), prefix) > 0) {
        return ROWS_MUST_MATCH;
      }
    }

    CharSequence upper = (CharSequence) upperBound(ref);
    if (upper != null) {
      // truncate upper bound so that its length is not greater than the length of prefix
      int length = Math.min(prefix.length(), upper.length());
      if (comparator.compare(upper.subSequence(0, length), prefix) < 0) {
        return ROWS_MUST_MATCH;
      }
    }

    return ROWS_MIGHT_NOT_MATCH;
  }

  /** Returns true if the field is not a top-level field of the schema. */
  private boolean isNestedColumn(int id) {
    return struct.field(id) == null;
  }
}
