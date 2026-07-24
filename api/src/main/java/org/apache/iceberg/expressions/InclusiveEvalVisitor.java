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
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.util.NaNUtil;

abstract class InclusiveEvalVisitor extends ExpressionVisitors.BoundVisitor<Boolean> {
  private static final int IN_PREDICATE_LIMIT = 200;

  protected static final boolean ROWS_MIGHT_MATCH = true;
  protected static final boolean ROWS_CANNOT_MATCH = false;

  /** Return true if null count is non-zero or is unknown, false if null count is 0. */
  protected abstract boolean mayContainNull(int id);

  /** Return true if null count is known and equal to value count, false otherwise. */
  protected abstract boolean containsNullsOnly(int id);

  /** Return true if NaN count is non-zero or is unknown, false if NaN count is 0. */
  protected abstract boolean mayContainNaN(int id);

  /** Return true if NaN count is known and equal to value count, false otherwise. */
  protected abstract boolean containsNaNsOnly(int id);

  /** Return the lower bound if it is known, or null otherwise. */
  protected <T> T lowerBound(BoundReference<T> ref) {
    return null;
  }

  /** Return the upper bound if it is known, or null otherwise. */
  protected <T> T upperBound(BoundReference<T> ref) {
    return null;
  }

  /** Return a variant field's lower bound if it is known, or null otherwise. */
  protected <T> T extractLowerBound(BoundExtract<T> bound) {
    return null;
  }

  /** Return a variant field's upper bound if it is known, or null otherwise. */
  protected <T> T extractUpperBound(BoundExtract<T> bound) {
    return null;
  }

  @Override
  public Boolean alwaysTrue() {
    return ROWS_MIGHT_MATCH; // all rows match
  }

  @Override
  public Boolean alwaysFalse() {
    return ROWS_CANNOT_MATCH; // all rows fail
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
  public <T> Boolean isNull(Bound<T> term) {
    // no need to check whether the field is required because binding evaluates that case
    // if the column has no null values, the expression cannot match
    if (isNonNullPreserving(term)) {
      // number of non-nulls is the same as for the ref
      int id = term.ref().fieldId();
      if (!mayContainNull(id)) {
        return ROWS_CANNOT_MATCH;
      }
    }

    return ROWS_MIGHT_MATCH;
  }

  @Override
  public <T> Boolean notNull(Bound<T> term) {
    // no need to check whether the field is required because binding evaluates that case
    // if the column has no non-null values, the expression cannot match

    // all terms are null preserving. see #isNullPreserving(Bound)
    int id = term.ref().fieldId();
    if (containsNullsOnly(id)) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  @Override
  public <T> Boolean isNaN(Bound<T> term) {
    // when there's no nanCounts information, but we already know the column only contains null,
    // it's guaranteed that there's no NaN value
    int id = term.ref().fieldId();
    if (containsNullsOnly(id)) {
      return ROWS_CANNOT_MATCH;
    }

    if (!(term instanceof BoundReference)) {
      return ROWS_MIGHT_MATCH;
    }

    if (!mayContainNaN(id)) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  @Override
  public <T> Boolean notNaN(Bound<T> term) {
    if (!(term instanceof BoundReference)) {
      // identity transforms are already removed by this time
      return ROWS_MIGHT_MATCH;
    }

    int id = term.ref().fieldId();

    if (containsNaNsOnly(id)) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  @Override
  public <T> Boolean lt(Bound<T> term, Literal<T> lit) {
    // all terms are null preserving. see #isNullPreserving(Bound)
    int id = term.ref().fieldId();
    if (containsNullsOnly(id) || containsNaNsOnly(id)) {
      return ROWS_CANNOT_MATCH;
    }

    T lower = evalLowerBound(term);
    if (null == lower || NaNUtil.isNaN(lower)) {
      // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
      return ROWS_MIGHT_MATCH;
    }

    // this also works for transforms that are order preserving:
    // if a transform f is order preserving, a < b means that f(a) <= f(b).
    // because lower <= a for all values of a in the file, f(lower) <= f(a).
    // when f(lower) >= X then f(a) >= f(lower) >= X, so there is no a such that f(a) < X
    // f(lower) >= X means rows cannot match
    int cmp = lit.comparator().compare(lower, lit.value());
    if (cmp >= 0) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  @Override
  public <T> Boolean ltEq(Bound<T> term, Literal<T> lit) {
    // all terms are null preserving. see #isNullPreserving(Bound)
    int id = term.ref().fieldId();
    if (containsNullsOnly(id) || containsNaNsOnly(id)) {
      return ROWS_CANNOT_MATCH;
    }

    T lower = evalLowerBound(term);
    if (null == lower || NaNUtil.isNaN(lower)) {
      // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
      return ROWS_MIGHT_MATCH;
    }

    // this also works for transforms that are order preserving:
    // if a transform f is order preserving, a < b means that f(a) <= f(b).
    // because lower <= a for all values of a in the file, f(lower) <= f(a).
    // when f(lower) > X then f(a) >= f(lower) > X, so there is no a such that f(a) <= X
    // f(lower) > X means rows cannot match
    int cmp = lit.comparator().compare(lower, lit.value());
    if (cmp > 0) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  @Override
  public <T> Boolean gt(Bound<T> term, Literal<T> lit) {
    // all terms are null preserving. see #isNullPreserving(Bound)
    int id = term.ref().fieldId();
    if (containsNullsOnly(id) || containsNaNsOnly(id)) {
      return ROWS_CANNOT_MATCH;
    }

    T upper = evalUpperBound(term);
    if (null == upper) {
      return ROWS_MIGHT_MATCH;
    }

    int cmp = lit.comparator().compare(upper, lit.value());
    if (cmp <= 0) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  @Override
  public <T> Boolean gtEq(Bound<T> term, Literal<T> lit) {
    // all terms are null preserving. see #isNullPreserving(Bound)
    int id = term.ref().fieldId();
    if (containsNullsOnly(id) || containsNaNsOnly(id)) {
      return ROWS_CANNOT_MATCH;
    }

    T upper = evalUpperBound(term);
    if (null == upper) {
      return ROWS_MIGHT_MATCH;
    }

    int cmp = lit.comparator().compare(upper, lit.value());
    if (cmp < 0) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  @Override
  public <T> Boolean eq(Bound<T> term, Literal<T> lit) {
    // all terms are null preserving. see #isNullPreserving(Bound)
    int id = term.ref().fieldId();
    if (containsNullsOnly(id) || containsNaNsOnly(id)) {
      return ROWS_CANNOT_MATCH;
    }

    T lower = evalLowerBound(term);
    if (lower != null && !NaNUtil.isNaN(lower)) {
      int cmp = lit.comparator().compare(lower, lit.value());
      if (cmp > 0) {
        return ROWS_CANNOT_MATCH;
      }
    }

    T upper = evalUpperBound(term);
    if (null == upper) {
      return ROWS_MIGHT_MATCH;
    }

    int cmp = lit.comparator().compare(upper, lit.value());
    if (cmp < 0) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  @Override
  public <T> Boolean notEq(Bound<T> term, Literal<T> lit) {
    // because the bounds are not necessarily a min or max value, this cannot be answered using
    // them. notEq(col, X) with (X, Y) doesn't guarantee that X is a value in col.
    // However, when min == max and the file has no nulls or NaN values, we can safely prune
    // if that value equals the literal.
    T value = uniqueValue(term);

    if (value != null && lit.comparator().compare(value, lit.value()) == 0) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  @Override
  public <T> Boolean in(Bound<T> term, Set<T> literalSet) {
    // all terms are null preserving. see #isNullPreserving(Bound)
    int id = term.ref().fieldId();
    if (containsNullsOnly(id) || containsNaNsOnly(id)) {
      return ROWS_CANNOT_MATCH;
    }

    Collection<T> literals = literalSet;

    if (literals.size() > IN_PREDICATE_LIMIT) {
      // skip evaluating the predicate if the number of values is too big
      return ROWS_MIGHT_MATCH;
    }

    T lower = evalLowerBound(term);
    if (null == lower || NaNUtil.isNaN(lower)) {
      // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
      return ROWS_MIGHT_MATCH;
    }

    Comparator<T> comparator = ((BoundTerm<T>) term).comparator();

    literals =
        literals.stream()
            .filter(v -> comparator.compare(lower, v) <= 0)
            .collect(Collectors.toList());
    // if all values are less than lower bound, rows cannot match
    if (literals.isEmpty()) {
      return ROWS_CANNOT_MATCH;
    }

    T upper = evalUpperBound(term);
    if (null == upper) {
      return ROWS_MIGHT_MATCH;
    }

    literals =
        literals.stream()
            .filter(v -> comparator.compare(upper, v) >= 0)
            .collect(Collectors.toList());
    // if remaining values are greater than upper bound, rows cannot match
    if (literals.isEmpty()) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  @Override
  public <T> Boolean notIn(Bound<T> term, Set<T> literalSet) {
    // because the bounds are not necessarily a min or max value, this cannot be answered using
    // them. notIn(col, {X, ...}) with (X, Y) doesn't guarantee that X is a value in col.
    // However, when min == max and the file has no nulls or NaN values, we can safely prune
    // if that value is in the exclusion set.
    T value = uniqueValue(term);

    if (value != null && literalSet.contains(value)) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  @Override
  public <T> Boolean startsWith(Bound<T> term, Literal<T> lit) {
    if (term instanceof BoundTransform && !((BoundTransform<?, ?>) term).transform().isIdentity()) {
      // truncate must be rewritten in binding. the result is either always or never compatible
      return ROWS_MIGHT_MATCH;
    }

    int id = term.ref().fieldId();
    if (containsNullsOnly(id)) {
      return ROWS_CANNOT_MATCH;
    }

    String prefix = (String) lit.value();

    Comparator<CharSequence> comparator = Comparators.charSequences();

    CharSequence lower = (CharSequence) evalLowerBound(term);
    if (null == lower) {
      return ROWS_MIGHT_MATCH;
    }

    // truncate lower bound so that its length in bytes is not greater than the length of prefix
    int length = Math.min(prefix.length(), lower.length());
    int cmp = comparator.compare(lower.subSequence(0, length), prefix);
    if (cmp > 0) {
      return ROWS_CANNOT_MATCH;
    }

    CharSequence upper = (CharSequence) evalUpperBound(term);
    if (null == upper) {
      return ROWS_MIGHT_MATCH;
    }

    // truncate upper bound so that its length in bytes is not greater than the length of prefix
    length = Math.min(prefix.length(), upper.length());
    cmp = comparator.compare(upper.subSequence(0, length), prefix);
    if (cmp < 0) {
      return ROWS_CANNOT_MATCH;
    }

    return ROWS_MIGHT_MATCH;
  }

  @Override
  public <T> Boolean notStartsWith(Bound<T> term, Literal<T> lit) {
    // the only transforms that produce strings are truncate and identity, which work with this
    int id = term.ref().fieldId();
    if (mayContainNull(id)) {
      return ROWS_MIGHT_MATCH;
    }

    String prefix = (String) lit.value();

    Comparator<CharSequence> comparator = Comparators.charSequences();

    // notStartsWith will match unless all values must start with the prefix. This happens when
    // the lower and upper bounds both start with the prefix.
    CharSequence lower = (CharSequence) evalLowerBound(term);
    CharSequence upper = (CharSequence) evalUpperBound(term);
    if (null == lower || null == upper) {
      return ROWS_MIGHT_MATCH;
    }

    // if lower is shorter than the prefix then lower doesn't start with the prefix
    if (lower.length() < prefix.length()) {
      return ROWS_MIGHT_MATCH;
    }

    int cmp = comparator.compare(lower.subSequence(0, prefix.length()), prefix);
    if (cmp == 0) {
      // if upper is shorter than the prefix then upper can't start with the prefix
      if (upper.length() < prefix.length()) {
        return ROWS_MIGHT_MATCH;
      }

      cmp = comparator.compare(upper.subSequence(0, prefix.length()), prefix);
      if (cmp == 0) {
        // both bounds match the prefix, so all rows must match the prefix and therefore do not
        // satisfy the predicate
        return ROWS_CANNOT_MATCH;
      }
    }

    return ROWS_MIGHT_MATCH;
  }

  /**
   * Returns the column's single value if all rows contain the same value. Defined as a column with
   * no nulls, no NaNs, and lower bound equals upper bound. Returns null otherwise.
   */
  private <T> T uniqueValue(Bound<T> term) {
    int id = term.ref().fieldId();
    if (mayContainNull(id)) {
      return null;
    }

    T lower = evalLowerBound(term);
    T upper = evalUpperBound(term);

    if (lower == null || upper == null || NaNUtil.isNaN(lower) || NaNUtil.isNaN(upper)) {
      return null;
    }

    if ((lower instanceof Float || lower instanceof Double) && mayContainNaN(id)) {
      return null;
    }

    if (!lower.equals(upper)) {
      return null;
    }

    return lower;
  }

  private <T> T evalLowerBound(Bound<T> term) {
    if (term instanceof BoundReference) {
      return lowerBound((BoundReference<T>) term);
    } else if (term instanceof BoundTransform) {
      return transformLowerBound((BoundTransform<?, T>) term);
    } else if (term instanceof BoundExtract) {
      return extractLowerBound((BoundExtract<T>) term);
    } else {
      return null;
    }
  }

  private <T> T evalUpperBound(Bound<T> term) {
    if (term instanceof BoundReference) {
      return upperBound((BoundReference<T>) term);
    } else if (term instanceof BoundTransform) {
      return transformUpperBound((BoundTransform<?, T>) term);
    } else if (term instanceof BoundExtract) {
      return extractUpperBound((BoundExtract<T>) term);
    } else {
      return null;
    }
  }

  private <S, T> T transformLowerBound(BoundTransform<S, T> boundTransform) {
    Transform<S, T> transform = boundTransform.transform();
    if (transform.preservesOrder()) {
      S lower = lowerBound(boundTransform.ref());
      return boundTransform.transform().bind(boundTransform.ref().type()).apply(lower);
    }

    return null;
  }

  private <S, T> T transformUpperBound(BoundTransform<S, T> boundTransform) {
    Transform<S, T> transform = boundTransform.transform();
    if (transform.preservesOrder()) {
      S upper = upperBound(boundTransform.ref());
      return boundTransform.transform().bind(boundTransform.ref().type()).apply(upper);
    }

    return null;
  }

  /** Returns true if the expression term produces a non-null value for non-null input. */
  private boolean isNonNullPreserving(Bound<?> term) {
    if (term instanceof BoundReference) {
      return true;
    } else if (term instanceof BoundTransform<?, ?>) {
      // if the transform preserves order, then non-null values are mapped to non-null
      return ((BoundTransform<?, ?>) term).transform().preservesOrder();
    }

    // a non-null variant does not necessarily contain a specific field
    // and unknown bound terms are not non-null preserving
    return false;
  }
}
