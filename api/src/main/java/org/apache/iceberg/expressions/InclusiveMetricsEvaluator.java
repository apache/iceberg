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

import static org.apache.iceberg.expressions.Expressions.rewriteNot;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.NaNUtil;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantObject;

/**
 * Evaluates an {@link Expression} on a {@link DataFile} to test whether rows in the file may match.
 *
 * <p>This evaluation is inclusive: it returns true if a file may match and false if it cannot
 * match.
 *
 * <p>Files are passed to {@link #eval(ContentFile)}, which returns true if the file may contain
 * matching rows and false if the file cannot contain matching rows. Files may be skipped if and
 * only if the return value of {@code eval} is false.
 *
 * <p>Due to the comparison implementation of ORC stats, for float/double columns in ORC files, if
 * the first value in a file is NaN, metrics of this file will report NaN for both upper and lower
 * bound despite that the column could contain non-NaN data. Thus in some scenarios explicitly
 * checks for NaN is necessary in order to not skip files that may contain matching data.
 */
public class InclusiveMetricsEvaluator {
  private static final int IN_PREDICATE_LIMIT = 200;

  private final Expression expr;

  public InclusiveMetricsEvaluator(Schema schema, Expression unbound) {
    this(schema, unbound, true);
  }

  public InclusiveMetricsEvaluator(Schema schema, Expression unbound, boolean caseSensitive) {
    StructType struct = schema.asStruct();
    this.expr = Binder.bind(struct, rewriteNot(unbound), caseSensitive);
  }

  /**
   * Test whether the file may contain records that match the expression.
   *
   * @param file a data file
   * @return false if the file cannot contain rows that match the expression, true otherwise.
   */
  public boolean eval(ContentFile<?> file) {
    // TODO: detect the case where a column is missing from the file using file's max field id.
    return new MetricsEvalVisitor().eval(file);
  }

  private static final boolean ROWS_MIGHT_MATCH = true;
  private static final boolean ROWS_CANNOT_MATCH = false;

  private class MetricsEvalVisitor extends ExpressionVisitors.BoundVisitor<Boolean> {
    private Map<Integer, Long> valueCounts = null;
    private Map<Integer, Long> nullCounts = null;
    private Map<Integer, Long> nanCounts = null;
    private Map<Integer, ByteBuffer> lowerBounds = null;
    private Map<Integer, ByteBuffer> upperBounds = null;

    private boolean eval(ContentFile<?> file) {
      if (file.recordCount() == 0) {
        return ROWS_CANNOT_MATCH;
      }

      if (file.recordCount() < 0) {
        // we haven't implemented parsing record count from avro file and thus set record count -1
        // when importing avro tables to iceberg tables. This should be updated once we implemented
        // and set correct record count.
        return ROWS_MIGHT_MATCH;
      }

      this.valueCounts = file.valueCounts();
      this.nullCounts = file.nullValueCounts();
      this.nanCounts = file.nanValueCounts();
      this.lowerBounds = file.lowerBounds();
      this.upperBounds = file.upperBounds();

      return ExpressionVisitors.visitEvaluator(expr, this);
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

      if (nanCounts != null && nanCounts.containsKey(id) && nanCounts.get(id) == 0) {
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

      T lower = lowerBound(term);
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

      T lower = lowerBound(term);
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

      T upper = upperBound(term);
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

      T upper = upperBound(term);
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

      T lower = lowerBound(term);
      if (lower != null && !NaNUtil.isNaN(lower)) {
        int cmp = lit.comparator().compare(lower, lit.value());
        if (cmp > 0) {
          return ROWS_CANNOT_MATCH;
        }
      }

      T upper = upperBound(term);
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

      T lower = lowerBound(term);
      if (null == lower || NaNUtil.isNaN(lower)) {
        // NaN indicates unreliable bounds. See the InclusiveMetricsEvaluator docs for more.
        return ROWS_MIGHT_MATCH;
      }

      literals =
          literals.stream()
              .filter(v -> ((BoundTerm<T>) term).comparator().compare(lower, v) <= 0)
              .collect(Collectors.toList());
      // if all values are less than lower bound, rows cannot match
      if (literals.isEmpty()) {
        return ROWS_CANNOT_MATCH;
      }

      T upper = upperBound(term);
      if (null == upper) {
        return ROWS_MIGHT_MATCH;
      }

      literals =
          literals.stream()
              .filter(v -> ((BoundTerm<T>) term).comparator().compare(upper, v) >= 0)
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
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean startsWith(Bound<T> term, Literal<T> lit) {
      if (term instanceof BoundTransform
          && !((BoundTransform<?, ?>) term).transform().isIdentity()) {
        // truncate must be rewritten in binding. the result is either always or never compatible
        return ROWS_MIGHT_MATCH;
      }

      int id = term.ref().fieldId();
      if (containsNullsOnly(id)) {
        return ROWS_CANNOT_MATCH;
      }

      String prefix = (String) lit.value();

      Comparator<CharSequence> comparator = Comparators.charSequences();

      CharSequence lower = (CharSequence) lowerBound(term);
      if (null == lower) {
        return ROWS_MIGHT_MATCH;
      }

      // truncate lower bound so that its length in bytes is not greater than the length of prefix
      int length = Math.min(prefix.length(), lower.length());
      int cmp = comparator.compare(lower.subSequence(0, length), prefix);
      if (cmp > 0) {
        return ROWS_CANNOT_MATCH;
      }

      CharSequence upper = (CharSequence) upperBound(term);
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
      CharSequence lower = (CharSequence) lowerBound(term);
      CharSequence upper = (CharSequence) upperBound(term);
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

    private boolean mayContainNull(Integer id) {
      return nullCounts == null || !nullCounts.containsKey(id) || nullCounts.get(id) != 0;
    }

    private boolean containsNullsOnly(Integer id) {
      return valueCounts != null
          && valueCounts.containsKey(id)
          && nullCounts != null
          && nullCounts.containsKey(id)
          && valueCounts.get(id) - nullCounts.get(id) == 0;
    }

    private boolean containsNaNsOnly(Integer id) {
      return nanCounts != null
          && nanCounts.containsKey(id)
          && valueCounts != null
          && nanCounts.get(id).equals(valueCounts.get(id));
    }

    private <T> T lowerBound(Bound<T> term) {
      if (term instanceof BoundReference) {
        return parseLowerBound((BoundReference<T>) term);
      } else if (term instanceof BoundTransform) {
        return transformLowerBound((BoundTransform<?, T>) term);
      } else if (term instanceof BoundExtract) {
        return extractLowerBound((BoundExtract<T>) term);
      } else {
        return null;
      }
    }

    private <T> T upperBound(Bound<T> term) {
      if (term instanceof BoundReference) {
        return parseUpperBound((BoundReference<T>) term);
      } else if (term instanceof BoundTransform) {
        return transformUpperBound((BoundTransform<?, T>) term);
      } else if (term instanceof BoundExtract) {
        return extractUpperBound((BoundExtract<T>) term);
      } else {
        return null;
      }
    }

    private <T> T parseLowerBound(BoundReference<T> ref) {
      int id = ref.fieldId();
      if (lowerBounds != null && lowerBounds.containsKey(id)) {
        return Conversions.fromByteBuffer(ref.ref().type(), lowerBounds.get(id));
      }

      return null;
    }

    private <T> T parseUpperBound(BoundReference<T> ref) {
      int id = ref.fieldId();
      if (upperBounds != null && upperBounds.containsKey(id)) {
        return Conversions.fromByteBuffer(ref.ref().type(), upperBounds.get(id));
      }

      return null;
    }

    private <S, T> T transformLowerBound(BoundTransform<S, T> boundTransform) {
      Transform<S, T> transform = boundTransform.transform();
      if (transform.preservesOrder()) {
        S lower = parseLowerBound(boundTransform.ref());
        return boundTransform.transform().bind(boundTransform.ref().type()).apply(lower);
      }

      return null;
    }

    private <S, T> T transformUpperBound(BoundTransform<S, T> boundTransform) {
      Transform<S, T> transform = boundTransform.transform();
      if (transform.preservesOrder()) {
        S upper = parseUpperBound(boundTransform.ref());
        return boundTransform.transform().bind(boundTransform.ref().type()).apply(upper);
      }

      return null;
    }

    private <T> T extractLowerBound(BoundExtract<T> bound) {
      int id = bound.ref().fieldId();
      if (lowerBounds != null && lowerBounds.containsKey(id)) {
        VariantObject fieldLowerBounds = parseBounds(lowerBounds.get(id));
        return VariantExpressionUtil.castTo(fieldLowerBounds.get(bound.path()), bound.type());
      }

      return null;
    }

    private <T> T extractUpperBound(BoundExtract<T> bound) {
      int id = bound.ref().fieldId();
      if (upperBounds != null && upperBounds.containsKey(id)) {
        VariantObject fieldUpperBounds = parseBounds(upperBounds.get(id));
        return VariantExpressionUtil.castTo(fieldUpperBounds.get(bound.path()), bound.type());
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

  private static VariantObject parseBounds(ByteBuffer buffer) {
    return Variant.from(buffer).value().asObject();
  }
}
