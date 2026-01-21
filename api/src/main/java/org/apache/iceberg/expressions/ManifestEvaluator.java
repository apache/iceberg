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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Accessors;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFile.PartitionFieldSummary;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.expressions.ExpressionVisitors.BoundExpressionVisitor;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.BinaryUtil;

/**
 * Evaluates an {@link Expression} on a {@link ManifestFile} to test whether the file contains
 * matching partitions.
 *
 * <p>For row expressions, evaluation is inclusive: it returns true if a file may match and false if
 * it cannot match.
 *
 * <p>Files are passed to {@link #eval(ManifestFile)}, which returns true if the manifest may
 * contain data files that match the partition expression. Manifest files may be skipped if and only
 * if the return value of {@code eval} is false.
 */
public class ManifestEvaluator {
  private static final int IN_PREDICATE_LIMIT = 200;

  private final Expression expr;
  // Expression using signed UUID comparator for backward compatibility with files written before
  // RFC-compliant UUID comparisons were introduced. Null if no UUID predicates.
  private final Expression signedUuidExpr;

  public static ManifestEvaluator forRowFilter(
      Expression rowFilter, PartitionSpec spec, boolean caseSensitive) {
    return new ManifestEvaluator(
        spec, Projections.inclusive(spec, caseSensitive).project(rowFilter), caseSensitive);
  }

  public static ManifestEvaluator forPartitionFilter(
      Expression partitionFilter, PartitionSpec spec, boolean caseSensitive) {
    return new ManifestEvaluator(spec, partitionFilter, caseSensitive);
  }

  private ManifestEvaluator(PartitionSpec spec, Expression partitionFilter, boolean caseSensitive) {
    Types.StructType partitionType = spec.partitionType();
    Expression rewritten = rewriteNot(partitionFilter);
    this.expr = Binder.bind(partitionType, rewritten, caseSensitive);

    // Create the signed UUID expression iff there are UUID predicates that compare against bounds.
    this.signedUuidExpr =
        ExpressionUtil.toSignedUUIDLiteral(rewritten)
            .map(transformed -> Binder.bind(partitionType, transformed, caseSensitive))
            .orElse(null);
  }

  /**
   * Test whether the file may contain records that match the expression.
   *
   * @param manifest a manifest file
   * @return false if the file cannot contain rows that match the expression, true otherwise.
   */
  public boolean eval(ManifestFile manifest) {
    boolean result = new ManifestEvalVisitor().eval(manifest, expr, false);

    // If the RFC-compliant evaluation says rows might match, or there's no signed UUID expression,
    // return the result.
    if (result || signedUuidExpr == null) {
      return result;
    }

    // Always try with signed UUID comparator as a fallback. There is no reliable way to detect
    // which comparator was used when the manifest's partition field summaries were written.
    return new ManifestEvalVisitor().eval(manifest, signedUuidExpr, true);
  }

  private static final boolean ROWS_MIGHT_MATCH = true;
  private static final boolean ROWS_CANNOT_MATCH = false;

  private class ManifestEvalVisitor extends BoundExpressionVisitor<Boolean> {
    private List<PartitionFieldSummary> stats = null;
    // Flag to use signed UUID comparator for backward compatibility.
    // This is needed for the IN predicate because the comparator information is lost
    // when binding converts literals to a Set<T> of raw values.
    private boolean useSignedUuidComparator = false;

    private boolean eval(ManifestFile manifest, Expression expression, boolean signedUuidMode) {
      this.stats = manifest.partitions();
      this.useSignedUuidComparator = signedUuidMode;
      if (stats == null) {
        return ROWS_MIGHT_MATCH;
      }

      return ExpressionVisitors.visitEvaluator(expression, this);
    }

    /**
     * Returns the appropriate comparator for the given reference. This is needed for the IN
     * predicate because the comparator information is lost when binding converts literals to a Set
     * of raw values. For other predicates, the literal carries the comparator.
     */
    @SuppressWarnings("unchecked")
    private <T> Comparator<T> comparatorForIn(BoundReference<T> ref) {
      if (useSignedUuidComparator && ref.type().typeId() == Type.TypeID.UUID) {
        return (Comparator<T>) Comparators.signedUUIDs();
      }

      return ref.comparator();
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
    public <T> Boolean isNull(BoundReference<T> ref) {
      int pos = Accessors.toPosition(ref.accessor());
      // no need to check whether the field is required because binding evaluates that case
      // if the column has no null values, the expression cannot match
      if (!stats.get(pos).containsNull()) {
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean notNull(BoundReference<T> ref) {
      int pos = Accessors.toPosition(ref.accessor());

      if (allValuesAreNull(stats.get(pos), ref.type().typeId())) {
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean isNaN(BoundReference<T> ref) {
      int pos = Accessors.toPosition(ref.accessor());

      if (stats.get(pos).containsNaN() != null && !stats.get(pos).containsNaN()) {
        return ROWS_CANNOT_MATCH;
      }

      if (allValuesAreNull(stats.get(pos), ref.type().typeId())) {
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean notNaN(BoundReference<T> ref) {
      PartitionFieldSummary fieldSummary = stats.get(Accessors.toPosition(ref.accessor()));

      // if containsNaN is true, containsNull is false and lowerBound is null, all values are NaN
      if (fieldSummary.containsNaN() != null
          && fieldSummary.containsNaN()
          && !fieldSummary.containsNull()
          && fieldSummary.lowerBound() == null) {
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean lt(BoundReference<T> ref, Literal<T> lit) {
      int pos = Accessors.toPosition(ref.accessor());
      ByteBuffer lowerBound = stats.get(pos).lowerBound();
      if (lowerBound == null) {
        return ROWS_CANNOT_MATCH; // values are all null
      }

      T lower = Conversions.fromByteBuffer(ref.type(), lowerBound);

      int cmp = lit.comparator().compare(lower, lit.value());
      if (cmp >= 0) {
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean ltEq(BoundReference<T> ref, Literal<T> lit) {
      int pos = Accessors.toPosition(ref.accessor());
      ByteBuffer lowerBound = stats.get(pos).lowerBound();
      if (lowerBound == null) {
        return ROWS_CANNOT_MATCH; // values are all null
      }

      T lower = Conversions.fromByteBuffer(ref.type(), lowerBound);

      int cmp = lit.comparator().compare(lower, lit.value());
      if (cmp > 0) {
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean gt(BoundReference<T> ref, Literal<T> lit) {
      int pos = Accessors.toPosition(ref.accessor());
      ByteBuffer upperBound = stats.get(pos).upperBound();
      if (upperBound == null) {
        return ROWS_CANNOT_MATCH; // values are all null
      }

      T upper = Conversions.fromByteBuffer(ref.type(), upperBound);

      int cmp = lit.comparator().compare(upper, lit.value());
      if (cmp <= 0) {
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean gtEq(BoundReference<T> ref, Literal<T> lit) {
      int pos = Accessors.toPosition(ref.accessor());
      ByteBuffer upperBound = stats.get(pos).upperBound();
      if (upperBound == null) {
        return ROWS_CANNOT_MATCH; // values are all null
      }

      T upper = Conversions.fromByteBuffer(ref.type(), upperBound);

      int cmp = lit.comparator().compare(upper, lit.value());
      if (cmp < 0) {
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean eq(BoundReference<T> ref, Literal<T> lit) {
      int pos = Accessors.toPosition(ref.accessor());
      PartitionFieldSummary fieldStats = stats.get(pos);
      if (fieldStats.lowerBound() == null) {
        return ROWS_CANNOT_MATCH; // values are all null and literal cannot contain null
      }

      T lower = Conversions.fromByteBuffer(ref.type(), fieldStats.lowerBound());
      int cmp = lit.comparator().compare(lower, lit.value());
      if (cmp > 0) {
        return ROWS_CANNOT_MATCH;
      }

      T upper = Conversions.fromByteBuffer(ref.type(), fieldStats.upperBound());
      cmp = lit.comparator().compare(upper, lit.value());
      if (cmp < 0) {
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean notEq(BoundReference<T> ref, Literal<T> lit) {
      // because the bounds are not necessarily a min or max value, this cannot be answered using
      // them. notEq(col, X) with (X, Y) doesn't guarantee that X is a value in col.
      // However, when lower == upper and the manifest has no nulls or NaN values, we can safely
      // prune if that value equals the literal.
      T value = uniqueValue(ref);

      if (value != null && lit.comparator().compare(value, lit.value()) == 0) {
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean in(BoundReference<T> ref, Set<T> literalSet) {
      int pos = Accessors.toPosition(ref.accessor());
      PartitionFieldSummary fieldStats = stats.get(pos);
      if (fieldStats.lowerBound() == null) {
        return ROWS_CANNOT_MATCH; // values are all null and literalSet cannot contain null.
      }

      Collection<T> literals = literalSet;

      if (literals.size() > IN_PREDICATE_LIMIT) {
        // skip evaluating the predicate if the number of values is too big
        return ROWS_MIGHT_MATCH;
      }

      Comparator<T> cmp = comparatorForIn(ref);
      T lower = Conversions.fromByteBuffer(ref.type(), fieldStats.lowerBound());
      literals =
          literals.stream().filter(v -> cmp.compare(lower, v) <= 0).collect(Collectors.toList());
      if (literals.isEmpty()) { // if all values are less than lower bound, rows cannot match.
        return ROWS_CANNOT_MATCH;
      }

      T upper = Conversions.fromByteBuffer(ref.type(), fieldStats.upperBound());
      literals =
          literals.stream().filter(v -> cmp.compare(upper, v) >= 0).collect(Collectors.toList());
      if (literals
          .isEmpty()) { // if all remaining values are greater than upper bound, rows cannot match.
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean notIn(BoundReference<T> ref, Set<T> literalSet) {
      // because the bounds are not necessarily a min or max value, this cannot be answered using
      // them. notIn(col, {X, ...}) with (X, Y) doesn't guarantee that X is a value in col.
      // However, when lower == upper and the manifest has no nulls or NaN values, we can safely
      // prune if that value is in the exclusion set.
      T value = uniqueValue(ref);

      if (value != null && literalSet.contains(value)) {
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean startsWith(BoundReference<T> ref, Literal<T> lit) {
      int pos = Accessors.toPosition(ref.accessor());
      PartitionFieldSummary fieldStats = stats.get(pos);

      if (fieldStats.lowerBound() == null) {
        return ROWS_CANNOT_MATCH; // values are all null and literal cannot contain null
      }

      ByteBuffer prefixAsBytes = lit.toByteBuffer();

      Comparator<ByteBuffer> comparator = Comparators.unsignedBytes();

      ByteBuffer lower = fieldStats.lowerBound();
      // truncate lower bound so that its length in bytes is not greater than the length of prefix
      int lowerLength = Math.min(prefixAsBytes.remaining(), lower.remaining());
      int lowerCmp =
          comparator.compare(BinaryUtil.truncateBinary(lower, lowerLength), prefixAsBytes);
      if (lowerCmp > 0) {
        return ROWS_CANNOT_MATCH;
      }

      ByteBuffer upper = fieldStats.upperBound();
      // truncate upper bound so that its length in bytes is not greater than the length of prefix
      int upperLength = Math.min(prefixAsBytes.remaining(), upper.remaining());
      int upperCmp =
          comparator.compare(BinaryUtil.truncateBinary(upper, upperLength), prefixAsBytes);
      if (upperCmp < 0) {
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean notStartsWith(BoundReference<T> ref, Literal<T> lit) {
      int pos = Accessors.toPosition(ref.accessor());
      PartitionFieldSummary fieldStats = stats.get(pos);

      if (fieldStats.containsNull()) {
        return ROWS_MIGHT_MATCH;
      }

      ByteBuffer lower = fieldStats.lowerBound();
      ByteBuffer upper = fieldStats.upperBound();

      // notStartsWith will match unless all values must start with the prefix. This happens when
      // the lower and upper
      // bounds both start with the prefix.
      if (lower != null && upper != null) {
        ByteBuffer prefixAsBytes = lit.toByteBuffer();
        Comparator<ByteBuffer> comparator = Comparators.unsignedBytes();

        // if lower is shorter than the prefix, it can't start with the prefix
        if (lower.remaining() < prefixAsBytes.remaining()) {
          return ROWS_MIGHT_MATCH;
        }

        // truncate lower bound to the prefix and check for equality
        int cmp =
            comparator.compare(
                BinaryUtil.truncateBinary(lower, prefixAsBytes.remaining()), prefixAsBytes);
        if (cmp == 0) {
          // the lower bound starts with the prefix; check the upper bound
          // if upper is shorter than the prefix, it can't start with the prefix
          if (upper.remaining() < prefixAsBytes.remaining()) {
            return ROWS_MIGHT_MATCH;
          }

          // truncate upper bound so that its length in bytes is not greater than the length of
          // prefix
          cmp =
              comparator.compare(
                  BinaryUtil.truncateBinary(upper, prefixAsBytes.remaining()), prefixAsBytes);
          if (cmp == 0) {
            // both bounds match the prefix, so all rows must match the prefix and none do not match
            return ROWS_CANNOT_MATCH;
          }
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    /**
     * Returns the partition field's single value if all partitions contain the same value. Defined
     * as a partition field with no nulls, no NaNs (for floating-point types), and lower bound
     * equals upper bound. Returns null otherwise.
     */
    private <T> T uniqueValue(BoundReference<T> ref) {
      int pos = Accessors.toPosition(ref.accessor());
      PartitionFieldSummary fieldStats = stats.get(pos);

      if (fieldStats.containsNull()) {
        return null;
      }

      Type.TypeID typeId = ref.type().typeId();
      if (Type.TypeID.FLOAT.equals(typeId) || Type.TypeID.DOUBLE.equals(typeId)) {
        if (fieldStats.containsNaN() == null || fieldStats.containsNaN()) {
          return null;
        }
      }

      ByteBuffer lowerBound = fieldStats.lowerBound();
      ByteBuffer upperBound = fieldStats.upperBound();

      if (lowerBound == null || upperBound == null) {
        return null;
      }

      T lower = Conversions.fromByteBuffer(ref.type(), lowerBound);
      T upper = Conversions.fromByteBuffer(ref.type(), upperBound);

      if (ref.comparator().compare(lower, upper) != 0) {
        return null;
      }

      return lower;
    }

    private boolean allValuesAreNull(PartitionFieldSummary summary, Type.TypeID typeId) {
      // containsNull encodes whether at least one partition value is null,
      // lowerBound is null if all partition values are null
      boolean allNull = summary.containsNull() && summary.lowerBound() == null;

      if (allNull && (Type.TypeID.DOUBLE.equals(typeId) || Type.TypeID.FLOAT.equals(typeId))) {
        // floating point types may include NaN values, which we check separately.
        // In case bounds don't include NaN value, containsNaN needs to be checked against.
        allNull = summary.containsNaN() != null && !summary.containsNaN();
      }
      return allNull;
    }
  }
}
