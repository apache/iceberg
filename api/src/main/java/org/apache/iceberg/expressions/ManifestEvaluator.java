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

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.Accessors;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFile.PartitionFieldSummary;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.expressions.ExpressionVisitors.BoundExpressionVisitor;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.BinaryUtil;

import static org.apache.iceberg.expressions.Expressions.rewriteNot;

/**
 * Evaluates an {@link Expression} on a {@link ManifestFile} to test whether the file contains
 * matching partitions.
 * <p>
 * For row expressions, evaluation is inclusive: it returns true if a file may match and false if it cannot match.
 * <p>
 * Files are passed to {@link #eval(ManifestFile)}, which returns true if the manifest may contain
 * data files that match the partition expression. Manifest files may be skipped if and only if the
 * return value of {@code eval} is false.
 */
public class ManifestEvaluator {
  private final StructType struct;
  private final Expression expr;
  private transient ThreadLocal<ManifestEvalVisitor> visitors = null;

  private ManifestEvalVisitor visitor() {
    if (visitors == null) {
      this.visitors = ThreadLocal.withInitial(ManifestEvalVisitor::new);
    }
    return visitors.get();
  }

  public static ManifestEvaluator forRowFilter(Expression rowFilter, PartitionSpec spec, boolean caseSensitive) {
    return new ManifestEvaluator(spec, Projections.inclusive(spec, caseSensitive).project(rowFilter), caseSensitive);
  }

  public static ManifestEvaluator forPartitionFilter(
      Expression partitionFilter, PartitionSpec spec, boolean caseSensitive) {
    return new ManifestEvaluator(spec, partitionFilter, caseSensitive);
  }

  private ManifestEvaluator(PartitionSpec spec, Expression partitionFilter, boolean caseSensitive) {
    this.struct = spec.partitionType();
    this.expr = Binder.bind(struct, rewriteNot(partitionFilter), caseSensitive);
  }

  /**
   * Test whether the file may contain records that match the expression.
   *
   * @param manifest a manifest file
   * @return false if the file cannot contain rows that match the expression, true otherwise.
   */
  public boolean eval(ManifestFile manifest) {
    return visitor().eval(manifest);
  }

  private static final boolean ROWS_MIGHT_MATCH = true;
  private static final boolean ROWS_CANNOT_MATCH = false;

  private class ManifestEvalVisitor extends BoundExpressionVisitor<Boolean> {
    private List<PartitionFieldSummary> stats = null;

    private boolean eval(ManifestFile manifest) {
      this.stats = manifest.partitions();
      if (stats == null) {
        return ROWS_MIGHT_MATCH;
      }

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
      // containsNull encodes whether at least one partition value is null, lowerBound is null if
      // all partition values are null.
      ByteBuffer lowerBound = stats.get(pos).lowerBound();
      if (lowerBound == null) {
        return ROWS_CANNOT_MATCH; // all values are null
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
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean in(BoundReference<T> ref, Literal<T> lit) {
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean notIn(BoundReference<T> ref, Literal<T> lit) {
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
      int lowerCmp = comparator.compare(BinaryUtil.truncateBinary(lower, lowerLength), prefixAsBytes);
      if (lowerCmp > 0) {
        return ROWS_CANNOT_MATCH;
      }

      ByteBuffer upper = fieldStats.upperBound();
      // truncate upper bound so that its length in bytes is not greater than the length of prefix
      int upperLength = Math.min(prefixAsBytes.remaining(), upper.remaining());
      int upperCmp = comparator.compare(BinaryUtil.truncateBinary(upper, upperLength), prefixAsBytes);
      if (upperCmp < 0) {
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }
  }
}
