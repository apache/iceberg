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

import org.apache.iceberg.ContentStats;
import org.apache.iceberg.FieldStats;
import org.apache.iceberg.Schema;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantObject;

/**
 * Evaluates an {@link Expression} on the {@link ContentStats} of a file to test whether rows in the
 * file may match.
 *
 * <p>This evaluation is inclusive: it returns true if a file may match and false if it cannot
 * match.
 *
 * <p>Stats are passed to {@link #eval(ContentStats, long)}, which returns true if the file may
 * contain matching rows and false if the file cannot contain matching rows. Files may be skipped if
 * and only if the return value of {@code eval} is false.
 *
 * <p>Due to the comparison implementation of ORC stats, for float/double columns in ORC files, if
 * the first value in a file is NaN, metrics of this file will report NaN for both upper and lower
 * bound despite that the column could contain non-NaN data. Thus, in some scenarios explicitly
 * checks for NaN is necessary in order to not skip files that may contain matching data.
 */
public class InclusiveStatsEvaluator {
  private final Expression expr;

  public InclusiveStatsEvaluator(Schema schema, Expression unbound) {
    this(schema, unbound, true);
  }

  public InclusiveStatsEvaluator(Schema schema, Expression unbound, boolean caseSensitive) {
    this.expr = Binder.bind(schema.asStruct(), rewriteNot(unbound), caseSensitive);
  }

  /**
   * Test whether a file may contain records that match the expression.
   *
   * @param stats the content stats of the file, or null if the file tracks no stats
   * @param recordCount the number of records in the file
   * @return false if the file cannot contain rows that match the expression, true otherwise.
   */
  public boolean eval(ContentStats stats, long recordCount) {
    return new StatsEvalVisitor().eval(stats, recordCount);
  }

  private class StatsEvalVisitor extends InclusiveEvalVisitor {
    private ContentStats stats = null;
    private long recordCount = 0;

    private boolean eval(ContentStats contentStats, long fileRecordCount) {
      if (fileRecordCount == 0) {
        return ROWS_CANNOT_MATCH;
      }

      if (fileRecordCount < 0) {
        // imported Avro files may have an incorrect -1 row count
        return ROWS_MIGHT_MATCH;
      }

      if (null == contentStats) {
        return ROWS_MIGHT_MATCH;
      }

      this.stats = contentStats;
      this.recordCount = fileRecordCount;

      return ExpressionVisitors.visitEvaluator(expr, this);
    }

    @Override
    protected boolean mayContainNull(int id) {
      FieldStats<?> fieldStats = stats.statsFor(id);
      return fieldStats == null
          // rows where a struct that contains this field is null are not counted in the value
          // count, so the field is null in every row that the value count does not cover
          || (fieldStats.hasValueCount() && fieldStats.valueCount() < recordCount)
          || !fieldStats.hasNullValueCount()
          || fieldStats.nullValueCount() != 0;
    }

    @Override
    protected boolean containsNullsOnly(int id) {
      FieldStats<?> fieldStats = stats.statsFor(id);
      return fieldStats != null
          && fieldStats.hasValueCount()
          && fieldStats.hasNullValueCount()
          && fieldStats.valueCount() - fieldStats.nullValueCount() == 0;
    }

    @Override
    protected boolean mayContainNaN(int id) {
      FieldStats<?> fieldStats = stats.statsFor(id);
      return fieldStats == null
          || !fieldStats.hasNanValueCount()
          || fieldStats.nanValueCount() != 0;
    }

    @Override
    protected boolean containsNaNsOnly(int id) {
      FieldStats<?> fieldStats = stats.statsFor(id);
      return fieldStats != null
          && fieldStats.hasValueCount()
          && fieldStats.hasNanValueCount()
          && fieldStats.valueCount() == fieldStats.nanValueCount();
    }

    @Override
    protected <T> T lowerBound(BoundReference<T> ref) {
      FieldStats<T> fieldStats = stats.statsFor(ref.fieldId());
      return fieldStats != null ? fieldStats.lowerBound() : null;
    }

    @Override
    protected <T> T upperBound(BoundReference<T> ref) {
      FieldStats<T> fieldStats = stats.statsFor(ref.fieldId());
      return fieldStats != null ? fieldStats.upperBound() : null;
    }

    @Override
    protected <T> T extractLowerBound(BoundExtract<T> bound) {
      FieldStats<Variant> fieldStats = stats.statsFor(bound.ref().fieldId());
      if (fieldStats != null && fieldStats.lowerBound() != null) {
        VariantObject fieldLowerBounds = fieldStats.lowerBound().value().asObject();
        return VariantExpressionUtil.castTo(fieldLowerBounds.get(bound.path()), bound.type());
      }

      return null;
    }

    @Override
    protected <T> T extractUpperBound(BoundExtract<T> bound) {
      FieldStats<Variant> fieldStats = stats.statsFor(bound.ref().fieldId());
      if (fieldStats != null && fieldStats.upperBound() != null) {
        VariantObject fieldUpperBounds = fieldStats.upperBound().value().asObject();
        return VariantExpressionUtil.castTo(fieldUpperBounds.get(bound.path()), bound.type());
      }

      return null;
    }
  }
}
