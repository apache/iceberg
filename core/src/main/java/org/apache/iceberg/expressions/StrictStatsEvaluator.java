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
import org.apache.iceberg.types.Types;

/**
 * Evaluates an {@link Expression} on the {@link ContentStats} of a file to test whether all rows in
 * the file match.
 *
 * <p>This evaluation is strict: it returns true if all rows in a file must match the expression.
 * For example, if a file's ts column has min X and max Y, this evaluator will return true for ts
 * &lt; Y+1 but not for ts &lt; Y-1.
 *
 * <p>Stats are passed to {@link #eval(ContentStats, long)}, which returns true if all rows in the
 * file must contain matching rows and false if the file may contain rows that do not match.
 *
 * <p>Due to the comparison implementation of ORC stats, for float/double columns in ORC files, if
 * the first value in a file is NaN, metrics of this file will report NaN for both upper and lower
 * bound despite that the column could contain non-NaN data. Thus in some scenarios explicitly
 * checks for NaN is necessary in order to not include files that may contain rows that don't match.
 */
public class StrictStatsEvaluator {
  private final Types.StructType struct;
  private final Expression expr;

  public StrictStatsEvaluator(Schema schema, Expression unbound) {
    this(schema, unbound, true);
  }

  public StrictStatsEvaluator(Schema schema, Expression unbound, boolean caseSensitive) {
    this.struct = schema.asStruct();
    this.expr = Binder.bind(struct, rewriteNot(unbound), caseSensitive);
  }

  /**
   * Test whether all records within the file match the expression.
   *
   * @param stats the content stats of the file, or null if the file tracks no stats
   * @param recordCount the number of records in the file
   * @return false if the file may contain any row that doesn't match the expression, true
   *     otherwise.
   */
  public boolean eval(ContentStats stats, long recordCount) {
    return new StatsEvalVisitor(struct).eval(stats, recordCount);
  }

  private class StatsEvalVisitor extends StrictEvalVisitor {
    private ContentStats stats = null;

    private StatsEvalVisitor(Types.StructType struct) {
      super(struct);
    }

    private boolean eval(ContentStats contentStats, long fileRecordCount) {
      if (fileRecordCount == 0) {
        return ROWS_MUST_MATCH;
      }

      if (fileRecordCount < 0) {
        // imported Avro files may have an incorrect -1 row count
        return ROWS_MIGHT_NOT_MATCH;
      }

      if (null == contentStats) {
        return ROWS_MIGHT_NOT_MATCH;
      }

      this.stats = contentStats;

      return ExpressionVisitors.visitEvaluator(expr, this);
    }

    @Override
    protected boolean mayContainNull(int id) {
      FieldStats<?> fieldStats = stats.statsFor(id);
      return fieldStats == null
          || !fieldStats.hasNullValueCount()
          || fieldStats.nullValueCount() != 0;
    }

    @Override
    protected boolean containsNullsOnly(int id) {
      FieldStats<?> fieldStats = stats.statsFor(id);
      return fieldStats != null
          && fieldStats.hasValueCount()
          && fieldStats.hasNullValueCount()
          && fieldStats.valueCount() == fieldStats.nullValueCount();
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
  }
}
