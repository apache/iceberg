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

import java.util.Collections;
import java.util.Set;
import org.apache.iceberg.ContentStats;
import org.apache.iceberg.FieldStats;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TrackedFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantObject;

/**
 * Evaluates an {@link Expression} on a {@link TrackedFile} to test whether rows in the file may
 * match.
 *
 * <p>This evaluation is inclusive: it returns true if a file may match and false if it cannot
 * match.
 *
 * <p>Files are passed to {@link #eval(TrackedFile)}, which returns true if the file may contain
 * matching rows and false if the file cannot contain matching rows. Files may be skipped if and
 * only if the return value of {@code eval} is false.
 *
 * <p>Due to the comparison implementation of ORC stats, for float/double columns in ORC files, if
 * the first value in a file is NaN, metrics of this file will report NaN for both upper and lower
 * bound despite that the column could contain non-NaN data. Thus, in some scenarios explicitly
 * checks for NaN is necessary in order to not skip files that may contain matching data.
 */
public class InclusiveStatsEvaluator {
  private final Expression expr;
  private final Set<Integer> neverNullIds;

  public InclusiveStatsEvaluator(Schema schema, Expression unbound) {
    this(schema, unbound, true);
  }

  public InclusiveStatsEvaluator(Schema schema, Expression unbound, boolean caseSensitive) {
    Types.StructType struct = schema.asStruct();
    this.expr = Binder.bind(struct, rewriteNot(unbound), caseSensitive);
    this.neverNullIds =
        neverNullIds(
            struct, Binder.boundReferences(struct, Collections.singletonList(expr), caseSensitive));
  }

  /**
   * Returns the IDs of the referenced fields that cannot contain null values.
   *
   * <p>Stats omit the null count for every required field. The count is zero for a field that
   * cannot be null, but it is unknown for a required field that an optional struct contains because
   * that field is null whenever the struct is null.
   */
  private static Set<Integer> neverNullIds(Types.StructType struct, Set<Integer> referencedIds) {
    ImmutableSet.Builder<Integer> neverNull = ImmutableSet.builder();

    for (int id : referencedIds) {
      if (TypeUtil.alwaysPresent(struct, id)) {
        neverNull.add(id);
      }
    }

    return neverNull.build();
  }

  /**
   * Test whether the file may contain records that match the expression.
   *
   * @param file a tracked file
   * @return false if the file cannot contain rows that match the expression, true otherwise.
   */
  public boolean eval(TrackedFile file) {
    return new StatsEvalVisitor().eval(file);
  }

  private class StatsEvalVisitor extends InclusiveEvalVisitor {
    private ContentStats stats = null;

    private boolean eval(TrackedFile file) {
      if (file.recordCount() == 0) {
        return ROWS_CANNOT_MATCH;
      }

      if (file.recordCount() < 0) {
        // imported Avro files may have an incorrect -1 row count
        return ROWS_MIGHT_MATCH;
      }

      ContentStats contentStats = file.contentStats();
      if (null == contentStats) {
        return ROWS_MIGHT_MATCH;
      }

      this.stats = contentStats;

      return ExpressionVisitors.visitEvaluator(expr, this);
    }

    @Override
    protected boolean mayContainNull(int id) {
      if (neverNullIds.contains(id)) {
        return false;
      }

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
