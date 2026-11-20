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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluates an {@link Expression} on a {@link DataFile} to test whether all rows in the file match.
 *
 * <p>This evaluation is strict: it returns true if all rows in a file must match the expression.
 * For example, if a file's ts column has min X and max Y, this evaluator will return true for ts
 * &lt; Y+1 but not for ts &lt; Y-1.
 *
 * <p>Files are passed to {@link #eval(ContentFile)}, which returns true if all rows in the file
 * must contain matching rows and false if the file may contain rows that do not match.
 *
 * <p>Due to the comparison implementation of ORC stats, for float/double columns in ORC files, if
 * the first value in a file is NaN, metrics of this file will report NaN for both upper and lower
 * bound despite that the column could contain non-NaN data. Thus in some scenarios explicitly
 * checks for NaN is necessary in order to not include files that may contain rows that don't match.
 */
public class StrictMetricsEvaluator {
  private static final Logger LOG = LoggerFactory.getLogger(StrictMetricsEvaluator.class);

  private final StructType struct;
  private final Expression expr;

  public StrictMetricsEvaluator(Schema schema, Expression unbound) {
    this(schema, unbound, true);
  }

  public StrictMetricsEvaluator(Schema schema, Expression unbound, boolean caseSensitive) {
    this.struct = schema.asStruct();
    this.expr = Binder.bind(struct, rewriteNot(unbound), caseSensitive);
  }

  /**
   * Test whether all records within the file match the expression.
   *
   * @param file a data file
   * @return false if the file may contain any row that doesn't match the expression, true
   *     otherwise.
   */
  public boolean eval(ContentFile<?> file) {
    // TODO: detect the case where a column is missing from the file using file's max field id.
    return new MetricsEvalVisitor(struct).eval(file);
  }

  private class MetricsEvalVisitor extends StrictEvalVisitor {
    private Map<Integer, Long> valueCounts = null;
    private Map<Integer, Long> nullCounts = null;
    private Map<Integer, Long> nanCounts = null;
    private Map<Integer, ByteBuffer> lowerBounds = null;
    private Map<Integer, ByteBuffer> upperBounds = null;

    private MetricsEvalVisitor(StructType struct) {
      super(struct);
    }

    private static List<Integer> equalityFieldIds(ContentFile<?> file) {
      List<Integer> equalityFieldIds = file.equalityFieldIds();
      if (equalityFieldIds == null || equalityFieldIds.isEmpty()) {
        LOG.warn(
            "Ignoring stats of equality delete file without equality field IDs: {}",
            file.location());
        return Collections.emptyList();
      }

      return equalityFieldIds;
    }

    private static <V> Map<Integer, V> statsForFields(
        Map<Integer, V> stats, List<Integer> fieldIds) {
      return stats != null ? Maps.filterKeys(stats, fieldIds::contains) : null;
    }

    private boolean eval(ContentFile<?> file) {
      if (file.recordCount() <= 0) {
        return ROWS_MUST_MATCH;
      }

      if (file.content() == FileContent.EQUALITY_DELETES) {
        // stats of columns outside the equality fields don't describe which rows the file deletes
        List<Integer> fieldIds = equalityFieldIds(file);
        this.valueCounts = statsForFields(file.valueCounts(), fieldIds);
        this.nullCounts = statsForFields(file.nullValueCounts(), fieldIds);
        this.nanCounts = statsForFields(file.nanValueCounts(), fieldIds);
        this.lowerBounds = statsForFields(file.lowerBounds(), fieldIds);
        this.upperBounds = statsForFields(file.upperBounds(), fieldIds);
      } else {
        this.valueCounts = file.valueCounts();
        this.nullCounts = file.nullValueCounts();
        this.nanCounts = file.nanValueCounts();
        this.lowerBounds = file.lowerBounds();
        this.upperBounds = file.upperBounds();
      }

      return ExpressionVisitors.visitEvaluator(expr, this);
    }

    @Override
    protected boolean mayContainNull(int id) {
      return nullCounts == null || !nullCounts.containsKey(id) || nullCounts.get(id) != 0;
    }

    @Override
    protected boolean containsNullsOnly(int id) {
      return valueCounts != null
          && valueCounts.containsKey(id)
          && nullCounts != null
          && nullCounts.containsKey(id)
          && valueCounts.get(id) - nullCounts.get(id) == 0;
    }

    @Override
    protected boolean canContainNulls(int id) {
      return nullCounts == null || (nullCounts.containsKey(id) && nullCounts.get(id) > 0);
    }

    @Override
    protected boolean mayContainNaN(int id) {
      return nanCounts == null || !nanCounts.containsKey(id) || nanCounts.get(id) != 0;
    }

    @Override
    protected boolean containsNaNsOnly(int id) {
      return nanCounts != null
          && nanCounts.containsKey(id)
          && valueCounts != null
          && nanCounts.get(id).equals(valueCounts.get(id));
    }

    @Override
    protected boolean canContainNaNs(int id) {
      return nanCounts != null && nanCounts.containsKey(id) && nanCounts.get(id) > 0;
    }

    @Override
    protected <T> T lowerBound(BoundReference<T> ref) {
      int id = ref.fieldId();
      if (lowerBounds != null && lowerBounds.containsKey(id)) {
        return Conversions.fromByteBuffer(ref.type(), lowerBounds.get(id));
      }

      return null;
    }

    @Override
    protected <T> T upperBound(BoundReference<T> ref) {
      int id = ref.fieldId();
      if (upperBounds != null && upperBounds.containsKey(id)) {
        return Conversions.fromByteBuffer(ref.type(), upperBounds.get(id));
      }

      return null;
    }
  }
}
