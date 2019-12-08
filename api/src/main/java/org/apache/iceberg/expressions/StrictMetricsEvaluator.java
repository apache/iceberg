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

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.ExpressionVisitors.BoundExpressionVisitor;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;

import static org.apache.iceberg.expressions.Expressions.rewriteNot;

/**
 * Evaluates an {@link Expression} on a {@link DataFile} to test whether all rows in the file match.
 * <p>
 * This evaluation is strict: it returns true if all rows in a file must match the expression. For
 * example, if a file's ts column has min X and max Y, this evaluator will return true for ts &lt; Y+1
 * but not for ts &lt; Y-1.
 * <p>
 * Files are passed to {@link #eval(DataFile)}, which returns true if all rows in the file must
 * contain matching rows and false if the file may contain rows that do not match.
 */
public class StrictMetricsEvaluator {
  private final Schema schema;
  private final StructType struct;
  private final Expression expr;
  private transient ThreadLocal<MetricsEvalVisitor> visitors = null;

  private MetricsEvalVisitor visitor() {
    if (visitors == null) {
      this.visitors = ThreadLocal.withInitial(MetricsEvalVisitor::new);
    }
    return visitors.get();
  }

  public StrictMetricsEvaluator(Schema schema, Expression unbound) {
    this.schema = schema;
    this.struct = schema.asStruct();
    this.expr = Binder.bind(struct, rewriteNot(unbound), true);
  }

  /**
   * Test whether the file may contain records that match the expression.
   *
   * @param file a data file
   * @return false if the file cannot contain rows that match the expression, true otherwise.
   */
  public boolean eval(DataFile file) {
    // TODO: detect the case where a column is missing from the file using file's max field id.
    return visitor().eval(file);
  }

  private static final boolean ROWS_MUST_MATCH = true;
  private static final boolean ROWS_MIGHT_NOT_MATCH = false;

  private class MetricsEvalVisitor extends BoundExpressionVisitor<Boolean> {
    private Map<Integer, Long> valueCounts = null;
    private Map<Integer, Long> nullCounts = null;
    private Map<Integer, ByteBuffer> lowerBounds = null;
    private Map<Integer, ByteBuffer> upperBounds = null;

    private boolean eval(DataFile file) {
      if (file.recordCount() <= 0) {
        return ROWS_MUST_MATCH;
      }

      this.valueCounts = file.valueCounts();
      this.nullCounts = file.nullValueCounts();
      this.lowerBounds = file.lowerBounds();
      this.upperBounds = file.upperBounds();

      return ExpressionVisitors.visitEvaluator(expr, this);
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
      Integer id = ref.fieldId();
      Preconditions.checkNotNull(struct.field(id),
          "Cannot filter by nested column: %s", schema.findField(id));

      if (containsNullsOnly(id)) {
        return ROWS_MUST_MATCH;
      }

      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean notNull(BoundReference<T> ref) {
      // no need to check whether the field is required because binding evaluates that case
      // if the column has any null values, the expression does not match
      Integer id = ref.fieldId();
      Preconditions.checkNotNull(struct.field(id),
          "Cannot filter by nested column: %s", schema.findField(id));

      if (nullCounts != null && nullCounts.containsKey(id) && nullCounts.get(id) == 0) {
        return ROWS_MUST_MATCH;
      }

      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean lt(BoundReference<T> ref, Literal<T> lit) {
      // Rows must match when: <----------Min----Max---X------->
      Integer id = ref.fieldId();
      Types.NestedField field = struct.field(id);
      Preconditions.checkNotNull(field, "Cannot filter by nested column: %s", schema.findField(id));

      if (canContainNulls(id)) {
        return ROWS_MIGHT_NOT_MATCH;
      }

      if (upperBounds != null && upperBounds.containsKey(id)) {
        T upper = Conversions.fromByteBuffer(field.type(), upperBounds.get(id));

        int cmp = lit.comparator().compare(upper, lit.value());
        if (cmp < 0) {
          return ROWS_MUST_MATCH;
        }
      }

      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean ltEq(BoundReference<T> ref, Literal<T> lit) {
      // Rows must match when: <----------Min----Max---X------->
      Integer id = ref.fieldId();
      Types.NestedField field = struct.field(id);
      Preconditions.checkNotNull(field, "Cannot filter by nested column: %s", schema.findField(id));

      if (canContainNulls(id)) {
        return ROWS_MIGHT_NOT_MATCH;
      }

      if (upperBounds != null && upperBounds.containsKey(id)) {
        T upper = Conversions.fromByteBuffer(field.type(), upperBounds.get(id));

        int cmp = lit.comparator().compare(upper, lit.value());
        if (cmp <= 0) {
          return ROWS_MUST_MATCH;
        }
      }

      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean gt(BoundReference<T> ref, Literal<T> lit) {
      // Rows must match when: <-------X---Min----Max---------->
      Integer id = ref.fieldId();
      Types.NestedField field = struct.field(id);
      Preconditions.checkNotNull(field, "Cannot filter by nested column: %s", schema.findField(id));

      if (canContainNulls(id)) {
        return ROWS_MIGHT_NOT_MATCH;
      }

      if (lowerBounds != null && lowerBounds.containsKey(id)) {
        T lower = Conversions.fromByteBuffer(field.type(), lowerBounds.get(id));

        int cmp = lit.comparator().compare(lower, lit.value());
        if (cmp > 0) {
          return ROWS_MUST_MATCH;
        }
      }

      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean gtEq(BoundReference<T> ref, Literal<T> lit) {
      // Rows must match when: <-------X---Min----Max---------->
      Integer id = ref.fieldId();
      Types.NestedField field = struct.field(id);
      Preconditions.checkNotNull(field, "Cannot filter by nested column: %s", schema.findField(id));

      if (canContainNulls(id)) {
        return ROWS_MIGHT_NOT_MATCH;
      }

      if (lowerBounds != null && lowerBounds.containsKey(id)) {
        T lower = Conversions.fromByteBuffer(field.type(), lowerBounds.get(id));

        int cmp = lit.comparator().compare(lower, lit.value());
        if (cmp >= 0) {
          return ROWS_MUST_MATCH;
        }
      }

      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean eq(BoundReference<T> ref, Literal<T> lit) {
      // Rows must match when Min == X == Max
      Integer id = ref.fieldId();
      Types.NestedField field = struct.field(id);
      Preconditions.checkNotNull(field, "Cannot filter by nested column: %s", schema.findField(id));

      if (canContainNulls(id)) {
        return ROWS_MIGHT_NOT_MATCH;
      }

      if (lowerBounds != null && lowerBounds.containsKey(id) &&
          upperBounds != null && upperBounds.containsKey(id)) {
        T lower = Conversions.fromByteBuffer(struct.field(id).type(), lowerBounds.get(id));

        int cmp = lit.comparator().compare(lower, lit.value());
        if (cmp != 0) {
          return ROWS_MIGHT_NOT_MATCH;
        }

        T upper = Conversions.fromByteBuffer(field.type(), upperBounds.get(id));

        cmp = lit.comparator().compare(upper, lit.value());
        if (cmp != 0) {
          return ROWS_MIGHT_NOT_MATCH;
        }

        return ROWS_MUST_MATCH;
      }

      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean notEq(BoundReference<T> ref, Literal<T> lit) {
      // Rows must match when X < Min or Max < X because it is not in the range
      Integer id = ref.fieldId();
      Types.NestedField field = struct.field(id);
      Preconditions.checkNotNull(field, "Cannot filter by nested column: %s", schema.findField(id));

      if (containsNullsOnly(id)) {
        return ROWS_MUST_MATCH;
      }

      if (lowerBounds != null && lowerBounds.containsKey(id)) {
        T lower = Conversions.fromByteBuffer(struct.field(id).type(), lowerBounds.get(id));

        int cmp = lit.comparator().compare(lower, lit.value());
        if (cmp > 0) {
          return ROWS_MUST_MATCH;
        }
      }

      if (upperBounds != null && upperBounds.containsKey(id)) {
        T upper = Conversions.fromByteBuffer(field.type(), upperBounds.get(id));

        int cmp = lit.comparator().compare(upper, lit.value());
        if (cmp < 0) {
          return ROWS_MUST_MATCH;
        }
      }

      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean in(BoundReference<T> ref, Set<T> literalSet) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean notIn(BoundReference<T> ref, Set<T> literalSet) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    @Override
    public <T> Boolean startsWith(BoundReference<T> ref, Literal<T> lit) {
      return ROWS_MIGHT_NOT_MATCH;
    }

    private boolean canContainNulls(Integer id) {
      return nullCounts == null || nullCounts.containsKey(id) && nullCounts.get(id) > 0;
    }

    private boolean containsNullsOnly(Integer id) {
      return valueCounts != null && valueCounts.containsKey(id) &&
          nullCounts != null && nullCounts.containsKey(id) &&
          valueCounts.get(id) - nullCounts.get(id) == 0;
    }
  }
}
