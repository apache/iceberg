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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types.StructType;
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
  private final Expression expr;
  private final Map<Set<?>, List<PartitionLiteralIndex>> partitionedLiterals;

  public InclusiveMetricsEvaluator(Schema schema, Expression unbound) {
    this(schema, null, unbound, true);
  }

  public InclusiveMetricsEvaluator(Schema schema, Expression unbound, boolean caseSensitive) {
    this(schema, null, unbound, caseSensitive);
  }

  public static InclusiveMetricsEvaluator forRowFilter(
      Expression rowFilter, PartitionSpec spec, boolean caseSensitive) {
    return new InclusiveMetricsEvaluator(spec.schema(), spec, rowFilter, caseSensitive);
  }

  private InclusiveMetricsEvaluator(
      Schema schema, PartitionSpec spec, Expression unbound, boolean caseSensitive) {
    StructType struct = schema.asStruct();
    this.expr = Binder.bind(struct, rewriteNot(unbound), caseSensitive);
    this.partitionedLiterals =
        spec != null ? indexPartitionedLiterals(spec, expr) : Collections.emptyMap();
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

  private class MetricsEvalVisitor extends InclusiveEvalVisitor {
    private Map<Integer, Long> valueCounts = null;
    private Map<Integer, Long> nullCounts = null;
    private Map<Integer, Long> nanCounts = null;
    private Map<Integer, ByteBuffer> lowerBounds = null;
    private Map<Integer, ByteBuffer> upperBounds = null;
    private StructLike partition = null;

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
      this.partition = file.partition();

      return ExpressionVisitors.visitEvaluator(expr, this);
    }

    @Override
    protected <T> Collection<T> literals(Bound<T> term, Set<T> literalSet) {
      List<PartitionLiteralIndex> indexes = partitionedLiterals.get(literalSet);
      if (indexes == null) {
        return literalSet;
      }

      Set<T> matching = null;
      for (PartitionLiteralIndex index : indexes) {
        Set<T> values = index.literals(partition);
        if (values == null) {
          return Collections.emptySet();
        } else if (matching == null) {
          matching = values;
        } else {
          matching = new HashSet<>(matching);
          matching.retainAll(values);
          if (matching.isEmpty()) {
            return matching;
          }
        }
      }

      return matching != null ? matching : literalSet;
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
    protected <T> T lowerBound(BoundReference<T> ref) {
      int id = ref.fieldId();
      if (lowerBounds != null && lowerBounds.containsKey(id)) {
        return Conversions.fromByteBuffer(ref.ref().type(), lowerBounds.get(id));
      }

      return null;
    }

    @Override
    protected <T> T upperBound(BoundReference<T> ref) {
      int id = ref.fieldId();
      if (upperBounds != null && upperBounds.containsKey(id)) {
        return Conversions.fromByteBuffer(ref.ref().type(), upperBounds.get(id));
      }

      return null;
    }

    @Override
    protected <T> T extractLowerBound(BoundExtract<T> bound) {
      int id = bound.ref().fieldId();
      if (lowerBounds != null && lowerBounds.containsKey(id)) {
        VariantObject fieldLowerBounds = parseBounds(lowerBounds.get(id));
        return VariantExpressionUtil.castTo(fieldLowerBounds.get(bound.path()), bound.type());
      }

      return null;
    }

    @Override
    protected <T> T extractUpperBound(BoundExtract<T> bound) {
      int id = bound.ref().fieldId();
      if (upperBounds != null && upperBounds.containsKey(id)) {
        VariantObject fieldUpperBounds = parseBounds(upperBounds.get(id));
        return VariantExpressionUtil.castTo(fieldUpperBounds.get(bound.path()), bound.type());
      }

      return null;
    }
  }

  @SuppressWarnings({"unchecked", "IdentityHashMapUsage"})
  private static Map<Set<?>, List<PartitionLiteralIndex>> indexPartitionedLiterals(
      PartitionSpec spec, Expression expression) {
    Map<Set<?>, List<PartitionLiteralIndex>> indexes = new IdentityHashMap<>();
    ExpressionVisitors.visit(
        expression,
        new ExpressionVisitors.ExpressionVisitor<Void>() {
          @Override
          public <T> Void predicate(BoundPredicate<T> predicate) {
            if (!predicate.isSetPredicate()
                || predicate.op() != Expression.Operation.IN
                || !(predicate.term() instanceof BoundReference)) {
              return null;
            }

            Set<T> literals = predicate.asSetPredicate().literalSet();
            List<PartitionField> fields = spec.getFieldsBySourceId(predicate.ref().fieldId());
            if (fields == null) {
              return null;
            }

            for (PartitionField field : fields) {
              Transform<T, ?> transform = (Transform<T, ?>) field.transform();
              if (transform.preservesOrder()
                  || transform.project(field.name(), predicate) == null) {
                continue;
              }

              Map<Object, Set<Object>> literalsByValue = new HashMap<>();
              Function<T, ?> function = transform.bind(predicate.term().type());
              for (T literal : literals) {
                literalsByValue
                    .computeIfAbsent(function.apply(literal), ignored -> new HashSet<>())
                    .add(literal);
              }

              int position = spec.fields().indexOf(field);
              indexes
                  .computeIfAbsent(literals, ignored -> new ArrayList<>())
                  .add(
                      new PartitionLiteralIndex(
                          position, spec.javaClasses()[position], literalsByValue));
            }
            return null;
          }
        });
    return indexes;
  }

  private static class PartitionLiteralIndex {
    private final int position;
    private final Class<?> javaClass;
    private final Map<Object, Set<Object>> literalsByValue;

    private PartitionLiteralIndex(
        int position, Class<?> javaClass, Map<Object, Set<Object>> literalsByValue) {
      this.position = position;
      this.javaClass = javaClass;
      this.literalsByValue = literalsByValue;
    }

    @SuppressWarnings("unchecked")
    private <T> Set<T> literals(StructLike partition) {
      Object partitionValue = partition.get(position, (Class<Object>) javaClass);
      return (Set<T>) (Set<?>) literalsByValue.get(partitionValue);
    }
  }

  private static VariantObject parseBounds(ByteBuffer buffer) {
    return Variant.from(buffer).value().asObject();
  }
}
