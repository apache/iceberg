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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.ExpressionVisitors.ExpressionVisitor;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.iceberg.transforms.Transform;

/**
 * Groups the literals of each bound {@code IN} predicate by the partition value they map to.
 *
 * <p>For a predicate on a column with a {@code bucket} partition field, a file can only contain
 * literals that map to the file's bucket value. {@link #literalsFor(Set, StructLike)} returns that
 * subset so an evaluator compares a file's metrics against only those literals. Other transforms
 * are not indexed: order-preserving ones, such as {@code identity} and {@code truncate}, already
 * bound the matching literals through file metrics.
 *
 * <p>Instances are built once per bound expression and partition spec, and queried once per file.
 */
class PartitionedInLiterals {
  private static final PartitionedInLiterals EMPTY =
      new PartitionedInLiterals(Collections.emptyMap());

  // Keyed by identity: each bound IN predicate holds one literal set instance, which the visitor
  // passes back to literals(), and two predicates with equal sets may reference different columns.
  private final Map<Set<?>, Index> indexes;

  private PartitionedInLiterals(Map<Set<?>, Index> indexes) {
    this.indexes = indexes;
  }

  /**
   * Index the {@code IN} predicates of a bound expression against a partition spec.
   *
   * @param boundExpr a bound expression
   * @param spec the partition spec of the files that will be evaluated
   * @return an index of IN literals by partition value
   */
  @SuppressWarnings("IdentityHashMapUsage")
  static PartitionedInLiterals build(Expression boundExpr, PartitionSpec spec) {
    if (spec.isUnpartitioned()) {
      return EMPTY;
    }

    Map<Set<?>, Index> indexes = new IdentityHashMap<>();
    ExpressionVisitors.visit(
        boundExpr,
        new ExpressionVisitor<Void>() {
          @Override
          public <T> Void predicate(BoundPredicate<T> predicate) {
            Index index = index(predicate, spec);
            if (index != null) {
              indexes.put(predicate.asSetPredicate().literalSet(), index);
            }
            return null;
          }
        });

    return indexes.isEmpty() ? EMPTY : new PartitionedInLiterals(indexes);
  }

  /**
   * Return the literals of an {@code IN} predicate that may match a file with the given partition.
   *
   * @param literalSet the literal set of a bound IN predicate in the indexed expression
   * @param partition the partition of the file being evaluated
   * @return the literals that map to the file's partition value, or the full set if the predicate
   *     is not indexed
   */
  <T> Collection<T> literalsFor(Set<T> literalSet, StructLike partition) {
    Index index = indexes.get(literalSet);
    return index != null ? index.literals(partition) : literalSet;
  }

  @SuppressWarnings("unchecked")
  private static <T> Index index(BoundPredicate<T> predicate, PartitionSpec spec) {
    if (!predicate.isSetPredicate()
        || predicate.op() != Expression.Operation.IN
        || !(predicate.term() instanceof BoundReference)) {
      return null;
    }

    List<PartitionField> fields = spec.getFieldsBySourceId(predicate.ref().fieldId());
    if (fields == null) {
      return null;
    }

    for (PartitionField field : fields) {
      if (!PartitionSpecVisitor.visit(spec.schema(), field, IsBucketTransform.INSTANCE)) {
        continue;
      }

      Transform<T, ?> transform = (Transform<T, ?>) field.transform();
      Function<T, ?> function = transform.bind(predicate.term().type());
      Map<Object, Set<Object>> literalsByValue = Maps.newHashMap();
      for (T literal : predicate.asSetPredicate().literalSet()) {
        literalsByValue
            .computeIfAbsent(function.apply(literal), ignored -> Sets.newHashSet())
            .add(literal);
      }

      int position = spec.fields().indexOf(field);
      return new Index(position, spec.javaClasses()[position], literalsByValue);
    }

    return null;
  }

  private static class Index {
    private final int position;
    private final Class<?> javaClass;
    private final Map<Object, Set<Object>> literalsByValue;

    private Index(int position, Class<?> javaClass, Map<Object, Set<Object>> literalsByValue) {
      this.position = position;
      this.javaClass = javaClass;
      this.literalsByValue = literalsByValue;
    }

    @SuppressWarnings("unchecked")
    private <T> Collection<T> literals(StructLike partition) {
      Object partitionValue = partition.get(position, (Class<Object>) javaClass);
      Set<Object> literals = literalsByValue.get(partitionValue);
      // a partition value with no literals, including null, cannot match any literal
      return literals != null ? (Set<T>) (Set<?>) literals : Collections.emptySet();
    }
  }

  // Only bucket fields narrow literals: other transforms either preserve order, so file metrics
  // already bound the matching literals, or cannot be applied to literals (void and unknown).
  private static class IsBucketTransform implements PartitionSpecVisitor<Boolean> {
    private static final IsBucketTransform INSTANCE = new IsBucketTransform();

    private IsBucketTransform() {}

    @Override
    public Boolean identity(int fieldId, String sourceName, int sourceId) {
      return false;
    }

    @Override
    public Boolean bucket(int fieldId, String sourceName, int sourceId, int numBuckets) {
      return true;
    }

    @Override
    public Boolean truncate(int fieldId, String sourceName, int sourceId, int width) {
      return false;
    }

    @Override
    public Boolean year(int fieldId, String sourceName, int sourceId) {
      return false;
    }

    @Override
    public Boolean month(int fieldId, String sourceName, int sourceId) {
      return false;
    }

    @Override
    public Boolean day(int fieldId, String sourceName, int sourceId) {
      return false;
    }

    @Override
    public Boolean hour(int fieldId, String sourceName, int sourceId) {
      return false;
    }

    @Override
    public Boolean alwaysNull(int fieldId, String sourceName, int sourceId) {
      return false;
    }

    @Override
    public Boolean unknown(int fieldId, String sourceName, int sourceId, String transform) {
      return false;
    }
  }
}
