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
package org.apache.iceberg.connect.data;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.UnknownTransform;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;

/**
 * Row filters built from the keys of equality delete files.
 *
 * <p>The filters are inclusive: every row with a deleted key satisfies them, and rows without a
 * deleted key may satisfy them too. {@link ScanKeyPositionResolver} uses them to prune the data
 * files it has to read; {@link EqualityDeleteConverter} uses them to detect files that another
 * writer added concurrently for the deleted keys.
 */
final class KeyFilters {

  // metrics evaluators stop pruning with an IN list above this many values, so keys are matched
  // with several lists of at most this size
  @VisibleForTesting static final int IN_PREDICATE_LIMIT = 200;

  // the filters are evaluated once per data file entry during planning; above this many keys the
  // lists are replaced by the bounds of the leading key column, two comparisons per entry
  @VisibleForTesting static final int MAX_LISTED_KEYS = 1_000;

  private KeyFilters() {}

  /**
   * Pins the partition of an equality delete file. Every partition field is expressed through its
   * transform, e.g. {@code bucket(16, id) = 3}, so the scan planner prunes to the partition even
   * when the key filter is a range that a transform like bucket cannot project. Identity fields
   * reference the column directly. Void fields carry no information and unknown transforms cannot
   * be evaluated, so both are skipped.
   */
  @SuppressWarnings("unchecked")
  static Expression partitionFilter(Schema tableSchema, PartitionSpec spec, StructLike partition) {
    Expression filter = Expressions.alwaysTrue();
    List<PartitionField> fields = spec.fields();
    for (int pos = 0; pos < fields.size(); pos++) {
      PartitionField field = fields.get(pos);
      Transform<?, ?> transform = field.transform();
      if (transform.isVoid() || transform instanceof UnknownTransform) {
        continue;
      }

      String column = tableSchema.findColumnName(field.sourceId());
      UnboundTerm<Object> term =
          transform.isIdentity()
              ? Expressions.ref(column)
              : Expressions.transform(column, (Transform<?, Object>) transform);
      Object value = partition.get(pos, Object.class);
      filter =
          Expressions.and(
              filter, value == null ? Expressions.isNull(term) : Expressions.equal(term, value));
    }

    return filter;
  }

  /**
   * Filter for the deleted keys. Up to {@link #MAX_LISTED_KEYS} keys are matched with {@code IN}
   * lists of at most {@link #IN_PREDICATE_LIMIT} keys each, one list per key column. More keys are
   * matched with the minimum and maximum of the leading key column, which may also cover files that
   * hold no deleted key.
   */
  static Expression keyFilter(Schema keySchema, StructLikeSet keys) {
    if (keys.size() <= MAX_LISTED_KEYS) {
      return listFilter(keySchema, keys);
    }

    return boundsFilter(keySchema, keys);
  }

  /** {@code IN} lists of at most {@link #IN_PREDICATE_LIMIT} keys each, joined with {@code OR}. */
  private static Expression listFilter(Schema keySchema, StructLikeSet keys) {
    List<StructLike> keyList = Lists.newArrayList(keys);
    Expression filter = Expressions.alwaysFalse();
    for (int from = 0; from < keyList.size(); from += IN_PREDICATE_LIMIT) {
      List<StructLike> chunk =
          keyList.subList(from, Math.min(from + IN_PREDICATE_LIMIT, keyList.size()));
      filter = Expressions.or(filter, inFilter(keySchema, chunk));
    }

    return filter;
  }

  /** One {@code IN} list per key column over the given keys. */
  private static Expression inFilter(Schema keySchema, Iterable<StructLike> keys) {
    Expression filter = Expressions.alwaysTrue();
    List<Types.NestedField> columns = keySchema.columns();
    for (int pos = 0; pos < columns.size(); pos++) {
      Types.NestedField column = columns.get(pos);
      if (!column.type().isPrimitiveType()) {
        // no pruning on a nested key column
        continue;
      }

      Set<Object> values = Sets.newHashSet();
      boolean hasNull = false;
      for (StructLike key : keys) {
        Object value = key.get(pos, Object.class);
        if (value == null) {
          hasNull = true;
        } else {
          values.add(value);
        }
      }

      Expression columnFilter =
          values.isEmpty() ? Expressions.alwaysFalse() : Expressions.in(column.name(), values);
      if (hasNull) {
        columnFilter = Expressions.or(columnFilter, Expressions.isNull(column.name()));
      }

      filter = Expressions.and(filter, columnFilter);
    }

    return filter;
  }

  /** The minimum and maximum of the leading key column over the given keys. */
  @SuppressWarnings("unchecked")
  private static Expression boundsFilter(Schema keySchema, StructLikeSet keys) {
    Types.NestedField lead = null;
    int leadPos = -1;
    List<Types.NestedField> columns = keySchema.columns();
    for (int pos = 0; pos < columns.size(); pos++) {
      if (columns.get(pos).type().isPrimitiveType()) {
        lead = columns.get(pos);
        leadPos = pos;
        break;
      }
    }

    if (lead == null) {
      return Expressions.alwaysTrue();
    }

    Comparator<Object> comparator =
        (Comparator<Object>) Comparators.forType(lead.type().asPrimitiveType());
    Object min = null;
    Object max = null;
    boolean hasNull = false;
    for (StructLike key : keys) {
      Object value = key.get(leadPos, Object.class);
      if (value == null) {
        hasNull = true;
      } else {
        if (min == null || comparator.compare(value, min) < 0) {
          min = value;
        }

        if (max == null || comparator.compare(value, max) > 0) {
          max = value;
        }
      }
    }

    Expression filter =
        min == null
            ? Expressions.alwaysFalse()
            : Expressions.and(
                Expressions.greaterThanOrEqual(lead.name(), min),
                Expressions.lessThanOrEqual(lead.name(), max));
    if (hasNull) {
      filter = Expressions.or(filter, Expressions.isNull(lead.name()));
    }

    return filter;
  }
}
