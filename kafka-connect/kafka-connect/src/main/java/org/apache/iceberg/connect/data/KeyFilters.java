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

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.ToDoubleFunction;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
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
import org.apache.iceberg.types.Type;
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

  // The filters are evaluated once per data file entry during planning, so their size is chosen
  // from the table's data file count: a budget of comparisons for the whole plan, divided by the
  // number of data files. The budget is what a fixed cap of 100 value ranges (200 comparisons per
  // file) would spend on a table with one million data files, so such a table keeps that cost
  // and smaller tables get a finer filter. The lower bound is that same cap and applies when the
  // count is missing; the upper bound limits what the filter costs when every file is read
  // anyway, e.g. keys spread over a small table, where a finer filter only adds evaluation time.
  @VisibleForTesting static final long COMPARISON_BUDGET = 200_000_000L;
  @VisibleForTesting static final int MIN_COMPARISONS_PER_FILE = 200;
  @VisibleForTesting static final int MAX_COMPARISONS_PER_FILE = 1_000;

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
   * Comparisons the key filter may cost per data file entry when planning against the snapshot:
   * {@link #COMPARISON_BUDGET} divided by the snapshot's {@code total-data-files}, kept between
   * {@link #MIN_COMPARISONS_PER_FILE} and {@link #MAX_COMPARISONS_PER_FILE}. A snapshot without the
   * count gets the minimum.
   */
  static int comparisonsPerFile(Snapshot snapshot) {
    String value = snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP);
    if (value == null) {
      return MIN_COMPARISONS_PER_FILE;
    }

    long dataFiles;
    try {
      dataFiles = Long.parseLong(value);
    } catch (NumberFormatException e) {
      return MIN_COMPARISONS_PER_FILE;
    }

    if (dataFiles <= 0) {
      return MAX_COMPARISONS_PER_FILE;
    }

    long perFile = COMPARISON_BUDGET / dataFiles;
    return (int) Math.max(MIN_COMPARISONS_PER_FILE, Math.min(MAX_COMPARISONS_PER_FILE, perFile));
  }

  /**
   * Filter for the deleted keys within a budget of comparisons per data file entry.
   *
   * <p>While the keys fit the budget they are matched with {@code IN} lists, {@link
   * #IN_PREDICATE_LIMIT} keys per list and one list per key column, which matches exactly the files
   * that hold a deleted key. A list costs one comparison per value, so the keys fit when their
   * number times the number of key columns is within the budget. Above that the keys are matched
   * with value ranges on the leading key column, two comparisons each, split at the largest gaps; a
   * range may also cover files without a deleted key.
   */
  static Expression keyFilter(Schema keySchema, StructLikeSet keys, int comparisonsPerFile) {
    long listComparisons = (long) keys.size() * primitiveColumns(keySchema);
    if (listComparisons <= comparisonsPerFile) {
      return listFilter(keySchema, keys);
    }

    return rangeFilter(keySchema, keys, Math.max(1, comparisonsPerFile / 2));
  }

  private static int primitiveColumns(Schema keySchema) {
    int count = 0;
    for (Types.NestedField column : keySchema.columns()) {
      if (column.type().isPrimitiveType()) {
        count++;
      }
    }

    return count;
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

  /**
   * Sorts the values of the leading key column and splits them into at most {@code maxRanges}
   * ranges. Numeric and temporal keys are split at the largest gaps, so a cluster of hot keys and a
   * few scattered keys each get a tight range; other types are split into ranges with an equal
   * number of keys.
   */
  @SuppressWarnings("unchecked")
  private static Expression rangeFilter(Schema keySchema, StructLikeSet keys, int maxRanges) {
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

    List<Object> values = Lists.newArrayListWithCapacity(keys.size());
    boolean hasNull = false;
    for (StructLike key : keys) {
      Object value = key.get(leadPos, Object.class);
      if (value == null) {
        hasNull = true;
      } else {
        values.add(value);
      }
    }

    Expression filter = Expressions.alwaysFalse();
    if (!values.isEmpty()) {
      Comparator<Object> comparator =
          (Comparator<Object>) Comparators.forType(lead.type().asPrimitiveType());
      values.sort(comparator);
      for (int[] range : splitIntoRanges(values, numericConverter(lead.type()), maxRanges)) {
        Object min = values.get(range[0]);
        Object max = values.get(range[1]);
        filter =
            Expressions.or(
                filter,
                Expressions.and(
                    Expressions.greaterThanOrEqual(lead.name(), min),
                    Expressions.lessThanOrEqual(lead.name(), max)));
      }
    }

    if (hasNull) {
      filter = Expressions.or(filter, Expressions.isNull(lead.name()));
    }

    return filter;
  }

  /**
   * Returns index ranges [first, last] over the sorted values, at most {@code maxRanges} of them.
   * With a numeric converter the ranges are separated at the largest gaps between neighboring
   * values, and only at gaps that leave out at least one value; otherwise the values are split into
   * ranges of equal size.
   */
  @VisibleForTesting
  static List<int[]> splitIntoRanges(
      List<Object> sortedValues, ToDoubleFunction<Object> numeric, int maxRanges) {
    int size = sortedValues.size();
    int rangeCount = Math.min(maxRanges, size);
    List<Integer> splits = Lists.newArrayListWithCapacity(rangeCount);

    if (numeric != null) {
      // a split after index i separates values i and i + 1. Only a gap wide enough to leave out a
      // value can exclude a data file, so adjacent integers are never split; of the remaining gaps
      // the widest are kept. Contiguous keys therefore form a single range however many they are.
      double minGap = isIntegral(sortedValues.get(0)) ? 1.0 : 0.0;
      List<Integer> byGap = Lists.newArrayList();
      for (int i = 0; i < size - 1; i++) {
        double gap =
            numeric.applyAsDouble(sortedValues.get(i + 1))
                - numeric.applyAsDouble(sortedValues.get(i));
        if (gap > minGap) {
          byGap.add(i);
        }
      }

      byGap.sort(
          Comparator.comparingDouble(
                  (Integer i) ->
                      numeric.applyAsDouble(sortedValues.get(i + 1))
                          - numeric.applyAsDouble(sortedValues.get(i)))
              .reversed());
      splits.addAll(byGap.subList(0, Math.min(rangeCount - 1, byGap.size())));
    } else {
      for (int i = 1; i < rangeCount; i++) {
        splits.add((int) ((long) i * size / rangeCount) - 1);
      }
    }

    splits.sort(Comparator.naturalOrder());

    List<int[]> ranges = Lists.newArrayListWithCapacity(rangeCount);
    int first = 0;
    for (int split : splits) {
      ranges.add(new int[] {first, split});
      first = split + 1;
    }

    ranges.add(new int[] {first, size - 1});
    return ranges;
  }

  private static boolean isIntegral(Object value) {
    return value instanceof Long || value instanceof Integer;
  }

  /** Converts internal key representations to a number for gap comparison, or null. */
  private static ToDoubleFunction<Object> numericConverter(Type type) {
    switch (type.typeId()) {
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case TIMESTAMP_NANO:
        return value -> ((Number) value).doubleValue();
      case DECIMAL:
        return value -> ((BigDecimal) value).doubleValue();
      default:
        return null;
    }
  }
}
