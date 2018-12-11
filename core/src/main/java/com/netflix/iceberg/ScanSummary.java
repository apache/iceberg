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

package com.netflix.iceberg;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.expressions.And;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Expression.Operation;
import com.netflix.iceberg.expressions.Expressions;
import com.netflix.iceberg.expressions.Literal;
import com.netflix.iceberg.expressions.NamedReference;
import com.netflix.iceberg.expressions.UnboundPredicate;
import com.netflix.iceberg.io.CloseableIterable;
import com.netflix.iceberg.types.Comparators;
import com.netflix.iceberg.types.Types;
import com.netflix.iceberg.util.Pair;
import java.io.IOException;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

public class ScanSummary {
  private ScanSummary() {
  }

  private static final List<String> SCAN_SUMMARY_COLUMNS = ImmutableList.of(
      "partition", "record_count", "file_size_in_bytes");

  /**
   * Create a scan summary builder for a table scan.
   *
   * @param scan a TableScan
   * @return a scan summary builder
   */
  public static ScanSummary.Builder of(TableScan scan) {
    return new Builder(scan);
  }

  public static class Builder {
    private static final Set<String> TIMESTAMP_NAMES = Sets.newHashSet(
        "dateCreated", "lastUpdated");
    private final TableScan scan;
    private final Table table;
    private final TableOperations ops;
    private final Map<Long, Long> snapshotTimestamps;
    private int limit = Integer.MAX_VALUE;
    private boolean throwIfLimited = false;
    private List<UnboundPredicate<Long>> timeFilters = Lists.newArrayList();

    public Builder(TableScan scan) {
      this.scan = scan;
      this.table = scan.table();
      this.ops = ((HasTableOperations) table).operations();
      ImmutableMap.Builder<Long, Long> builder = ImmutableMap.builder();
      for (Snapshot snap : table.snapshots()) {
        builder.put(snap.snapshotId(), snap.timestampMillis());
      }
      this.snapshotTimestamps = builder.build();
    }

    private void addTimestampFilter(UnboundPredicate<Long> filter) {
      throwIfLimited(); // ensure all partitions can be returned
      timeFilters.add(filter);
    }

    public Builder after(String timestamp) {
      Literal<Long> tsLiteral = Literal.of(timestamp).to(Types.TimestampType.withoutZone());
      return after(tsLiteral.value() / 1000);
    }

    public Builder after(long timestampMillis) {
      addTimestampFilter(Expressions.greaterThanOrEqual("timestamp_ms", timestampMillis));
      return this;
    }

    public Builder before(String timestamp) {
      Literal<Long> tsLiteral = Literal.of(timestamp).to(Types.TimestampType.withoutZone());
      return before(tsLiteral.value() / 1000);
    }

    public Builder before(long timestampMillis) {
      addTimestampFilter(Expressions.lessThanOrEqual("timestamp_ms", timestampMillis));
      return this;
    }

    public Builder throwIfLimited() {
      this.throwIfLimited = true;
      return this;
    }

    public Builder limit(int numPartitions) {
      this.limit = numPartitions;
      return this;
    }

    private void removeTimeFilters(List<Expression> expressions, Expression expression) {
      if (expression.op() == Operation.AND) {
        And and = (And) expression;
        removeTimeFilters(expressions, and.left());
        removeTimeFilters(expressions, and.right());
        return;

      } else if (expression instanceof UnboundPredicate) {
        UnboundPredicate pred = (UnboundPredicate) expression;
        NamedReference ref = (NamedReference) pred.ref();
        Literal<?> lit = pred.literal();
        if (TIMESTAMP_NAMES.contains(ref.name())) {
          Literal<Long> tsLiteral = lit.to(Types.TimestampType.withoutZone());
          long millis = toMillis(tsLiteral.value());
          addTimestampFilter(Expressions.predicate(pred.op(), "timestamp_ms", millis));
          return;
        }
      }

      expressions.add(expression);
    }

    /**
     * Summarizes a table scan as a map of partition key to metrics for that partition.
     *
     * @return a map from partition key to metrics for that partition.
     */
    public Map<String, PartitionMetrics> build() {
      TopN<String, PartitionMetrics> topN = new TopN<>(
          limit, throwIfLimited, Comparators.charSequences());

      List<Expression> filters = Lists.newArrayList();
      removeTimeFilters(filters, Expressions.rewriteNot(scan.filter()));
      Expression rowFilter = joinFilters(filters);

      Iterable<ManifestFile> manifests = table.currentSnapshot().manifests();

      boolean filterByTimestamp = !timeFilters.isEmpty();
      Set<Long> snapshotsInTimeRange = Sets.newHashSet();
      if (filterByTimestamp) {
        Pair<Long, Long> range = timestampRange(timeFilters);
        long minTimestamp = range.first();
        long maxTimestamp = range.second();

        for (Map.Entry<Long, Long> entry : snapshotTimestamps.entrySet()) {
          long snapshotId = entry.getKey();
          long timestamp = entry.getValue();
          if (timestamp >= minTimestamp && timestamp <= maxTimestamp) {
            snapshotsInTimeRange.add(snapshotId);
          }
        }

        // when filtering by dateCreated or lastUpdated timestamp, this matches the set of files
        // that were added in the time range. files are added in new snapshots, so to get the new
        // files, this only needs to scan new manifests in the set of snapshots that match the
        // filter. ManifestFile.snapshotId() returns the snapshot when the manifest was added, so
        // the only manifests that need to be scanned are those with snapshotId() in the timestamp
        // range, or those that don't have a snapshot ID.
        manifests = Iterables.filter(manifests, manifest ->
            manifest.snapshotId() == null || snapshotsInTimeRange.contains(manifest.snapshotId()));
      }

      try (CloseableIterable<ManifestEntry> entries = new ManifestGroup(ops, manifests)
          .filterData(rowFilter)
          .ignoreDeleted()
          .select(SCAN_SUMMARY_COLUMNS)
          .entries()) {

        PartitionSpec spec = table.spec();
        for (ManifestEntry entry : entries) {
          Long timestamp = snapshotTimestamps.get(entry.snapshotId());

          // if filtering, skip timestamps that are outside the range
          if (filterByTimestamp && !snapshotsInTimeRange.contains(entry.snapshotId())) {
            continue;
          }

          String partition = spec.partitionToPath(entry.file().partition());
          topN.update(partition, metrics -> (metrics == null ? new PartitionMetrics() : metrics)
              .updateFromFile(entry.file(), timestamp));
        }

      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }

      return topN.get();
    }
  }

  public static class PartitionMetrics {
    private int fileCount = 0;
    private long recordCount = 0L;
    private long totalSize = 0L;
    private Long dataTimestampMillis = null;

    public int fileCount() {
      return fileCount;
    }

    public long recordCount() {
      return recordCount;
    }

    public long totalSize() {
      return totalSize;
    }

    public Long dataTimestampMillis() {
      return dataTimestampMillis;
    }

    private PartitionMetrics updateFromFile(DataFile file, Long timestampMillis) {
      this.fileCount += 1;
      this.recordCount += file.recordCount();
      this.totalSize += file.fileSizeInBytes();
      if (timestampMillis != null &&
          (dataTimestampMillis == null || dataTimestampMillis < timestampMillis)) {
        this.dataTimestampMillis = timestampMillis;
      }
      return this;
    }

    @Override
    public String toString() {
      String dataTimestamp = dataTimestampMillis != null ?
          new Date(dataTimestampMillis).toString() : null;
      return "PartitionMetrics(fileCount=" + fileCount +
          ", recordCount=" + recordCount +
          ", totalSize=" + totalSize +
          ", dataTimestamp=" + dataTimestamp + ")";
    }
  }

  private static class TopN<K, V> {
    private final int maxSize;
    private final boolean throwIfLimited;
    private final TreeMap<K, V> map;
    private final Comparator<? super K> keyComparator;
    private K cut = null;

    TopN(int N, boolean throwIfLimited, Comparator<? super K> keyComparator) {
      this.maxSize = N;
      this.throwIfLimited = throwIfLimited;
      this.map = Maps.newTreeMap(keyComparator);
      this.keyComparator = keyComparator;
    }

    public void update(K key, Function<V, V> updateFunc) {
      // if there is a cut and it comes before the given key, do nothing
      if (cut != null && keyComparator.compare(cut, key) <= 0) {
        return;
      }

      // call the update function and add the result to the map
      map.put(key, updateFunc.apply(map.get(key)));

      // enforce the size constraint and update the cut if some keys are excluded
      while (map.size() > maxSize) {
        if (throwIfLimited) {
          throw new IllegalStateException(
              String.format("Too many matching keys: more than %d", maxSize));
        }
        this.cut = map.lastKey();
        map.remove(cut);
      }
    }

    public Map<K, V> get() {
      return ImmutableMap.copyOf(map);
    }
  }

  static Expression joinFilters(List<Expression> expressions) {
    Expression result = Expressions.alwaysTrue();
    for (Expression expression : expressions) {
      result = Expressions.and(result, expression);
    }
    return result;
  }

  static long toMillis(long timestamp) {
    if (timestamp < 10000000000L) {
      // in seconds
      return timestamp * 1000;
    } else if (timestamp < 10000000000000L) {
      // in millis
      return timestamp;
    }
    // in micros
    return timestamp / 1000;
  }

  static Pair<Long, Long> timestampRange(List<UnboundPredicate<Long>> timeFilters) {
    // evaluation is inclusive
    long minTimestamp = Long.MIN_VALUE;
    long maxTimestamp = Long.MAX_VALUE;

    for (UnboundPredicate<Long> pred : timeFilters) {
      long value = pred.literal().value();
      switch (pred.op()) {
        case LT:
          if (value - 1 < maxTimestamp) {
            maxTimestamp = value - 1;
          }
          break;
        case LT_EQ:
          if (value < maxTimestamp) {
            maxTimestamp = value;
          }
          break;
        case GT:
          if (value + 1 > minTimestamp) {
            minTimestamp = value + 1;
          }
          break;
        case GT_EQ:
          if (value > minTimestamp) {
            minTimestamp = value;
          }
          break;
        case EQ:
          if (value < maxTimestamp) {
            maxTimestamp = value;
          }
          if (value > minTimestamp) {
            minTimestamp = value;
          }
          break;
        default:
          throw new UnsupportedOperationException(
              "Cannot filter timestamps using predicate: " + pred);
      }
    }

    if (maxTimestamp < minTimestamp) {
      throw new IllegalArgumentException(
          "No timestamps can match filters: " + Joiner.on(", ").join(timeFilters));
    }

    return Pair.of(minTimestamp, maxTimestamp);
  }
}
