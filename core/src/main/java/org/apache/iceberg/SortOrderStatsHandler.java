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
package org.apache.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionMap;

/**
 * Computes file overlap statistics on the table's sort order from data file column bounds.
 *
 * <p>For each partition, this handler measures how well the data file layout matches the table's
 * declared {@link SortOrder}: the <em>overlap depth</em> of a point on the sort key is the number
 * of data files whose [lower bound, upper bound] range on the first sort field contains that point.
 * A perfectly clustered partition has a maximum overlap depth of 1 (no two files overlap); a
 * partition where every file spans the whole key range has a depth equal to its file count.
 *
 * <p>The computation reads only data file metadata ({@link ContentFile#lowerBounds()} and {@link
 * ContentFile#upperBounds()}) — no data files are opened.
 *
 * <p>Only the first field of the sort order is considered, analogous to how clustering depth is
 * commonly reported for multi-column layouts. Because column bounds may be truncated, the reported
 * depth is an upper-bound estimate: truncation can only widen a file's range.
 */
public class SortOrderStatsHandler {

  private SortOrderStatsHandler() {}

  /** Overlap statistics for a single partition. */
  public static class PartitionOverlapStats {
    private final StructLike partition;
    private final int specId;
    private final int fileCount;
    private final int filesMissingBounds;
    private final Integer maxOverlapDepth;
    private final Double avgOverlapDepth;

    PartitionOverlapStats(
        StructLike partition,
        int specId,
        int fileCount,
        int filesMissingBounds,
        Integer maxOverlapDepth,
        Double avgOverlapDepth) {
      this.partition = partition;
      this.specId = specId;
      this.fileCount = fileCount;
      this.filesMissingBounds = filesMissingBounds;
      this.maxOverlapDepth = maxOverlapDepth;
      this.avgOverlapDepth = avgOverlapDepth;
    }

    public StructLike partition() {
      return partition;
    }

    public int specId() {
      return specId;
    }

    public int fileCount() {
      return fileCount;
    }

    /** Number of files without bounds for the sort field; excluded from depth computation. */
    public int filesMissingBounds() {
      return filesMissingBounds;
    }

    /** Maximum overlap depth, or null if no file in the partition has usable bounds. */
    public Integer maxOverlapDepth() {
      return maxOverlapDepth;
    }

    /** Mean overlap depth over files with bounds, or null if none have usable bounds. */
    public Double avgOverlapDepth() {
      return avgOverlapDepth;
    }
  }

  /**
   * Computes per-partition overlap stats for the table's current snapshot.
   *
   * @param table the table to analyze
   * @return per-partition overlap statistics
   */
  public static List<PartitionOverlapStats> computeStats(Table table) {
    return computeStats(table, null);
  }

  /**
   * Computes per-partition overlap stats for the given snapshot.
   *
   * @param table the table to analyze
   * @param snapshotId the snapshot to analyze, or null for the current snapshot
   * @return per-partition overlap statistics
   */
  public static List<PartitionOverlapStats> computeStats(Table table, Long snapshotId) {
    Preconditions.checkArgument(table != null, "Invalid table: null");

    SortOrder sortOrder = table.sortOrder();
    ValidationException.check(
        sortOrder.isSorted(), "Table %s does not declare a sort order", table.name());

    SortField sortField = sortOrder.fields().get(0);
    ValidationException.check(
        sortField.transform().preservesOrder(),
        "Cannot compute overlap stats: transform %s of the first sort field does not preserve"
            + " order",
        sortField.transform());

    int sourceId = sortField.sourceId();
    Type.PrimitiveType boundType = table.schema().findType(sourceId).asPrimitiveType();

    TableScan scan = table.newScan().includeColumnStats();
    if (snapshotId != null) {
      scan = scan.useSnapshot(snapshotId);
    }

    PartitionMap<List<Range>> rangesByPartition = PartitionMap.create(table.specs());
    PartitionMap<Integer> missingByPartition = PartitionMap.create(table.specs());

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        DataFile file = task.file();
        int specId = file.specId();
        StructLike partition = file.partition();
        Range range = boundsRange(file, sourceId, boundType);
        if (range != null) {
          rangesByPartition.computeIfAbsent(specId, partition, Lists::newArrayList).add(range);
        } else {
          Integer missing = missingByPartition.get(specId, partition);
          missingByPartition.put(specId, partition, missing == null ? 1 : missing + 1);
          // ensure the partition is reported even if no file has bounds
          rangesByPartition.computeIfAbsent(specId, partition, Lists::newArrayList);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    Comparator<Object> comparator = comparator(boundType);
    List<PartitionOverlapStats> results = Lists.newArrayList();
    for (Map.Entry<Pair<Integer, StructLike>, List<Range>> entry : rangesByPartition.entrySet()) {
      int specId = entry.getKey().first();
      StructLike partition = entry.getKey().second();
      List<Range> ranges = entry.getValue();
      Integer missing = missingByPartition.get(specId, partition);
      results.add(
          partitionStats(partition, specId, ranges, missing == null ? 0 : missing, comparator));
    }

    return results;
  }

  @SuppressWarnings("unchecked")
  private static Comparator<Object> comparator(Type.PrimitiveType type) {
    return (Comparator<Object>) Comparators.forType(type);
  }

  private static Range boundsRange(DataFile file, int sourceId, Type.PrimitiveType type) {
    Map<Integer, ByteBuffer> lowers = file.lowerBounds();
    Map<Integer, ByteBuffer> uppers = file.upperBounds();
    if (lowers == null || uppers == null) {
      return null;
    }

    ByteBuffer lowerBuffer = lowers.get(sourceId);
    ByteBuffer upperBuffer = uppers.get(sourceId);
    if (lowerBuffer == null || upperBuffer == null) {
      return null;
    }

    Object lower = Conversions.fromByteBuffer(type, lowerBuffer);
    Object upper = Conversions.fromByteBuffer(type, upperBuffer);
    if (lower == null || upper == null) {
      return null;
    }

    return new Range(lower, upper);
  }

  private static PartitionOverlapStats partitionStats(
      StructLike partition,
      int specId,
      List<Range> ranges,
      int missing,
      Comparator<Object> comparator) {
    int fileCount = ranges.size() + missing;
    if (ranges.isEmpty()) {
      return new PartitionOverlapStats(partition, specId, fileCount, missing, null, null);
    }

    // sweep line over closed intervals: at equal values, starts are processed before ends so
    // ranges that merely touch at a point are counted as overlapping
    List<Event> events = Lists.newArrayListWithCapacity(ranges.size() * 2);
    for (Range range : ranges) {
      events.add(new Event(range.lower, 1));
      events.add(new Event(range.upper, -1));
    }

    events.sort(
        (left, right) -> {
          int cmp = comparator.compare(left.value, right.value);
          if (cmp != 0) {
            return cmp;
          }
          return Integer.compare(right.delta, left.delta);
        });

    int depth = 0;
    int maxDepth = 0;
    for (Event event : events) {
      depth += event.delta;
      maxDepth = Math.max(maxDepth, depth);
    }

    // average depth over files: mean, per file, of how many files its range overlaps with
    // (including itself); computed as sum of pairwise overlaps via sweep re-scan
    long overlapPairs = 0;
    int active = 0;
    for (Event event : events) {
      if (event.delta > 0) {
        overlapPairs += active;
        active += 1;
      } else {
        active -= 1;
      }
    }

    double avgDepth = 1.0 + (2.0 * overlapPairs) / ranges.size();

    return new PartitionOverlapStats(partition, specId, fileCount, missing, maxDepth, avgDepth);
  }

  private static class Range {
    private final Object lower;
    private final Object upper;

    Range(Object lower, Object upper) {
      this.lower = lower;
      this.upper = upper;
    }
  }

  private static class Event {
    private final Object value;
    private final int delta;

    Event(Object value, int delta) {
      this.value = value;
      this.delta = delta;
    }
  }
}
