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
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
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

    /**
     * Mean, over files with bounds, of the number of files each file's range overlaps with
     * (including itself), or null if no file has usable bounds.
     *
     * <p>This is a per-file average — how many neighbors a file has on the sort key — not the mean
     * sweep depth over the key range. A perfectly clustered partition reports 1; files that merely
     * touch at a boundary count as overlapping, so values slightly above 1 do not necessarily
     * indicate a degraded layout.
     */
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

    int sourceId = sortSourceId(table.sortOrder(), table.name());
    Type.PrimitiveType boundType = table.schema().findType(sourceId).asPrimitiveType();

    // request bounds for the sort column only, so file metadata for wide schemas stays small
    TableScan scan =
        table
            .newScan()
            .includeColumnStats(ImmutableSet.of(table.schema().findColumnName(sourceId)));
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

  /**
   * Returns the locations of data files that sit in a region of the sort key where the overlap
   * depth reaches {@code minOverlapDepth}.
   *
   * <p>A file is reported when its [lower bound, upper bound] range on the first sort field
   * intersects at least one point covered by {@code minOverlapDepth} or more files of the same
   * partition. Files without bounds for the sort field are never reported, since their layout
   * cannot be assessed from metadata. As in {@link #computeStats(Table)}, only data file metadata
   * is read.
   *
   * <p>This is the selection counterpart of {@link #computeStats(Table)}: it identifies the
   * individual files behind a high depth rather than summarizing the partition.
   *
   * @param files data files to consider, which may span partitions and specs
   * @param specsById the partition specs of the table, keyed by spec id
   * @param sortOrder the sort order to measure overlap on
   * @param tableName the table name, used in error messages
   * @param minOverlapDepth the depth at which a region is considered overlapping, must be > 1
   * @return locations of the files in such regions
   */
  public static Set<String> highOverlapFiles(
      Iterable<DataFile> files,
      Map<Integer, PartitionSpec> specsById,
      SortOrder sortOrder,
      String tableName,
      int minOverlapDepth) {
    Preconditions.checkArgument(files != null, "Invalid file list: null");
    Preconditions.checkArgument(specsById != null, "Invalid specs: null");
    Preconditions.checkArgument(
        minOverlapDepth > 1, "Invalid minimum overlap depth: %s, must be > 1", minOverlapDepth);

    int sourceId = sortSourceId(sortOrder, tableName);
    Type.PrimitiveType boundType = null;

    PartitionMap<List<Range>> rangesByPartition = PartitionMap.create(specsById);
    for (DataFile file : files) {
      if (boundType == null) {
        boundType = specsById.get(file.specId()).schema().findType(sourceId).asPrimitiveType();
      }

      Range range = boundsRange(file, sourceId, boundType);
      if (range != null) {
        rangesByPartition
            .computeIfAbsent(file.specId(), file.partition(), Lists::newArrayList)
            .add(range);
      }
    }

    if (boundType == null) {
      return ImmutableSet.of();
    }

    Comparator<Object> comparator = comparator(boundType);
    Set<String> marked = Sets.newHashSet();
    for (List<Range> ranges : rangesByPartition.values()) {
      markOverlapping(ranges, comparator, minOverlapDepth, marked);
    }

    return marked;
  }

  private static int sortSourceId(SortOrder sortOrder, String tableName) {
    ValidationException.check(
        sortOrder != null && sortOrder.isSorted(),
        "Table %s does not declare a sort order",
        tableName);

    SortField sortField = sortOrder.fields().get(0);
    ValidationException.check(
        sortField.transform().preservesOrder(),
        "Cannot compute overlap stats: transform %s of the first sort field does not preserve"
            + " order",
        sortField.transform());

    return sortField.sourceId();
  }

  /**
   * Marks every range that intersects a point where the number of covering ranges reaches {@code
   * minDepth}, using the same sweep line as the depth computation.
   */
  private static void markOverlapping(
      List<Range> ranges, Comparator<Object> comparator, int minDepth, Set<String> marked) {
    if (ranges.size() < minDepth) {
      return;
    }

    List<Event> events = events(ranges, comparator);

    // ranges that are currently active but not marked yet; once the active count reaches the
    // threshold, all of them intersect that region and are marked in one pass, which keeps the
    // total marking work linear in the number of ranges
    Set<Range> pending = Sets.newLinkedHashSet();
    int active = 0;
    for (Event event : events) {
      if (event.delta > 0) {
        active += 1;
        pending.add(event.range);
        if (active >= minDepth) {
          for (Range range : pending) {
            marked.add(range.location);
          }

          pending.clear();
        }
      } else {
        active -= 1;
        pending.remove(event.range);
      }
    }
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

    return new Range(lower, upper, file.location());
  }

  /**
   * Builds the sweep line events for the given ranges. At equal values, starts are processed before
   * ends so ranges that merely touch at a point are counted as overlapping.
   */
  private static List<Event> events(List<Range> ranges, Comparator<Object> comparator) {
    List<Event> events = Lists.newArrayListWithCapacity(ranges.size() * 2);
    for (Range range : ranges) {
      events.add(new Event(range.lower, 1, range));
      events.add(new Event(range.upper, -1, range));
    }

    events.sort(
        (left, right) -> {
          int cmp = comparator.compare(left.value, right.value);
          if (cmp != 0) {
            return cmp;
          }
          return Integer.compare(right.delta, left.delta);
        });

    return events;
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

    List<Event> events = events(ranges, comparator);

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
    private final String location;

    Range(Object lower, Object upper, String location) {
      this.lower = lower;
      this.upper = upper;
      this.location = location;
    }
  }

  private static class Event {
    private final Object value;
    private final int delta;
    private final Range range;

    Event(Object value, int delta, Range range) {
      this.value = value;
      this.delta = delta;
      this.range = range;
    }
  }
}
