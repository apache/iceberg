/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.types.Comparators;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

public class ScanSummary {
  private ScanSummary() {
  }

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
    private final TableScan scan;
    private final Table table;
    private int limit = Integer.MAX_VALUE;

    public Builder(TableScan scan) {
      this.scan = scan;
      this.table = scan.table();
    }

    public Builder limit(int numPartitions) {
      this.limit = numPartitions;
      return this;
    }

    /**
     * Summarizes a table scan as a map of partition key to metrics for that partition.
     *
     * @return a map from partition key to metrics for that partition.
     */
    public Map<String, PartitionMetrics> build() {
      TopN<String, PartitionMetrics> topN = new TopN<>(limit, Comparators.charSequences());

      try {
        for (FileScanTask task : scan.planFiles()) {
          String partition = task.spec().partitionToPath(task.file().partition());
          topN.update(partition, metrics ->
              (metrics == null ? new PartitionMetrics() : metrics).updateFromFile(task.file()));
        }

      } finally {
        closeScan(scan);
      }

      return topN.get();
    }
  }

  private static void closeScan(TableScan scan) {
    try {
      scan.close();
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  public static class PartitionMetrics {
    private int fileCount = 0;
    private long recordCount = 0L;

    public int fileCount() {
      return fileCount;
    }

    public long recordCount() {
      return recordCount;
    }

    private PartitionMetrics updateFromFile(DataFile file) {
      this.fileCount += 1;
      this.recordCount += file.recordCount();
      return this;
    }

    @Override
    public String toString() {
      return "PartitionMetrics(fileCount=" + fileCount + ", recordCount=" + recordCount + ")";
    }
  }

  private static class TopN<K, V> {
    private final int maxSize;
    private final TreeMap<K, V> map;
    private final Comparator<? super K> keyComparator;
    private K cut = null;

    TopN(int N, Comparator<? super K> keyComparator) {
      this.maxSize = N;
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
        this.cut = map.lastKey();
        map.remove(cut);
      }
    }

    public Map<K, V> get() {
      return ImmutableMap.copyOf(map);
    }
  }
}
