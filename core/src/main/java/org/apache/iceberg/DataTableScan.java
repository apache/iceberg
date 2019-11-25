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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.util.Collection;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.ParallelIterable;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataTableScan extends BaseTableScan {
  private static final Logger LOG = LoggerFactory.getLogger(DataTableScan.class);

  private static final ImmutableList<String> SCAN_COLUMNS = ImmutableList.of(
      "snapshot_id", "file_path", "file_ordinal", "file_format", "block_size_in_bytes",
      "file_size_in_bytes", "record_count", "partition", "key_metadata"
  );
  private static final ImmutableList<String> SCAN_WITH_STATS_COLUMNS = ImmutableList.<String>builder()
      .addAll(SCAN_COLUMNS)
      .add("value_counts", "null_value_counts", "lower_bounds", "upper_bounds", "column_sizes")
      .build();
  private static final boolean PLAN_SCANS_WITH_WORKER_POOL =
      SystemProperties.getBoolean(SystemProperties.SCAN_THREAD_POOL_ENABLED, true);

  public DataTableScan(TableOperations ops, Table table) {
    super(ops, table, table.schema());
  }

  protected DataTableScan(TableOperations ops, Table table, Long snapshotId, Schema schema,
                          Expression rowFilter, boolean caseSensitive, boolean colStats,
                          Collection<String> selectedColumns, ImmutableMap<String, String> options) {
    super(ops, table, snapshotId, schema, rowFilter, caseSensitive, colStats, selectedColumns, options);
  }

  @Override
  protected TableScan newRefinedScan(
      TableOperations ops, Table table, Long snapshotId, Schema schema, Expression rowFilter,
      boolean caseSensitive, boolean colStats, Collection<String> selectedColumns,
      ImmutableMap<String, String> options) {
    return new DataTableScan(
        ops, table, snapshotId, schema, rowFilter, caseSensitive, colStats, selectedColumns, options);
  }

  @Override
  public CloseableIterable<FileScanTask> planFiles(TableOperations ops, Snapshot snapshot,
                                                   Expression rowFilter, boolean caseSensitive, boolean colStats) {
    LoadingCache<Integer, ManifestEvaluator> evalCache = Caffeine.newBuilder().build(specId -> {
      PartitionSpec spec = ops.current().spec(specId);
      return ManifestEvaluator.forRowFilter(rowFilter, spec, caseSensitive);
    });

    Iterable<ManifestFile> nonEmptyManifests = Iterables.filter(snapshot.manifests(),
        manifest -> manifest.hasAddedFiles() || manifest.hasExistingFiles());
    Iterable<ManifestFile> matchingManifests = Iterables.filter(nonEmptyManifests,
        manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest));

    Iterable<CloseableIterable<FileScanTask>> readers = Iterables.transform(
        matchingManifests,
        manifest -> {
          ManifestReader reader = ManifestReader.read(
              ops.io().newInputFile(manifest.path()),
              ops.current().specsById());
          PartitionSpec spec = ops.current().spec(manifest.partitionSpecId());
          String schemaString = SchemaParser.toJson(spec.schema());
          String specString = PartitionSpecParser.toJson(spec);
          ResidualEvaluator residuals = ResidualEvaluator.of(spec, rowFilter, caseSensitive);
          return CloseableIterable.transform(
              reader.filterRows(rowFilter)
                  .caseSensitive(caseSensitive)
                  .select(colStats ? SCAN_WITH_STATS_COLUMNS : SCAN_COLUMNS),
              file -> new BaseFileScanTask(file, schemaString, specString, residuals)
          );
        });

    if (PLAN_SCANS_WITH_WORKER_POOL && snapshot.manifests().size() > 1) {
      return new ParallelIterable<>(readers, ThreadPools.getWorkerPool());
    } else {
      return CloseableIterable.concat(readers);
    }
  }

  @Override
  protected long targetSplitSize(TableOperations ops) {
    return ops.current().propertyAsLong(
        TableProperties.SPLIT_SIZE, TableProperties.SPLIT_SIZE_DEFAULT);
  }
}
