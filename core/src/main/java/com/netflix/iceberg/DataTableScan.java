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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.InclusiveManifestEvaluator;
import com.netflix.iceberg.expressions.ResidualEvaluator;
import com.netflix.iceberg.io.CloseableIterable;
import com.netflix.iceberg.util.ParallelIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.netflix.iceberg.util.ThreadPools.getWorkerPool;

public class DataTableScan extends BaseTableScan {
  private static final Logger LOG = LoggerFactory.getLogger(DataTableScan.class);

  private static final List<String> SNAPSHOT_COLUMNS = ImmutableList.of(
      "snapshot_id", "file_path", "file_ordinal", "file_format", "block_size_in_bytes",
      "file_size_in_bytes", "record_count", "partition", "value_counts", "null_value_counts",
      "lower_bounds", "upper_bounds"
  );
  private static final boolean PLAN_SCANS_WITH_WORKER_POOL =
      SystemProperties.getBoolean(SystemProperties.SCAN_THREAD_POOL_ENABLED, true);

  public DataTableScan(TableOperations ops, Table table) {
    super(ops, table, table.schema());
  }

  protected DataTableScan(TableOperations ops, Table table, Long snapshotId, Schema schema,
                          Expression rowFilter) {
    super(ops, table, snapshotId, schema, rowFilter);
  }

  protected TableScan newRefinedScan(TableOperations ops, Table table, Long snapshotId,
                                     Schema schema, Expression rowFilter) {
    return new DataTableScan(ops, table, snapshotId, schema, rowFilter);
  }

  public CloseableIterable<FileScanTask> planFiles(TableOperations ops, Snapshot snapshot,
                                                   Expression rowFilter) {
    LoadingCache<Integer, InclusiveManifestEvaluator> evalCache = CacheBuilder
        .newBuilder()
        .build(new CacheLoader<Integer, InclusiveManifestEvaluator>() {
          @Override
          public InclusiveManifestEvaluator load(Integer specId) {
            PartitionSpec spec = ops.current().spec(specId);
            return new InclusiveManifestEvaluator(spec, rowFilter);
          }
        });

    Iterable<ManifestFile> matchingManifests = Iterables.filter(snapshot.manifests(),
        manifest -> evalCache.getUnchecked(manifest.partitionSpecId()).eval(manifest));

    ConcurrentLinkedQueue<Closeable> toClose = new ConcurrentLinkedQueue<>();
    Iterable<Iterable<FileScanTask>> readers = Iterables.transform(
        matchingManifests,
        manifest -> {
          ManifestReader reader = ManifestReader.read(ops.io().newInputFile(manifest.path()));
          toClose.add(reader);
          String schemaString = SchemaParser.toJson(reader.spec().schema());
          String specString = PartitionSpecParser.toJson(reader.spec());
          ResidualEvaluator residuals = ResidualEvaluator.of(reader.spec(), rowFilter);
          return Iterables.transform(
              reader.filterRows(rowFilter).select(SNAPSHOT_COLUMNS),
              file -> new BaseFileScanTask(file, schemaString, specString, residuals)
          );
        });

    if (PLAN_SCANS_WITH_WORKER_POOL && snapshot.manifests().size() > 1) {
      return CloseableIterable.combine(
          new ParallelIterable<>(readers, getWorkerPool()),
          toClose);
    } else {
      return CloseableIterable.combine(Iterables.concat(readers), toClose);
    }
  }

  protected long targetSplitSize(TableOperations ops) {
    return ops.current().propertyAsLong(
        TableProperties.SPLIT_SIZE, TableProperties.SPLIT_SIZE_DEFAULT);
  }
}
