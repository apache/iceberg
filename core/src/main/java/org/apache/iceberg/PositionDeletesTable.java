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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ParallelIterable;
import org.apache.iceberg.util.TableScanUtil;

/**
 * A {@link Table} implementation whose {@link Scan} provides {@link PositionDeletesScanTask}, for
 * reading of position delete files.
 */
public class PositionDeletesTable extends BaseMetadataTable {

  public static final String PARTITION = "partition";
  public static final String SPEC_ID = "spec_id";
  public static final String DELETE_FILE_PATH = "delete_file_path";

  private final Schema schema;
  private final int defaultSpecId;
  private final Map<Integer, PartitionSpec> specs;

  PositionDeletesTable(Table table) {
    this(table, table.name() + ".position_deletes");
  }

  PositionDeletesTable(Table table, String name) {
    super(table, name);
    this.schema = calculateSchema();
    this.defaultSpecId = table.spec().specId();
    this.specs = transformSpecs(schema(), table.specs());
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.POSITION_DELETES;
  }

  @Override
  public TableScan newScan() {
    throw new UnsupportedOperationException(
        "Cannot create TableScan from table of type POSITION_DELETES");
  }

  @Override
  public BatchScan newBatchScan() {
    return new PositionDeletesBatchScan(table(), schema());
  }

  @Override
  public Schema schema() {
    return schema;
  }

  @Override
  public PartitionSpec spec() {
    return specs.get(defaultSpecId);
  }

  @Override
  public Map<Integer, PartitionSpec> specs() {
    return specs;
  }

  private Schema calculateSchema() {
    Types.StructType partitionType = Partitioning.partitionType(table());
    Schema result =
        new Schema(
            MetadataColumns.DELETE_FILE_PATH,
            MetadataColumns.DELETE_FILE_POS,
            Types.NestedField.optional(
                MetadataColumns.DELETE_FILE_ROW_FIELD_ID,
                MetadataColumns.DELETE_FILE_ROW_FIELD_NAME,
                table().schema().asStruct(),
                MetadataColumns.DELETE_FILE_ROW_DOC),
            Types.NestedField.required(
                MetadataColumns.PARTITION_COLUMN_ID,
                PARTITION,
                partitionType,
                "Partition that position delete row belongs to"),
            Types.NestedField.required(
                MetadataColumns.SPEC_ID_COLUMN_ID,
                SPEC_ID,
                Types.IntegerType.get(),
                MetadataColumns.SPEC_ID_COLUMN_DOC),
            Types.NestedField.required(
                MetadataColumns.FILE_PATH_COLUMN_ID,
                DELETE_FILE_PATH,
                Types.StringType.get(),
                MetadataColumns.FILE_PATH_COLUMN_DOC));

    if (partitionType.fields().size() > 0) {
      return result;
    } else {
      // avoid returning an empty struct, which is not always supported.
      // instead, drop the partition field
      return TypeUtil.selectNot(result, Sets.newHashSet(MetadataColumns.PARTITION_COLUMN_ID));
    }
  }

  public static class PositionDeletesBatchScan
      extends SnapshotScan<BatchScan, ScanTask, ScanTaskGroup<ScanTask>> implements BatchScan {

    protected PositionDeletesBatchScan(Table table, Schema schema) {
      super(table, schema, TableScanContext.empty());
    }

    protected PositionDeletesBatchScan(Table table, Schema schema, TableScanContext context) {
      super(table, schema, context);
    }

    @Override
    protected PositionDeletesBatchScan newRefinedScan(
        Table newTable, Schema newSchema, TableScanContext newContext) {
      return new PositionDeletesBatchScan(newTable, newSchema, newContext);
    }

    @Override
    public CloseableIterable<ScanTaskGroup<ScanTask>> planTasks() {
      return TableScanUtil.planTaskGroups(
          planFiles(), targetSplitSize(), splitLookback(), splitOpenFileCost());
    }

    @Override
    protected List<String> scanColumns() {
      return context().returnColumnStats() ? DELETE_SCAN_WITH_STATS_COLUMNS : DELETE_SCAN_COLUMNS;
    }

    @Override
    protected CloseableIterable<ScanTask> doPlanFiles() {
      String schemaString = SchemaParser.toJson(tableSchema());

      // prepare transformed partition specs and caches
      Map<Integer, PartitionSpec> transformedSpecs = transformSpecs(tableSchema(), table().specs());

      LoadingCache<Integer, ResidualEvaluator> residualCache =
          partitionCacheOf(
              transformedSpecs,
              spec ->
                  ResidualEvaluator.of(
                      spec,
                      shouldIgnoreResiduals() ? Expressions.alwaysTrue() : filter(),
                      isCaseSensitive()));

      LoadingCache<Integer, String> specStringCache =
          partitionCacheOf(transformedSpecs, PartitionSpecParser::toJson);

      LoadingCache<Integer, ManifestEvaluator> evalCache =
          partitionCacheOf(
              transformedSpecs,
              spec -> ManifestEvaluator.forRowFilter(filter(), spec, isCaseSensitive()));

      // iterate through delete manifests
      List<ManifestFile> manifests = snapshot().deleteManifests(table().io());

      CloseableIterable<ManifestFile> matchingManifests =
          CloseableIterable.filter(
              scanMetrics().skippedDeleteManifests(),
              CloseableIterable.withNoopClose(manifests),
              manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest));

      matchingManifests =
          CloseableIterable.count(scanMetrics().scannedDeleteManifests(), matchingManifests);

      Iterable<CloseableIterable<ScanTask>> tasks =
          CloseableIterable.transform(
              matchingManifests,
              manifest ->
                  posDeletesScanTasks(
                      manifest, schemaString, transformedSpecs, residualCache, specStringCache));

      if (planExecutor() != null) {
        return new ParallelIterable<>(tasks, planExecutor());
      } else {
        return CloseableIterable.concat(tasks);
      }
    }

    private CloseableIterable<ScanTask> posDeletesScanTasks(
        ManifestFile manifest,
        String schemaString,
        Map<Integer, PartitionSpec> transformedSpecs,
        LoadingCache<Integer, ResidualEvaluator> residualCache,
        LoadingCache<Integer, String> specStringCache) {
      return new CloseableIterable<ScanTask>() {
        private CloseableIterable<ScanTask> iterable;

        @Override
        public void close() throws IOException {
          if (iterable != null) {
            iterable.close();
          }
        }

        @Override
        public CloseableIterator<ScanTask> iterator() {
          // Filter partitions
          CloseableIterable<ManifestEntry<DeleteFile>> deleteFileEntries =
              ManifestFiles.readDeleteManifest(manifest, table().io(), transformedSpecs)
                  .caseSensitive(isCaseSensitive())
                  .select(scanColumns())
                  .filterRows(filter())
                  .scanMetrics(scanMetrics())
                  .liveEntries();

          // Filter delete file type
          CloseableIterable<ManifestEntry<DeleteFile>> positionDeleteEntries =
              CloseableIterable.filter(
                  deleteFileEntries,
                  entry -> entry.file().content().equals(FileContent.POSITION_DELETES));

          this.iterable =
              CloseableIterable.transform(
                  positionDeleteEntries,
                  entry -> {
                    int specId = entry.file().specId();
                    return new BasePositionDeletesScanTask(
                        entry.file().copy(context().returnColumnStats()),
                        schemaString,
                        specStringCache.get(specId),
                        residualCache.get(specId));
                  });
          return iterable.iterator();
        }
      };
    }

    private <T> LoadingCache<Integer, T> partitionCacheOf(
        Map<Integer, PartitionSpec> specs, Function<PartitionSpec, T> constructor) {
      return Caffeine.newBuilder()
          .build(
              specId -> {
                PartitionSpec spec = specs.get(specId);
                return constructor.apply(spec);
              });
    }
  }
}
