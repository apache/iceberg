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
import java.util.Map;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.StructType;

/** Base class logic for files metadata tables */
abstract class BaseFilesTable extends BaseMetadataTable {

  BaseFilesTable(TableOperations ops, Table table, String name) {
    super(ops, table, name);
  }

  @Override
  public Schema schema() {
    StructType partitionType = Partitioning.partitionType(table());
    Schema schema = new Schema(DataFile.getType(partitionType).fields());
    if (partitionType.fields().size() < 1) {
      // avoid returning an empty struct, which is not always supported. instead, drop the partition
      // field
      return TypeUtil.selectNot(schema, Sets.newHashSet(DataFile.PARTITION_ID));
    } else {
      return schema;
    }
  }

  private static CloseableIterable<FileScanTask> planFiles(
      Table table,
      CloseableIterable<ManifestFile> manifests,
      Schema tableSchema,
      Schema projectedSchema,
      TableScanContext context) {
    Expression rowFilter = context.rowFilter();
    boolean caseSensitive = context.caseSensitive();
    boolean ignoreResiduals = context.ignoreResiduals();

    LoadingCache<Integer, ManifestEvaluator> evalCache =
        Caffeine.newBuilder()
            .build(
                specId -> {
                  PartitionSpec spec = table.specs().get(specId);
                  PartitionSpec transformedSpec = BaseFilesTable.transformSpec(tableSchema, spec);
                  return ManifestEvaluator.forRowFilter(rowFilter, transformedSpec, caseSensitive);
                });

    CloseableIterable<ManifestFile> filteredManifests =
        CloseableIterable.filter(
            manifests, manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest));

    String schemaString = SchemaParser.toJson(projectedSchema);
    String specString = PartitionSpecParser.toJson(PartitionSpec.unpartitioned());
    Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : rowFilter;
    ResidualEvaluator residuals = ResidualEvaluator.unpartitioned(filter);

    return CloseableIterable.transform(
        filteredManifests,
        manifest ->
            new ManifestReadTask(
                table, manifest, projectedSchema, schemaString, specString, residuals));
  }

  abstract static class BaseFilesTableScan extends BaseMetadataTableScan {

    protected BaseFilesTableScan(
        TableOperations ops, Table table, Schema schema, MetadataTableType tableType) {
      super(ops, table, schema, tableType);
    }

    protected BaseFilesTableScan(
        TableOperations ops,
        Table table,
        Schema schema,
        MetadataTableType tableType,
        TableScanContext context) {
      super(ops, table, schema, tableType, context);
    }

    /** Returns an iterable of manifest files to explore for this files metadata table scan */
    protected abstract CloseableIterable<ManifestFile> manifests();

    @Override
    protected CloseableIterable<FileScanTask> doPlanFiles() {
      return BaseFilesTable.planFiles(table(), manifests(), tableSchema(), schema(), context());
    }
  }

  abstract static class BaseAllFilesTableScan extends BaseAllMetadataTableScan {

    protected BaseAllFilesTableScan(
        TableOperations ops, Table table, Schema schema, MetadataTableType tableType) {
      super(ops, table, schema, tableType);
    }

    protected BaseAllFilesTableScan(
        TableOperations ops,
        Table table,
        Schema schema,
        MetadataTableType tableType,
        TableScanContext context) {
      super(ops, table, schema, tableType, context);
    }

    /** Returns an iterable of manifest files to explore for this all files metadata table scan */
    protected abstract CloseableIterable<ManifestFile> manifests();

    @Override
    protected CloseableIterable<FileScanTask> doPlanFiles() {
      return BaseFilesTable.planFiles(table(), manifests(), tableSchema(), schema(), context());
    }
  }

  static class ManifestReadTask extends BaseFileScanTask implements DataTask {
    private final FileIO io;
    private final Map<Integer, PartitionSpec> specsById;
    private final ManifestFile manifest;
    private final Schema schema;

    ManifestReadTask(
        Table table,
        ManifestFile manifest,
        Schema schema,
        String schemaString,
        String specString,
        ResidualEvaluator residuals) {
      super(DataFiles.fromManifest(manifest), null, schemaString, specString, residuals);
      this.io = table.io();
      this.specsById = Maps.newHashMap(table.specs());
      this.manifest = manifest;
      this.schema = schema;
    }

    @Override
    public CloseableIterable<StructLike> rows() {
      return CloseableIterable.transform(manifestEntries(), file -> (StructLike) file);
    }

    private CloseableIterable<? extends ContentFile<?>> manifestEntries() {
      switch (manifest.content()) {
        case DATA:
          return ManifestFiles.read(manifest, io, specsById).project(schema);
        case DELETES:
          return ManifestFiles.readDeleteManifest(manifest, io, specsById).project(schema);
        default:
          throw new IllegalArgumentException(
              "Unsupported manifest content type:" + manifest.content());
      }
    }

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
      return ImmutableList.of(this); // don't split
    }

    @VisibleForTesting
    ManifestFile manifest() {
      return manifest;
    }
  }
}
