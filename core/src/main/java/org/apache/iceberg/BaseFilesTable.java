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

import java.util.Map;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.StructType;

/**
 * Base class logic for files metadata tables
 */
abstract class BaseFilesTable extends BaseMetadataTable {

  BaseFilesTable(TableOperations ops, Table table, String name) {
    super(ops, table, name);
  }

  @Override
  public Schema schema() {
    StructType partitionType = Partitioning.partitionType(table());
    Schema schema = new Schema(DataFile.getType(partitionType).fields());
    if (partitionType.fields().size() < 1) {
      // avoid returning an empty struct, which is not always supported. instead, drop the partition field
      return TypeUtil.selectNot(schema, Sets.newHashSet(DataFile.PARTITION_ID));
    } else {
      return schema;
    }
  }

  abstract static class BaseFilesTableScan extends BaseMetadataTableScan {

    private final Schema fileSchema;
    private final MetadataTableType type;

    protected BaseFilesTableScan(TableOperations ops, Table table, Schema fileSchema, MetadataTableType type) {
      super(ops, table, fileSchema);
      this.fileSchema = fileSchema;
      this.type = type;
    }

    protected BaseFilesTableScan(TableOperations ops, Table table, Schema schema, Schema fileSchema,
                                 TableScanContext context, MetadataTableType type) {
      super(ops, table, schema, context);
      this.fileSchema = fileSchema;
      this.type = type;
    }

    protected Schema fileSchema() {
      return fileSchema;
    }

    @Override
    public TableScan appendsBetween(long fromSnapshotId, long toSnapshotId) {
      throw new UnsupportedOperationException(
          String.format("Cannot incrementally scan table of type %s", type.name()));
    }

    @Override
    public TableScan appendsAfter(long fromSnapshotId) {
      throw new UnsupportedOperationException(
          String.format("Cannot incrementally scan table of type %s", type.name()));
    }

    @Override
    protected CloseableIterable<FileScanTask> planFiles(TableOperations ops, Snapshot snapshot, Expression rowFilter,
                                                        boolean ignoreResiduals, boolean caseSensitive,
                                                        boolean colStats) {
      CloseableIterable<ManifestFile> filtered = filterManifests(manifests(), rowFilter, caseSensitive);

      String schemaString = SchemaParser.toJson(schema());
      String specString = PartitionSpecParser.toJson(PartitionSpec.unpartitioned());
      Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : rowFilter;
      ResidualEvaluator residuals = ResidualEvaluator.unpartitioned(filter);

      // Data tasks produce the table schema, not the projection schema and projection is done by processing engines.
      // This data task needs to use the table schema, which may not include a partition schema to avoid having an
      // empty struct in the schema for unpartitioned tables. Some engines, like Spark, can't handle empty structs in
      // all cases.
      return CloseableIterable.transform(filtered, manifest ->
          new ManifestReadTask(ops.io(), ops.current().specsById(),
              manifest, schema(), schemaString, specString, residuals));
    }

    /**
     * Returns list of {@link ManifestFile} files to explore for this file's metadata table scan
     */
    protected abstract CloseableIterable<ManifestFile> manifests();

    private CloseableIterable<ManifestFile> filterManifests(CloseableIterable<ManifestFile> manifests,
                                                            Expression rowFilter,
                                                            boolean caseSensitive) {
      // use an inclusive projection to remove the partition name prefix and filter out any non-partition expressions
      PartitionSpec spec = transformSpec(fileSchema, table().spec(), PARTITION_FIELD_PREFIX);
      Expression partitionFilter = Projections.inclusive(spec, caseSensitive).project(rowFilter);

      ManifestEvaluator manifestEval = ManifestEvaluator.forPartitionFilter(
          partitionFilter, table().spec(), caseSensitive);

      return CloseableIterable.filter(manifests, manifestEval::eval);
    }
  }

  static class ManifestReadTask extends BaseFileScanTask implements DataTask {
    private final FileIO io;
    private final Map<Integer, PartitionSpec> specsById;
    private final ManifestFile manifest;
    private final Schema schema;

    ManifestReadTask(FileIO io, Map<Integer, PartitionSpec> specsById, ManifestFile manifest,
                     Schema schema, String schemaString, String specString, ResidualEvaluator residuals) {
      super(DataFiles.fromManifest(manifest), null, schemaString, specString, residuals);
      this.io = io;
      this.specsById = specsById;
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
          throw new IllegalArgumentException("Unsupported manifest content type:" + manifest.content());
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
