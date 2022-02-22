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
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * A {@link Table} implementation that exposes a table's delete files as rows.
 */
public class DeleteFilesTable extends BaseFilesTable {

  DeleteFilesTable(TableOperations ops, Table table) {
    this(ops, table, table.name() + ".delete_files");
  }

  DeleteFilesTable(TableOperations ops, Table table, String name) {
    super(ops, table, name);
  }

  @Override
  public TableScan newScan() {
    return new DeleteFilesTableScan(operations(), table(), schema());
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.DELETE_FILES;
  }

  public static class DeleteFilesTableScan extends BaseFilesTableScan {
    private final Schema fileSchema;

    DeleteFilesTableScan(TableOperations ops, Table table, Schema fileSchema) {
      super(ops, table, fileSchema);
      this.fileSchema = fileSchema;
    }

    private DeleteFilesTableScan(TableOperations ops, Table table, Schema schema, Schema fileSchema,
                           TableScanContext context) {
      super(ops, table, schema, fileSchema, context);
      this.fileSchema = fileSchema;
    }

    @Override
    protected TableScan newRefinedScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
      return new DeleteFilesTableScan(ops, table, schema, fileSchema, context);
    }

    @Override
    protected CloseableIterable<FileScanTask> planFiles(
        TableOperations ops, Snapshot snapshot, Expression rowFilter,
        boolean ignoreResiduals, boolean caseSensitive, boolean colStats) {
      CloseableIterable<ManifestFile> filtered = filterManifests(snapshot.deleteManifests(), rowFilter, caseSensitive);

      String schemaString = SchemaParser.toJson(schema());
      String specString = PartitionSpecParser.toJson(PartitionSpec.unpartitioned());
      Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : rowFilter;
      ResidualEvaluator residuals = ResidualEvaluator.unpartitioned(filter);

      // Data tasks produce the table schema, not the projection schema and projection is done by processing engines.
      // This data task needs to use the table schema, which may not include a partition schema to avoid having an
      // empty struct in the schema for unpartitioned tables. Some engines, like Spark, can't handle empty structs in
      // all cases.
      return CloseableIterable.transform(filtered, manifest ->
          new DeleteManifestReadTask(ops.io(), ops.current().specsById(),
              manifest, schema(), schemaString, specString, residuals));
    }
  }

  static class DeleteManifestReadTask extends BaseFileScanTask implements DataTask {
    private final FileIO io;
    private final Map<Integer, PartitionSpec> specsById;
    private final ManifestFile manifest;
    private final Schema schema;

    DeleteManifestReadTask(FileIO io, Map<Integer, PartitionSpec> specsById, ManifestFile manifest,
                           Schema schema, String schemaString, String specString, ResidualEvaluator residuals) {
      super(DataFiles.fromManifest(manifest), null, schemaString, specString, residuals);
      this.io = io;
      this.specsById = specsById;
      this.manifest = manifest;
      this.schema = schema;
    }

    @Override
    public CloseableIterable<StructLike> rows() {
      return CloseableIterable.transform(
          ManifestFiles.readDeleteManifest(manifest, io, specsById).project(schema),
          file -> (GenericDeleteFile) file);
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
