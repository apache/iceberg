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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.Collection;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.TypeUtil;

/**
 * A {@link Table} implementation that exposes a table's manifest entries as rows.
 * <p>
 * WARNING: this table exposes internal details, like files that have been deleted. For a table of the live data files,
 * use {@link DataFilesTable}.
 */
public class ManifestEntriesTable extends BaseMetadataTable {
  private final TableOperations ops;
  private final Table table;

  public ManifestEntriesTable(TableOperations ops, Table table) {
    this.ops = ops;
    this.table = table;
  }

  @Override
  Table table() {
    return table;
  }

  @Override
  String metadataTableName() {
    return "entries";
  }

  @Override
  public TableScan newScan() {
    return new EntriesTableScan(ops, table, schema());
  }

  @Override
  public Schema schema() {
    Schema schema = ManifestEntry.getSchema(table.spec().partitionType());
    if (table.spec().fields().size() < 1) {
      // avoid returning an empty struct, which is not always supported. instead, drop the partition field (id 102)
      return TypeUtil.selectNot(schema, Sets.newHashSet(102));
    } else {
      return schema;
    }
  }

  private static class EntriesTableScan extends BaseTableScan {

    EntriesTableScan(TableOperations ops, Table table, Schema schema) {
      super(ops, table, schema);
    }

    private EntriesTableScan(
        TableOperations ops, Table table, Long snapshotId, Schema schema, Expression rowFilter,
        boolean caseSensitive, boolean colStats, Collection<String> selectedColumns,
        ImmutableMap<String, String> options) {
      super(ops, table, snapshotId, schema, rowFilter, caseSensitive, colStats, selectedColumns, options);
    }

    @Override
    protected TableScan newRefinedScan(
        TableOperations ops, Table table, Long snapshotId, Schema schema, Expression rowFilter,
        boolean caseSensitive, boolean colStats, Collection<String> selectedColumns,
        ImmutableMap<String, String> options) {
      return new EntriesTableScan(
          ops, table, snapshotId, schema, rowFilter, caseSensitive, colStats, selectedColumns, options);
    }

    @Override
    protected long targetSplitSize(TableOperations ops) {
      return ops.current().propertyAsLong(
          TableProperties.METADATA_SPLIT_SIZE, TableProperties.METADATA_SPLIT_SIZE_DEFAULT);
    }

    @Override
    protected CloseableIterable<FileScanTask> planFiles(
        TableOperations ops, Snapshot snapshot, Expression rowFilter, boolean caseSensitive, boolean colStats) {
      CloseableIterable<ManifestFile> manifests = CloseableIterable.withNoopClose(snapshot.manifests());
      Schema fileSchema = new Schema(schema().findType("data_file").asStructType().fields());
      String schemaString = SchemaParser.toJson(schema());
      String specString = PartitionSpecParser.toJson(PartitionSpec.unpartitioned());
      ResidualEvaluator residuals = ResidualEvaluator.unpartitioned(rowFilter);

      return CloseableIterable.transform(manifests, manifest ->
          new ManifestReadTask(ops.io(), manifest, fileSchema, schemaString, specString, residuals));
    }
  }

  static class ManifestReadTask extends BaseFileScanTask implements DataTask {
    private final Schema fileSchema;
    private final FileIO io;
    private final ManifestFile manifest;

    ManifestReadTask(FileIO io, ManifestFile manifest, Schema fileSchema, String schemaString,
                     String specString, ResidualEvaluator residuals) {
      super(DataFiles.fromManifest(manifest), schemaString, specString, residuals);
      this.fileSchema = fileSchema;
      this.io = io;
      this.manifest = manifest;
    }

    @Override
    public CloseableIterable<StructLike> rows() {
      return CloseableIterable.transform(
          ManifestFiles.read(manifest, io).project(fileSchema).entries(),
          file -> (GenericManifestEntry) file);
    }

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
      return ImmutableList.of(this); // don't split
    }
  }
}
