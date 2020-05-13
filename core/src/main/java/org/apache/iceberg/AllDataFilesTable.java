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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.ParallelIterable;
import org.apache.iceberg.util.ThreadPools;

/**
 * A {@link Table} implementation that exposes a table's valid data files as rows.
 * <p>
 * A valid data file is one that is readable from any snapshot currently tracked by the table.
 * <p>
 * This table may return duplicate rows.
 */
public class AllDataFilesTable extends BaseMetadataTable {
  private final TableOperations ops;
  private final Table table;

  public AllDataFilesTable(TableOperations ops, Table table) {
    this.ops = ops;
    this.table = table;
  }

  @Override
  Table table() {
    return table;
  }

  @Override
  String metadataTableName() {
    return "all_data_files";
  }

  @Override
  public TableScan newScan() {
    return new AllDataFilesTableScan(ops, table, schema());
  }

  @Override
  public Schema schema() {
    Schema schema = new Schema(DataFile.getType(table.spec().partitionType()).fields());
    if (table.spec().fields().size() < 1) {
      // avoid returning an empty struct, which is not always supported. instead, drop the partition field (id 102)
      return TypeUtil.selectNot(schema, Sets.newHashSet(102));
    } else {
      return schema;
    }
  }

  public static class AllDataFilesTableScan extends BaseAllMetadataTableScan {
    private final Schema fileSchema;

    AllDataFilesTableScan(TableOperations ops, Table table, Schema fileSchema) {
      super(ops, table, fileSchema);
      this.fileSchema = fileSchema;
    }

    private AllDataFilesTableScan(
        TableOperations ops, Table table, Long snapshotId, Schema schema, Expression rowFilter,
        boolean caseSensitive, boolean colStats, Collection<String> selectedColumns, Schema fileSchema,
        ImmutableMap<String, String> options) {
      super(ops, table, snapshotId, schema, rowFilter, caseSensitive, colStats, selectedColumns, options);
      this.fileSchema = fileSchema;
    }

    @Override
    protected TableScan newRefinedScan(
        TableOperations ops, Table table, Long snapshotId, Schema schema, Expression rowFilter,
        boolean caseSensitive, boolean colStats, Collection<String> selectedColumns,
        ImmutableMap<String, String> options) {
      return new AllDataFilesTableScan(
          ops, table, snapshotId, schema, rowFilter, caseSensitive, colStats, selectedColumns, fileSchema, options);
    }

    @Override
    public TableScan useSnapshot(long scanSnapshotId) {
      throw new UnsupportedOperationException("Cannot select snapshot: all_data_files is for all snapshots");
    }

    @Override
    public TableScan asOfTime(long timestampMillis) {
      throw new UnsupportedOperationException("Cannot select snapshot: all_data_files is for all snapshots");
    }

    @Override
    protected long targetSplitSize(TableOperations ops) {
      return ops.current().propertyAsLong(
          TableProperties.METADATA_SPLIT_SIZE, TableProperties.METADATA_SPLIT_SIZE_DEFAULT);
    }

    @Override
    protected CloseableIterable<FileScanTask> planFiles(
        TableOperations ops, Snapshot snapshot, Expression rowFilter, boolean caseSensitive, boolean colStats) {
      CloseableIterable<ManifestFile> manifests = allManifestFiles(ops.current().snapshots());
      String schemaString = SchemaParser.toJson(schema());
      String specString = PartitionSpecParser.toJson(PartitionSpec.unpartitioned());
      ResidualEvaluator residuals = ResidualEvaluator.unpartitioned(rowFilter);

      // Data tasks produce the table schema, not the projection schema and projection is done by processing engines.
      // This data task needs to use the table schema, which may not include a partition schema to avoid having an
      // empty struct in the schema for unpartitioned tables. Some engines, like Spark, can't handle empty structs in
      // all cases.
      return CloseableIterable.transform(manifests, manifest ->
          new DataFilesTable.ManifestReadTask(ops.io(), manifest, fileSchema, schemaString, specString, residuals));
    }
  }

  static CloseableIterable<ManifestFile> allManifestFiles(List<Snapshot> snapshots) {
    try (CloseableIterable<ManifestFile> iterable = new ParallelIterable<>(
        Iterables.transform(snapshots, Snapshot::manifests), ThreadPools.getWorkerPool())) {
      return CloseableIterable.withNoopClose(Sets.newHashSet(iterable));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close parallel iterable");
    }
  }
}
