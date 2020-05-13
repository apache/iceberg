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
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.Collection;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;

/**
 * A {@link Table} implementation that exposes a table's valid manifest files as rows.
 * <p>
 * A valid manifest file is one that is referenced from any snapshot currently tracked by the table.
 * <p>
 * This table may return duplicate rows.
 */
public class AllManifestsTable extends BaseMetadataTable {
  private static final Schema MANIFEST_FILE_SCHEMA = new Schema(
      Types.NestedField.required(1, "path", Types.StringType.get()),
      Types.NestedField.required(2, "length", Types.LongType.get()),
      Types.NestedField.optional(3, "partition_spec_id", Types.IntegerType.get()),
      Types.NestedField.optional(4, "added_snapshot_id", Types.LongType.get()),
      Types.NestedField.optional(5, "added_data_files_count", Types.IntegerType.get()),
      Types.NestedField.optional(6, "existing_data_files_count", Types.IntegerType.get()),
      Types.NestedField.optional(7, "deleted_data_files_count", Types.IntegerType.get()),
      Types.NestedField.optional(8, "partition_summaries", Types.ListType.ofRequired(9, Types.StructType.of(
          Types.NestedField.required(10, "contains_null", Types.BooleanType.get()),
          Types.NestedField.optional(11, "lower_bound", Types.StringType.get()),
          Types.NestedField.optional(12, "upper_bound", Types.StringType.get())
      )))
  );

  private final TableOperations ops;
  private final Table table;

  public AllManifestsTable(TableOperations ops, Table table) {
    this.ops = ops;
    this.table = table;
  }

  @Override
  Table table() {
    return table;
  }

  @Override
  String metadataTableName() {
    return "all_manifests";
  }

  @Override
  public TableScan newScan() {
    return new AllManifestsTableScan(ops, table, MANIFEST_FILE_SCHEMA);
  }

  @Override
  public Schema schema() {
    return MANIFEST_FILE_SCHEMA;
  }

  public static class AllManifestsTableScan extends BaseAllMetadataTableScan {

    AllManifestsTableScan(TableOperations ops, Table table, Schema fileSchema) {
      super(ops, table, fileSchema);
    }

    private AllManifestsTableScan(
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
      return new AllManifestsTableScan(
          ops, table, snapshotId, schema, rowFilter, caseSensitive, colStats, selectedColumns, options);
    }

    @Override
    public TableScan useSnapshot(long scanSnapshotId) {
      throw new UnsupportedOperationException("Cannot select snapshot: all_manifests is for all snapshots");
    }

    @Override
    public TableScan asOfTime(long timestampMillis) {
      throw new UnsupportedOperationException("Cannot select snapshot: all_manifests is for all snapshots");
    }

    @Override
    protected long targetSplitSize(TableOperations ops) {
      return ops.current().propertyAsLong(
          TableProperties.METADATA_SPLIT_SIZE, TableProperties.METADATA_SPLIT_SIZE_DEFAULT);
    }

    @Override
    protected CloseableIterable<FileScanTask> planFiles(
        TableOperations ops, Snapshot snapshot, Expression rowFilter, boolean caseSensitive, boolean colStats) {
      String schemaString = SchemaParser.toJson(schema());
      String specString = PartitionSpecParser.toJson(PartitionSpec.unpartitioned());

      // Data tasks produce the table schema, not the projection schema and projection is done by processing engines.
      return CloseableIterable.withNoopClose(Iterables.transform(ops.current().snapshots(), snap -> {
        if (snap.manifestListLocation() != null) {
          return new ManifestListReadTask(ops.io(), table().spec(), new BaseFileScanTask(
              DataFiles.fromManifestList(ops.io().newInputFile(snap.manifestListLocation())),
              schemaString, specString, ResidualEvaluator.unpartitioned(rowFilter)));
        } else {
          return StaticDataTask.of(
              ops.io().newInputFile(ops.current().file().location()),
              snap.manifests(),
              manifest -> ManifestsTable.manifestFileToRow(table().spec(), manifest));
        }
      }));
    }
  }

  static class ManifestListReadTask implements DataTask {
    private final FileIO io;
    private final PartitionSpec spec;
    private final FileScanTask manifestListTask;

    ManifestListReadTask(FileIO io, PartitionSpec spec, FileScanTask manifestListTask) {
      this.io = io;
      this.spec = spec;
      this.manifestListTask = manifestListTask;
    }

    @Override
    public CloseableIterable<StructLike> rows() {
      try (CloseableIterable<ManifestFile> manifests = Avro
          .read(io.newInputFile(manifestListTask.file().path().toString()))
          .rename("manifest_file", GenericManifestFile.class.getName())
          .rename("partitions", GenericPartitionFieldSummary.class.getName())
          .rename("r508", GenericPartitionFieldSummary.class.getName())
          .project(ManifestFile.schema())
          .classLoader(GenericManifestFile.class.getClassLoader())
          .reuseContainers(false)
          .build()) {

        return CloseableIterable.transform(manifests,
            manifest -> ManifestsTable.manifestFileToRow(spec, manifest));

      } catch (IOException e) {
        throw new RuntimeIOException(e, "Cannot read manifest list file: %s", manifestListTask.file().path());
      }
    }

    @Override
    public DataFile file() {
      return manifestListTask.file();
    }

    @Override
    public PartitionSpec spec() {
      return manifestListTask.spec();
    }

    @Override
    public long start() {
      return 0;
    }

    @Override
    public long length() {
      return manifestListTask.length();
    }

    @Override
    public Expression residual() {
      return manifestListTask.residual();
    }

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
      return ImmutableList.of(this); // don't split
    }
  }
}
