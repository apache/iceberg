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

import static org.apache.iceberg.MetadataColumns.POSITION_DELETE_TABLE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.POSITION_DELETE_TABLE_PARTITION_FIELD_ID;
import static org.apache.iceberg.MetadataColumns.POSITION_DELETE_TABLE_SPEC_ID;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.ParallelIterable;
import org.apache.iceberg.util.PartitionUtil;

public class PositionDeletesTable extends BaseMetadataTable {

  PositionDeletesTable(TableOperations ops, Table table) {
    super(ops, table, table.name() + ".position_deletes");
  }

  PositionDeletesTable(TableOperations ops, Table table, String name) {
    super(ops, table, name);
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.POSITION_DELETES;
  }

  @Override
  public TableScan newScan() {
    return new PositionDeletesTableScan(operations(), table(), schema());
  }

  @Override
  public Schema schema() {
    return PositionDeletesTable.schema(table(), Partitioning.partitionType(table()));
  }

  public static Schema schema(Table table, Types.StructType partitionType) {
    Schema result =
        new Schema(
            MetadataColumns.DELETE_FILE_PATH,
            MetadataColumns.DELETE_FILE_POS,
            Types.NestedField.optional(
                MetadataColumns.DELETE_FILE_ROW_FIELD_ID,
                "row",
                table.schema().asStruct(),
                MetadataColumns.DELETE_FILE_ROW_DOC),
            Types.NestedField.required(
                POSITION_DELETE_TABLE_PARTITION_FIELD_ID,
                "partition",
                partitionType,
                "Partition that position delete row belongs to"),
            Types.NestedField.required(
                POSITION_DELETE_TABLE_SPEC_ID,
                "spec_id",
                Types.IntegerType.get(),
                "Spec ID of the file that the position delete row belongs to"),
            Types.NestedField.required(
                POSITION_DELETE_TABLE_FILE_PATH,
                "delete_file_path",
                Types.StringType.get(),
                "Spec ID of the file that the position delete row belongs to"));

    if (partitionType.fields().size() > 0) {
      return result;
    } else {
      // avoid returning an empty struct, which is not always supported. instead, drop the partition
      // field
      return TypeUtil.selectNot(result, Sets.newHashSet(POSITION_DELETE_TABLE_PARTITION_FIELD_ID));
    }
  }

  public class PositionDeletesTableScan extends BaseMetadataTableScan {

    PositionDeletesTableScan(TableOperations ops, Table table, Schema schema) {
      super(ops, table, schema, MetadataTableType.POSITION_DELETES);
    }

    PositionDeletesTableScan(
        TableOperations ops, Table table, Schema schema, TableScanContext context) {
      super(ops, table, schema, MetadataTableType.POSITION_DELETES, context);
    }

    @Override
    protected TableScan newRefinedScan(
        TableOperations ops, Table table, Schema schema, TableScanContext context) {
      return new PositionDeletesTableScan(ops, table, schema, context);
    }

    @Override
    protected CloseableIterable<FileScanTask> doPlanFiles() {
      Expression rowFilter = context().rowFilter();
      String schemaString = SchemaParser.toJson(tableSchema());

      Map<Integer, PartitionSpec> transformedSpecs =
          table().specs().entrySet().stream()
              .map(
                  e ->
                      Pair.of(
                          e.getKey(), BaseMetadataTable.transformSpec(tableSchema(), e.getValue())))
              .collect(Collectors.toMap(Pair::first, Pair::second));

      CloseableIterable<ManifestFile> deleteManifests =
          CloseableIterable.withNoopClose(snapshot().deleteManifests(tableOps().io()));
      CloseableIterable<CloseableIterable<FileScanTask>> results =
          CloseableIterable.transform(
              deleteManifests,
              m -> {

                // Filter partitions
                CloseableIterable<ManifestEntry<DeleteFile>> deleteFileEntries =
                    ManifestFiles.readDeleteManifest(m, tableOps().io(), transformedSpecs)
                        .caseSensitive(isCaseSensitive())
                        .filterRows(rowFilter)
                        .liveEntries();

                // Filter delete file type
                CloseableIterable<ManifestEntry<DeleteFile>> positionDeleteEntries =
                    CloseableIterable.filter(
                        deleteFileEntries,
                        entry -> entry.file().content().equals(FileContent.POSITION_DELETES));

                Types.StructType partitionType = Partitioning.partitionType(table());

                return CloseableIterable.transform(
                    positionDeleteEntries,
                    entry -> {
                      PartitionSpec spec = transformedSpecs.get(entry.file().specId());
                      FileScanTask delegate =
                          new BaseFileScanTask(
                              DataFiles.fromDeleteFile(entry.file(), spec),
                              null, /* Deletes */
                              schemaString,
                              PartitionSpecParser.toJson(spec),
                              // TODO select relevant filters for residuals
                              ResidualEvaluator.of(
                                  spec, Expressions.alwaysTrue(), context().caseSensitive()));
                      return new PositionDeletesFileScanTask(
                          delegate, partitionType, entry.file().partition(), entry.file().specId());
                    });
              });

      return new ParallelIterable<>(results, planExecutor());
    }
  }

  public static class PositionDeletesFileScanTask implements FileScanTask {

    private final FileScanTask delegate;
    private final Types.StructType partitionType;
    private final StructLike partition;
    private final int specId;

    /**
     * Marker file scan task class for scan of position deletes metadata table
     *
     * @param delegate delegate file scan task
     * @param partitionType table's PartitionType (from {@link
     *     org.apache.iceberg.Partitioning#partitionType(Table)}
     * @param partition partition data, of this file scan task
     * @param specId spec id associated with partition, for this file scan task
     */
    public PositionDeletesFileScanTask(
        FileScanTask delegate, Types.StructType partitionType, StructLike partition, int specId) {
      this.delegate = delegate;
      this.partitionType = partitionType;
      this.partition = partition;
      this.specId = specId;
    }

    @Override
    public DataFile file() {
      return delegate.file();
    }

    @Override
    public PartitionSpec spec() {
      return delegate.spec();
    }

    @Override
    public long start() {
      return delegate.start();
    }

    @Override
    public long length() {
      return delegate.length();
    }

    @Override
    public Expression residual() {
      return delegate.residual();
    }

    @Override
    public List<DeleteFile> deletes() {
      return delegate.deletes();
    }

    @Override
    public Iterable<FileScanTask> split(long targetSplitSize) {
      return Iterables.transform(
          delegate.split(targetSplitSize),
          task -> new PositionDeletesFileScanTask(delegate, partitionType, partition, specId));
    }

    /**
     * Utility method to get constant values to populate for rows to be scanned by this task. The
     * following columns are constants of each PositionDeletesFileScanTask and are not read directly
     * from the position delete file: 1. spec_id 2. partition 3. delete_file_path
     *
     * @param convertConstant callback to convert from an Iceberg typed object to an engine specific
     *     object
     * @return a map of column id to constant values returned by this task type
     */
    public Map<Integer, ?> constantsMap(BiFunction<Type, Object, Object> convertConstant) {
      Map<Integer, Object> idToConstant = Maps.newHashMap();
      StructLike partitionData = file().partition();

      idToConstant.put(
          POSITION_DELETE_TABLE_FILE_PATH,
          convertConstant.apply(Types.StringType.get(), file().path()));

      // add spec_id constant column
      idToConstant.put(
          POSITION_DELETE_TABLE_SPEC_ID,
          convertConstant.apply(Types.IntegerType.get(), file().specId()));

      // add partition constant column
      if (partitionType != null) {
        if (partitionType.fields().size() > 0) {
          StructLike coercedPartition =
              PartitionUtil.coercePartition(partitionType, spec(), partitionData);
          idToConstant.put(
              POSITION_DELETE_TABLE_PARTITION_FIELD_ID,
              convertConstant.apply(partitionType, coercedPartition));
        } else {
          // use null as some query engines may not be able to handle empty structs
          idToConstant.put(POSITION_DELETE_TABLE_PARTITION_FIELD_ID, null);
        }
      }

      return idToConstant;
    }
  }
}
