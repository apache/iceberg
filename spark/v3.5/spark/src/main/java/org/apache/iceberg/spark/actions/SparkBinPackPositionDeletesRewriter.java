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
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.MetadataTableType.POSITION_DELETES;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.iceberg.DataFilesTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.SizeBasedPositionDeletesRewriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.PositionDeletesRewriteCoordinator;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkTableCache;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.SparkValueConverter;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;

class SparkBinPackPositionDeletesRewriter extends SizeBasedPositionDeletesRewriter {

  private final SparkSession spark;
  private final SparkTableCache tableCache = SparkTableCache.get();
  private final ScanTaskSetManager taskSetManager = ScanTaskSetManager.get();
  private final PositionDeletesRewriteCoordinator coordinator =
      PositionDeletesRewriteCoordinator.get();

  SparkBinPackPositionDeletesRewriter(SparkSession spark, Table table) {
    super(table);
    // Disable Adaptive Query Execution as this may change the output partitioning of our write
    this.spark = spark.cloneSession();
    this.spark.conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), false);
  }

  @Override
  public String description() {
    return "BIN-PACK";
  }

  @Override
  public Set<DeleteFile> rewrite(List<PositionDeletesScanTask> group) {
    String groupId = UUID.randomUUID().toString();
    Table deletesTable = MetadataTableUtils.createMetadataTableInstance(table(), POSITION_DELETES);
    try {
      tableCache.add(groupId, deletesTable);
      taskSetManager.stageTasks(deletesTable, groupId, group);

      doRewrite(groupId, group);

      return coordinator.fetchNewFiles(deletesTable, groupId);
    } finally {
      tableCache.remove(groupId);
      taskSetManager.removeTasks(deletesTable, groupId);
      coordinator.clearRewrite(deletesTable, groupId);
    }
  }

  protected void doRewrite(String groupId, List<PositionDeletesScanTask> group) {
    // all position deletes are of the same partition, because they are in same file group
    Preconditions.checkArgument(!group.isEmpty(), "Empty group");
    Types.StructType partitionType = group.get(0).spec().partitionType();
    StructLike partition = group.get(0).partition();

    // read the deletes packing them into splits of the required size
    Dataset<Row> posDeletes =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.SCAN_TASK_SET_ID, groupId)
            .option(SparkReadOptions.SPLIT_SIZE, splitSize(inputSize(group)))
            .option(SparkReadOptions.FILE_OPEN_COST, "0")
            .load(table().name());

    // keep only valid position deletes
    Dataset<Row> dataFiles = dataFiles(partitionType, partition);
    Column joinCond = posDeletes.col("file_path").equalTo(dataFiles.col("file_path"));
    Dataset<Row> validDeletes = posDeletes.join(dataFiles, joinCond, "leftsemi");

    // write the packed deletes into new files where each split becomes a new file
    validDeletes
        .sortWithinPartitions("file_path", "pos")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupId)
        .option(SparkWriteOptions.TARGET_DELETE_FILE_SIZE_BYTES, writeMaxFileSize())
        .mode("append")
        .save(table().name());
  }

  /** Returns entries of {@link DataFilesTable} of specified partition */
  private Dataset<Row> dataFiles(Types.StructType partitionType, StructLike partition) {
    List<Types.NestedField> fields = partitionType.fields();
    Optional<Column> condition =
        IntStream.range(0, fields.size())
            .mapToObj(
                i -> {
                  Type type = fields.get(i).type();
                  Object value = partition.get(i, type.typeId().javaClass());
                  Object convertedValue = SparkValueConverter.convertToSpark(type, value);
                  Column col = col("partition.`" + fields.get(i).name() + "`");
                  return col.eqNullSafe(lit(convertedValue));
                })
            .reduce(Column::and);
    if (condition.isPresent()) {
      return SparkTableUtil.loadMetadataTable(spark, table(), MetadataTableType.DATA_FILES)
          .filter(condition.get());
    } else {
      return SparkTableUtil.loadMetadataTable(spark, table(), MetadataTableType.DATA_FILES);
    }
  }
}
