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

import static org.apache.iceberg.actions.RewriteDataFiles.REMOVE_DANGLING_DELETES;
import static org.apache.iceberg.actions.RewriteDataFiles.REMOVE_DANGLING_DELETES_DEFAULT;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.min;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.RemoveDanglingDeletesMode;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * I'd consider the following algorithm:
 *
 * <p>1. Extend data_files and delete_files metadata tables to include data sequence numbers, if
 * needed. I don't remember if we already populate them. This should be trivial as each DeleteFile
 * object already has this info.
 *
 * <p>1. Query data_files, aggregate, compute min data sequence number per partition. Don't cache
 * the computed result, just keep a reference to it.
 *
 * <p>1. Query delete_files, potentially projecting only strictly required columns.
 *
 * <p>1. Join the summary with delete_files on the spec ID and partition. Find delete files that can
 * be discarded in one go by having a predicate that accounts for the delete type (position vs
 * equality).
 *
 * <p>1. Collect the result to the driver and use SparkDeleteFile to wrap Spark rows as valid delete
 * files. See the action for rewriting manifests for an example.
 */
class RemoveDanglingDeleteSparkAction
    extends BaseSnapshotUpdateSparkAction<RemoveDanglingDeleteSparkAction> {

  private static final Logger LOG = LoggerFactory.getLogger(RewriteDataFilesSparkAction.class);
  private final Table table;

  private boolean removeDanglingDeletes = REMOVE_DANGLING_DELETES_DEFAULT;

  protected RemoveDanglingDeleteSparkAction(SparkSession spark, Table table) {
    super(spark.cloneSession());
    this.table = table;
  }

  @Override
  protected RemoveDanglingDeleteSparkAction self() {
    return this;
  }

  public List<DeleteFile> execute() {
    List<DeleteFile> result = Lists.newArrayList();

    removeDanglingDeletes =
        PropertyUtil.propertyAsBoolean(
            options(), REMOVE_DANGLING_DELETES, REMOVE_DANGLING_DELETES_DEFAULT);

    if (removeDanglingDeletes) {
      result.addAll(removeDanglingDeletes());
    }
    return result;
  }

  private List<DeleteFile> removeDanglingDeletes() {
    if (table.specs().size() == 1 && table.spec().isUnpartitioned()) {
      // ManifestFilterManager already performs this table-wide on each commit
      return Collections.emptyList();
    }

    RewriteFiles rewriteFiles = table.newRewrite();
    List<DeleteFile> toRemove = Lists.newArrayList();
    LOG.info("Evaluating dangling delete files for {}", table.name());

    Dataset<Row> minSequenceNumberByPartition =
        loadMetadataTable(table, MetadataTableType.DATA_FILES)
            .selectExpr(
                "partition",
                "spec_id",
                "file_path",
                "file_size_in_bytes",
                "record_count",
                "data_sequence_number")
            .groupBy("partition", "spec_id")
            .agg(min("data_sequence_number"))
            .toDF("grouped_partition", "grouped_spec_id", "min_data_sequence_number");

    Dataset<Row> deletes = loadMetadataTable(table, MetadataTableType.DELETE_FILES);

    Column joinCond =
        deletes
            .col("spec_id")
            .equalTo(minSequenceNumberByPartition.col("grouped_spec_id"))
            .and(
                deletes
                    .col("partition")
                    .equalTo(minSequenceNumberByPartition.col("grouped_partition")));

    Column filterCondition =
        col("min_data_sequence_number")
            .isNull()
            .or(
                col("content")
                    .equalTo("1")
                    .and(col("data_sequence_number").$less(col("min_data_sequence_number"))))
            .or(
                col("content")
                    .equalTo("2")
                    .and(col("data_sequence_number").$less$eq(col("min_data_sequence_number"))));

    Dataset<Row> danglingDeletes =
        deletes
            .join(minSequenceNumberByPartition, joinCond, "left")
            .filter(filterCondition)
            .select("partition", "spec_id", "file_path", "file_size_in_bytes", "record_count");

    // TODO, use SparkDelete to serialized row to POJO
    MakeDeleteFile makePosDeleteFn =
        new MakeDeleteFile(true, Partitioning.partitionType(table), table.specs());
    Dataset<DeleteFile> danglingDeleteFiles =
        danglingDeletes.map(makePosDeleteFn, Encoders.javaSerialization(DeleteFile.class));
    toRemove.addAll(danglingDeleteFiles.collectAsList());

    for (DeleteFile f : toRemove) {
      LOG.debug("Removing dangling delete file {}", f.path());
      toRemove.forEach(rewriteFiles::deleteFile);
    }

    if (!toRemove.isEmpty()) {
      commit(rewriteFiles);
    }

    return toRemove;
  }

  private static class MakeDeleteFile implements MapFunction<Row, DeleteFile> {

    private final boolean posDeletes;
    private final Types.StructType partitionType;
    private final Map<Integer, PartitionSpec> specsById;

    /**
     * Map function that transforms entries table rows into {@link DeleteFile}
     *
     * @param posDeletes true for position deletes, false for equality deletes
     * @param partitionType partition type of table
     * @param specsById table's partition specs
     */
    MakeDeleteFile(
        boolean posDeletes, Types.StructType partitionType, Map<Integer, PartitionSpec> specsById) {
      this.posDeletes = posDeletes;
      this.partitionType = partitionType;
      this.specsById = specsById;
    }

    @Override
    public DeleteFile call(Row row) throws Exception {
      PartitionData partition = new PartitionData(partitionType);
      GenericRowWithSchema partitionRow = row.getAs(0);

      for (int i = 0; i < partitionRow.length(); i++) {
        partition.set(i, partitionRow.get(i));
      }

      int specId = row.getAs(1);
      String path = row.getAs(2);
      long fileSize = row.getAs(3);
      long recordCount = row.getAs(4);

      FileMetadata.Builder builder = FileMetadata.deleteFileBuilder(specsById.get(specId));
      if (posDeletes) {
        builder.ofPositionDeletes();
      } else {
        builder.ofEqualityDeletes();
      }

      return builder
          .withPath(path)
          .withPartition(partition)
          .withFileSizeInBytes(fileSize)
          .withRecordCount(recordCount)
          .build();
    }
  }
}
