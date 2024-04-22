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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.min;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RemoveDanglingDeleteFiles;
import org.apache.iceberg.actions.RemoveDanglingDeleteFilesActionResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.SparkDeleteFile;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An action that removes dangling delete files from the current snapshot. A delete file is dangling
 * if its deletes no longer applies to any non-expired data file.
 *
 * <p>The following dangling delete files are removed:
 *
 * <ul>
 *   <li>Position delete files with a data sequence number less than that of any data file in the
 *       same partition
 *   <li>Equality delete files with a data sequence number less than or equal to that of any data
 *       file in the same partition
 * </ul>
 */
class RemoveDanglingDeletesSparkAction
    extends BaseSnapshotUpdateSparkAction<RemoveDanglingDeletesSparkAction>
    implements RemoveDanglingDeleteFiles {

  private static final Logger LOG = LoggerFactory.getLogger(RemoveDanglingDeletesSparkAction.class);
  private final Table table;

  protected RemoveDanglingDeletesSparkAction(SparkSession spark, Table table) {
    super(spark.cloneSession());
    this.table = table;
  }

  @Override
  protected RemoveDanglingDeletesSparkAction self() {
    return this;
  }

  public Result execute() {
    if (table.specs().size() == 1 && table.spec().isUnpartitioned()) {
      // ManifestFilterManager already performs this table-wide on each commit
      return new RemoveDanglingDeleteFilesActionResult(Collections.emptyList());
    }

    String desc = String.format("Removing dangling delete in %s", table.name());
    JobGroupInfo info = newJobGroupInfo("REMOVE-DELETES", desc);
    return withJobGroupInfo(info, this::doExecute);
  }

  Result doExecute() {
    RewriteFiles rewriteFiles = table.newRewrite();
    List<DeleteFile> danglingDeletes = findDanglingDeletes();
    for (DeleteFile deleteFile : danglingDeletes) {
      LOG.info("Removing dangling delete file {}", deleteFile.path());
      rewriteFiles.deleteFile(deleteFile);
    }

    if (!danglingDeletes.isEmpty()) {
      commit(rewriteFiles);
    }

    return new RemoveDanglingDeleteFilesActionResult(danglingDeletes);
  }

  /**
   * Dangling delete files can be identified with following steps
   *
   * <p>1. Query data_files to group by partition spec ID and partition value aggregate to compute
   * min data sequence number per group
   *
   * <p>2. Left join delete files table on grouped partition spec ID and partition value to account
   * for partition evolution
   *
   * <p>3. Filter to identify delete files that can be discarded by comparing its data sequence
   * number having single predicate to account for both position and equality delete
   */
  private List<DeleteFile> findDanglingDeletes() {
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
            // dangling position delete files
            .or(
                col("content")
                    .equalTo("1")
                    .and(col("data_sequence_number").$less(col("min_data_sequence_number"))))
            // dangling equality delete files
            .or(
                col("content")
                    .equalTo("2")
                    .and(col("data_sequence_number").$less$eq(col("min_data_sequence_number"))));

    Dataset<Row> danglingDeletes =
        deletes.join(minSequenceNumberByPartition, joinCond, "left").filter(filterCondition);
    // can we reuse wrapper without run into duplicate
    SparkDeleteFile wrapper = deleteFileWrapper(deletes.schema());
    return danglingDeletes.collectAsList().stream()
        .map(row -> deleteFileWrapper(deletes.schema()).wrap(row))
        .collect(Collectors.toList());
  }

  private SparkDeleteFile deleteFileWrapper(StructType sparkFileType) {
    Types.StructType combinedFileType = DataFile.getType(Partitioning.partitionType(table));
    Types.StructType fileType = DataFile.getType(table.spec().partitionType());
    return new SparkDeleteFile(combinedFileType, fileType, sparkFileType);
  }
}
