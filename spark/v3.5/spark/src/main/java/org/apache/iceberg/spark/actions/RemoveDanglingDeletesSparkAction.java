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
import org.apache.iceberg.actions.ImmutableRemoveDanglingDeleteFiles;
import org.apache.iceberg.actions.RemoveDanglingDeleteFiles;
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
 * if its deletes no longer applies to any live data files.
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
      // ManifestFilterManager already performs this table-wide delete on each commit
      return ImmutableRemoveDanglingDeleteFiles.Result.builder()
          .removedDeleteFiles(Collections.emptyList())
          .build();
    }

    String desc = String.format("Removing dangling delete in %s", table.name());
    JobGroupInfo info = newJobGroupInfo("REMOVE-DELETES", desc);
    return withJobGroupInfo(info, this::doExecute);
  }

  Result doExecute() {
    RewriteFiles rewriteFiles = table.newRewrite();
    List<DeleteFile> danglingDeletes = findDanglingDeletes();
    for (DeleteFile deleteFile : danglingDeletes) {
      LOG.debug("Removing dangling delete file {}", deleteFile.path());
      rewriteFiles.deleteFile(deleteFile);
    }

    if (!danglingDeletes.isEmpty()) {
      commit(rewriteFiles);
    }

    return ImmutableRemoveDanglingDeleteFiles.Result.builder()
        .removedDeleteFiles(danglingDeletes)
        .build();
  }

  /**
   * Dangling delete files can be identified with following steps
   *
   * <p>1.Group data files by partition keys and find the minimum data sequence number in each
   * group.
   *
   * <p>2. Left outer join delete files with partition-grouped data files on partition keys.
   *
   * <p>3. Filter results to find dangling delete files by comparing delete file sequence_number to
   * its partitions' minimum data sequence number.
   *
   * <p>4. Collect results row to driver and use {@link SparkDeleteFile SparkDeleteFile} to wrap
   * rows to valid delete files
   */
  private List<DeleteFile> findDanglingDeletes() {
    Dataset<Row> minSequenceNumberByPartition =
        loadMetadataTable(table, MetadataTableType.ENTRIES)
            // find live entry for data files
            .filter("data_file.content == 0 AND status < 2")
            .selectExpr(
                "data_file.partition as partition",
                "data_file.spec_id as spec_id",
                "sequence_number")
            .groupBy("partition", "spec_id")
            .agg(min("sequence_number"))
            .toDF("grouped_partition", "grouped_spec_id", "min_data_sequence_number");

    Dataset<Row> deleteEntries =
        loadMetadataTable(table, MetadataTableType.ENTRIES)
            // find live entry for delete files
            .filter(" data_file.content != 0 AND status < 2");

    Column joinOnPartition =
        deleteEntries
            .col("data_file.spec_id")
            .equalTo(minSequenceNumberByPartition.col("grouped_spec_id"))
            .and(
                deleteEntries
                    .col("data_file.partition")
                    .equalTo(minSequenceNumberByPartition.col("grouped_partition")));

    Column filterOnDanglingDeletes =
        col("min_data_sequence_number")
            // when all data files are rewritten with new partition spec
            .isNull()
            // dangling position delete files
            .or(
                col("data_file.content")
                    .equalTo("1")
                    .and(col("sequence_number").$less(col("min_data_sequence_number"))))
            // dangling equality delete files
            .or(
                col("data_file.content")
                    .equalTo("2")
                    .and(col("sequence_number").$less$eq(col("min_data_sequence_number"))));

    Dataset<Row> danglingDeletes =
        deleteEntries
            .join(minSequenceNumberByPartition, joinOnPartition, "left")
            .filter(filterOnDanglingDeletes)
            .select("data_file.*");
    return danglingDeletes.collectAsList().stream()
        .map(row -> deleteFileWrapper(danglingDeletes.schema(), row))
        .collect(Collectors.toList());
  }

  private DeleteFile deleteFileWrapper(StructType sparkFileType, Row row) {
    int specId = row.getInt(row.fieldIndex("spec_id"));
    Types.StructType combinedFileType = DataFile.getType(Partitioning.partitionType(table));
    // Set correct spec id
    Types.StructType projection = DataFile.getType(table.specs().get(specId).partitionType());
    return new SparkDeleteFile(combinedFileType, projection, sparkFileType).wrap(row);
  }
}
