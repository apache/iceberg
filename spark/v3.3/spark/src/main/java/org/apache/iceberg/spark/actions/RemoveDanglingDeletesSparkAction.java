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

import static org.apache.iceberg.MetadataTableType.ENTRIES;
import static org.apache.spark.sql.functions.min;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RemoveDanglingDeleteFiles;
import org.apache.iceberg.actions.RemoveDanglingDeleteFilesActionResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import scala.collection.JavaConverters;

/**
 * An action that removes dangling delete files from the current snapshot. A delete file is dangling
 * if its deletes no longer applies to any data file.
 *
 * <p>The following dangling delete files are removed:
 *
 * <ul>
 *   <li>Position delete files with a sequence number less than that of any data file in the same
 *       partition
 *   <li>Equality delete files with a sequence number less than or equal to that of any data file in
 *       the same partition
 * </ul>
 */
public class RemoveDanglingDeletesSparkAction
    extends BaseSnapshotUpdateSparkAction<RemoveDanglingDeletesSparkAction>
    implements RemoveDanglingDeleteFiles {

  private final Table table;

  protected RemoveDanglingDeletesSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
  }

  @Override
  protected RemoveDanglingDeletesSparkAction self() {
    return this;
  }

  @Override
  public Result execute() {
    if (table.specs().size() == 1 && table.spec().isUnpartitioned()) {
      // ManifestFilterManager already performs this table-wide on each commit
      return RemoveDanglingDeleteFilesActionResult.empty();
    }

    String desc = String.format("Remove dangling delete files for %s", table.name());
    JobGroupInfo info = newJobGroupInfo("REWRITE-MANIFESTS", desc);
    return withJobGroupInfo(info, this::doExecute);
  }

  private Result doExecute() {
    Dataset<Row> entries =
        loadMetadataTable(table, ENTRIES)
            .filter("status < 2") // live entries
            .selectExpr(
                "data_file.partition as partition",
                "data_file.spec_id as spec_id",
                "data_file.file_path as file_path",
                "data_file.content as content",
                "data_file.file_size_in_bytes as file_size_in_bytes",
                "data_file.record_count as record_count",
                "sequence_number");

    DeleteFiles deleteFiles = table.newDelete();
    List<DeleteFile> toRemove = withReusableDS(entries, this::danglingDeletes);
    toRemove.forEach(deleteFiles::deleteFile);
    deleteFiles.commit();

    return new RemoveDanglingDeleteFilesActionResult(toRemove);
  }

  /**
   * Calculate dangling delete files
   *
   * <ul>
   *   <li>Group all files by partition, calculate the minimum data file sequence number in each.
   *   <li>For each partition, check if any position delete files have a sequence number less than
   *       the partition's min_data_sequence_number
   *   <li>For each partition, check if any equality delete files have a sequence number less than
   *       or equal to the partition's min_data_sequence_number
   * </ul>
   *
   * @param entries dataset of file entries, marked by content (0 for data, 1 for posDeletes, 2 for
   *     eqDeletes)
   * @return list of dangling delete files
   */
  private List<DeleteFile> danglingDeletes(Dataset<Row> entries) {
    List<DeleteFile> removedDeleteFiles = Lists.newArrayList();

    // Minimum sequence number of data files in each partition
    Dataset<Row> minDataSeqNumberPerPartition =
        entries
            .filter("content == 0") // data files
            .groupBy("partition", "spec_id")
            .agg(min("sequence_number"))
            .toDF("partition", "spec_id", "min_data_sequence_number");

    // Dangling position delete files
    Dataset<Row> posDeleteDs =
        entries
            .filter("content == 1") // position delete files
            .join(
                minDataSeqNumberPerPartition,
                JavaConverters.asScalaBuffer(ImmutableList.of("partition", "spec_id")))
            .filter("sequence_number < min_data_sequence_number")
            .selectExpr("partition", "spec_id", "file_path", "file_size_in_bytes", "record_count");
    MakeDeleteFile makePosDeleteFn =
        new MakeDeleteFile(true, Partitioning.partitionType(table), table.specs());
    Dataset<DeleteFile> posDeletesToRemove =
        posDeleteDs.map(makePosDeleteFn, Encoders.javaSerialization(DeleteFile.class));

    removedDeleteFiles.addAll(posDeletesToRemove.collectAsList());

    // Dangling equality delete files
    Dataset<Row> eqDeleteDs =
        entries
            .filter("content == 2") // equality delete files
            .join(
                minDataSeqNumberPerPartition,
                JavaConverters.asScalaBuffer(ImmutableList.of("partition", "spec_id")))
            .filter("sequence_number <= min_data_sequence_number")
            .selectExpr("partition", "spec_id", "file_path", "file_size_in_bytes", "record_count");
    MakeDeleteFile makeEqDeleteFn =
        new MakeDeleteFile(false, Partitioning.partitionType(table), table.specs());
    Dataset<DeleteFile> eqDeletesToRemove =
        eqDeleteDs.map(makeEqDeleteFn, Encoders.javaSerialization(DeleteFile.class));

    removedDeleteFiles.addAll(eqDeletesToRemove.collectAsList());
    return removedDeleteFiles;
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
