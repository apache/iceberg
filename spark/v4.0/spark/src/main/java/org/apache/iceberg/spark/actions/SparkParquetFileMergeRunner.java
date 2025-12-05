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

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.ParquetFileMerger;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extension of SparkBinPackFileRewriteRunner that uses ParquetFileMerger for efficient row-group
 * level merging of Parquet files when applicable.
 *
 * <p>This runner uses {@link ParquetFileMerger} to merge Parquet files at the row-group level
 * without full deserialization, which is significantly faster than the standard Spark rewrite
 * approach for Parquet files.
 *
 * <p>The decision to use this runner vs. SparkBinPackFileRewriteRunner is controlled by the
 * configuration option {@code use-parquet-file-merger}.
 */
public class SparkParquetFileMergeRunner extends SparkBinPackFileRewriteRunner {
  private static final Logger LOG = LoggerFactory.getLogger(SparkParquetFileMergeRunner.class);
  private final FileRewriteCoordinator coordinator = FileRewriteCoordinator.get();

  public SparkParquetFileMergeRunner(SparkSession spark, Table table) {
    super(spark, table);
  }

  @Override
  public String description() {
    return "PARQUET-MERGE";
  }

  @Override
  protected void doRewrite(String groupId, RewriteFileGroup group) {
    // Early validation: check if requirements are met
    if (!canMerge(group)) {
      LOG.info(
          "Row-group merge requirements not met for group {}. Using standard Spark rewrite.",
          groupId);
      super.doRewrite(groupId, group);
      return;
    }

    // Requirements met - attempt row-group level merge
    try {
      LOG.info(
          "Merging {} Parquet files using row-group level merge (group: {})",
          group.rewrittenFiles().size(),
          groupId);
      mergeParquetFilesDistributed(groupId, group);
    } catch (Exception e) {
      LOG.info(
          "Row-group merge failed for group {}, falling back to standard Spark rewrite: {}",
          groupId,
          e.getMessage(),
          e);
      // Fallback to standard rewrite
      super.doRewrite(groupId, group);
    }
  }

  @VisibleForTesting
  boolean canMerge(RewriteFileGroup group) {
    return ParquetFileMerger.canMerge(group, table().sortOrder(), table().io());
  }

  /**
   * Merges all input files in a group into a single output file.
   *
   * <p>This method assumes the group has been validated by {@link #canMerge(RewriteFileGroup)} to
   * have exactly one expected output file.
   */
  private void mergeParquetFilesDistributed(String groupId, RewriteFileGroup group) {
    PartitionSpec spec = table().specs().get(group.outputSpecId());
    StructLike partition = group.info().partition();

    LOG.info(
        "Merging {} Parquet files into 1 output file (group: {})",
        group.rewrittenFiles().size(),
        groupId);

    // Get parameters needed for merge
    long rowGroupSize =
        PropertyUtil.propertyAsLong(
            table().properties(),
            TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
            TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT);
    List<DataFile> dataFiles = Lists.newArrayList(group.rewrittenFiles());
    FileIO fileIO = table().io();
    Table serializableTable = table();

    // Execute merge on an executor
    // Use parallelize with a single dummy element to run the merge on an executor
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
    JavaRDD<Integer> taskRDD = jsc.parallelize(Lists.newArrayList(1), 1);
    DataFile newFile =
        taskRDD
            .map(
                ignored ->
                    mergeFilesForTask(
                        dataFiles, rowGroupSize, partition, spec, fileIO, serializableTable))
            .collect()
            .get(0);

    // Register merged file with coordinator
    coordinator.stageRewrite(table(), groupId, Sets.newHashSet(newFile));

    LOG.info(
        "Successfully merged {} Parquet files into 1 output file (group: {})",
        group.rewrittenFiles().size(),
        groupId);
  }

  /**
   * Performs the actual merge operation for a single task on an executor.
   *
   * <p>IMPORTANT: OutputFileFactory is created here on the executor (not serialized from driver)
   * using TaskContext.taskAttemptId() to ensure unique filenames across task retry attempts.
   *
   * <p>This method calls {@link ParquetFileMerger#mergeFiles} which handles the merge and returns a
   * complete DataFile with metrics.
   */
  private static DataFile mergeFilesForTask(
      List<DataFile> dataFiles,
      long rowGroupSize,
      StructLike partition,
      PartitionSpec spec,
      FileIO fileIO,
      Table table)
      throws IOException {
    // Create OutputFileFactory on executor using Spark's TaskContext.taskAttemptId()
    // This ensures unique filenames across retry attempts (taskAttemptId changes on each retry)
    TaskContext sparkContext = TaskContext.get();
    long taskAttemptId = sparkContext != null ? sparkContext.taskAttemptId() : 0L;

    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 0, taskAttemptId)
            .defaultSpec(spec)
            .format(FileFormat.PARQUET)
            .build();

    // Use OutputFileFactory to generate output file with proper naming and partition handling
    // Encryption is handled internally based on table configuration
    EncryptedOutputFile encryptedOutputFile =
        partition != null ? fileFactory.newOutputFile(partition) : fileFactory.newOutputFile();
    OutputFile outputFile = encryptedOutputFile.encryptingOutputFile();

    // Merge files and return DataFile with complete metadata
    // ParquetFileMerger handles schema reading, merging, metrics extraction, and DataFile
    // construction
    return ParquetFileMerger.mergeFiles(
        dataFiles, fileIO, outputFile, rowGroupSize, spec, partition);
  }
}
