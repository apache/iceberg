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
import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.parquet.ParquetFileMerger;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.FileRewriteCoordinator;
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
    if (!canUseMerger(group)) {
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
      LOG.warn(
          "Row-group merge failed for group {}, falling back to standard Spark rewrite: {}",
          groupId,
          e.getMessage(),
          e);
      // Fallback to standard rewrite
      super.doRewrite(groupId, group);
    }
  }

  /**
   * Checks if the file group can use row-group level merging.
   *
   * <p>Requirements:
   *
   * <ul>
   *   <li>All files must be Parquet format
   *   <li>Files must have compatible schemas (verified by ParquetFileMerger.canMerge)
   *   <li>Files must not be encrypted (TODO: add encryption check)
   * </ul>
   *
   * @param group the file group to check
   * @return true if row-group merging can be used, false otherwise
   */
  private boolean canUseMerger(RewriteFileGroup group) {
    // Check if all files are Parquet format
    boolean allParquet =
        group.rewrittenFiles().stream().allMatch(file -> file.format() == FileFormat.PARQUET);

    if (!allParquet) {
      LOG.debug("Cannot use row-group merge: not all files are Parquet format");
      return false;
    }

    // Validate schema compatibility
    try {
      List<String> filePaths =
          group.rewrittenFiles().stream()
              .map(f -> f.path().toString())
              .collect(Collectors.toList());

      List<Path> paths = filePaths.stream().map(Path::new).collect(Collectors.toList());
      Configuration hadoopConf = spark().sessionState().newHadoopConf();

      boolean canMerge = ParquetFileMerger.canMerge(paths, hadoopConf);

      if (!canMerge) {
        LOG.warn(
            "Cannot use row-group merge: schema validation failed for {} files. "
                + "Falling back to standard rewrite.",
            group.rewrittenFiles().size());
        return false;
      }

      // TODO: Add encryption check here
      // if (hasEncryptedFiles(group)) {
      //   LOG.warn("Cannot use row-group merge: encrypted files detected");
      //   return false;
      // }

      return true;
    } catch (Exception e) {
      LOG.warn("Cannot use row-group merge: validation failed", e);
      return false;
    }
  }

  /**
   * Merges all Parquet files in the group into a single output file. The planner has already
   * created appropriately-sized groups, so the runner merges the entire group without further
   * splitting.
   */
  private void mergeParquetFilesDistributed(String groupId, RewriteFileGroup group) {
    PartitionSpec spec = table().specs().get(group.outputSpecId());
    StructLike partition = group.info().partition();

    LOG.info(
        "Merging {} Parquet files into a single output file (group: {})",
        group.rewrittenFiles().size(),
        groupId);

    // Extract file paths for the entire group
    List<String> filePaths =
        group.rewrittenFiles().stream().map(f -> f.path().toString()).collect(Collectors.toList());

    // Create a single merge task for the entire group
    MergeTaskInfo mergeTask = new MergeTaskInfo(groupId, filePaths, spec, partition);

    // Get Hadoop configuration for executors
    Configuration hadoopConf = spark().sessionState().newHadoopConf();

    // Execute merge on an executor - returns only metadata (path + size)
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
    JavaRDD<MergeTaskInfo> taskRDD = jsc.parallelize(Lists.newArrayList(mergeTask), 1);
    MergeResult mergeResult =
        taskRDD.map(task -> mergeFilesForTask(task, hadoopConf)).collect().get(0);

    // Driver constructs DataFile from metadata using Table.io()
    MetricsConfig metricsConfig = MetricsConfig.getDefault();
    Metrics metrics =
        ParquetUtil.fileMetrics(table().io().newInputFile(mergeResult.getPath()), metricsConfig);

    DataFile resultFile =
        org.apache.iceberg.DataFiles.builder(mergeResult.getSpec())
            .withPath(mergeResult.getPath())
            .withFormat(FileFormat.PARQUET)
            .withPartition(mergeResult.getPartition())
            .withFileSizeInBytes(mergeResult.getFileSize())
            .withMetrics(metrics)
            .build();

    // Register merged file with coordinator
    Set<DataFile> newFiles = Sets.newHashSet(resultFile);
    coordinator.stageRewrite(table(), groupId, newFiles);

    LOG.info(
        "Successfully merged {} Parquet files into 1 output file (group: {})",
        group.rewrittenFiles().size(),
        groupId);
  }

  /**
   * Performs the actual merge operation for a single task on an executor. Returns only metadata
   * (file path and size); DataFile construction happens on the driver.
   */
  private static MergeResult mergeFilesForTask(MergeTaskInfo task, Configuration hadoopConf)
      throws IOException {
    // Convert file path strings to Hadoop Path objects
    List<Path> inputPaths =
        task.getFilePaths().stream().map(Path::new).collect(Collectors.toList());

    // Generate output file path - derive directory from first input file
    String outputFileName = String.format("%s-%s.parquet", task.getTaskId(), UUID.randomUUID());
    Path firstInputFilePath = new Path(task.getFilePaths().get(0));
    Path outputDir = firstInputFilePath.getParent();
    Path outputPath = new Path(outputDir, outputFileName);

    // Merge files using static method
    ParquetFileMerger.mergeFiles(inputPaths, outputPath, hadoopConf);

    // Get file size
    long fileSize = outputPath.getFileSystem(hadoopConf).getFileStatus(outputPath).getLen();

    // Return lightweight metadata - driver will construct DataFile with metrics
    return new MergeResult(outputPath.toString(), fileSize, task.getSpec(), task.getPartition());
  }

  /**
   * Lightweight serializable task containing only the essential information needed for merging.
   * Uses simple types ({@code String}, {@code List<String>}) that serialize reliably.
   */
  private static class MergeTaskInfo implements Serializable {
    private final String taskId;
    private final List<String> filePaths;
    private final PartitionSpec spec;
    private final StructLike partition;

    MergeTaskInfo(String taskId, List<String> filePaths, PartitionSpec spec, StructLike partition) {
      this.taskId = taskId;
      this.filePaths = filePaths;
      this.spec = spec;
      this.partition = partition;
    }

    String getTaskId() {
      return taskId;
    }

    List<String> getFilePaths() {
      return filePaths;
    }

    PartitionSpec getSpec() {
      return spec;
    }

    StructLike getPartition() {
      return partition;
    }
  }

  /**
   * Result of a merge operation on an executor. Contains only lightweight metadata; DataFile
   * construction with metrics happens on the driver.
   */
  private static class MergeResult implements Serializable {
    private final String path;
    private final long fileSize;
    private final PartitionSpec spec;
    private final StructLike partition;

    MergeResult(String path, long fileSize, PartitionSpec spec, StructLike partition) {
      this.path = path;
      this.fileSize = fileSize;
      this.spec = spec;
      this.partition = partition;
    }

    String getPath() {
      return path;
    }

    long getFileSize() {
      return fileSize;
    }

    PartitionSpec getSpec() {
      return spec;
    }

    StructLike getPartition() {
      return partition;
    }
  }
}
