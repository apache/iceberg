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
import org.apache.iceberg.hadoop.HadoopFileIO;
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
    // Check if all files are Parquet format
    if (canUseMerger(group)) {
      try {
        LOG.info(
            "Merging {} Parquet files using row-group level merge (group: {})",
            group.rewrittenFiles().size(),
            groupId);
        mergeParquetFilesDistributed(groupId, group);
        return;
      } catch (Exception e) {
        LOG.warn(
            "Failed to merge Parquet files using ParquetFileMerger, falling back to Spark rewrite",
            e);
      }
    }

    // Use standard Spark rewrite (same as parent class)
    super.doRewrite(groupId, group);
  }

  private boolean canUseMerger(RewriteFileGroup group) {
    // Check if all files are Parquet format
    return group.rewrittenFiles().stream().allMatch(file -> file.format() == FileFormat.PARQUET);
  }

  /**
   * Distributes Parquet file merging across Spark executors. Groups input files by target size and
   * processes each group in parallel.
   */
  private void mergeParquetFilesDistributed(String groupId, RewriteFileGroup group) {
    long targetFileSize = group.maxOutputFileSize();
    PartitionSpec spec = table().specs().get(group.outputSpecId());
    StructLike partition = group.info().partition();

    // Group files by target size
    List<List<DataFile>> fileGroups = groupFilesBySize(group.rewrittenFiles(), targetFileSize);

    LOG.info(
        "Grouped {} input files into {} output groups (target size: {} bytes, group: {})",
        group.rewrittenFiles().size(),
        fileGroups.size(),
        targetFileSize,
        groupId);

    // Create merge tasks - lightweight serializable objects with just the data needed
    List<MergeTaskInfo> mergeTasks = Lists.newArrayList();
    for (int i = 0; i < fileGroups.size(); i++) {
      List<DataFile> filesInGroup = fileGroups.get(i);
      String taskId = String.format("%s-%d", groupId, i);

      // Extract just the file paths for serialization
      List<String> filePaths =
          filesInGroup.stream().map(f -> f.path().toString()).collect(Collectors.toList());

      mergeTasks.add(new MergeTaskInfo(taskId, filePaths, spec, partition));
    }

    // Get Hadoop configuration for executors
    Configuration hadoopConf = spark().sessionState().newHadoopConf();

    // Use JavaRDD for simpler distributed processing
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
    JavaRDD<MergeTaskInfo> tasksRDD = jsc.parallelize(mergeTasks, mergeTasks.size());

    // Execute merges in parallel and collect results
    List<DataFile> resultFiles =
        tasksRDD.map(task -> mergeFilesForTask(task, hadoopConf)).collect();

    // Register merged files with coordinator
    Set<DataFile> newFiles = Sets.newHashSet(resultFiles);
    coordinator.stageRewrite(table(), groupId, newFiles);

    LOG.info(
        "Successfully merged {} Parquet files into {} output files (group: {})",
        group.rewrittenFiles().size(),
        resultFiles.size(),
        groupId);
  }

  /**
   * Groups files into sub-groups where each group's total size is close to the target size. Uses a
   * simple bin-packing algorithm.
   */
  private List<List<DataFile>> groupFilesBySize(Set<DataFile> files, long targetSize) {
    List<DataFile> sortedFiles =
        files.stream()
            .sorted((f1, f2) -> Long.compare(f2.fileSizeInBytes(), f1.fileSizeInBytes()))
            .collect(Collectors.toList());

    List<List<DataFile>> groups = Lists.newArrayList();
    List<DataFile> currentGroup = Lists.newArrayList();
    long currentGroupSize = 0;

    for (DataFile file : sortedFiles) {
      long fileSize = file.fileSizeInBytes();

      // If adding this file would exceed target size and we already have files in the group,
      // start a new group
      if (currentGroupSize > 0 && currentGroupSize + fileSize > targetSize * 1.1) {
        groups.add(currentGroup);
        currentGroup = Lists.newArrayList();
        currentGroupSize = 0;
      }

      currentGroup.add(file);
      currentGroupSize += fileSize;
    }

    // Add the last group if it's not empty
    if (!currentGroup.isEmpty()) {
      groups.add(currentGroup);
    }

    return groups;
  }

  /**
   * Performs the actual merge operation for a single task on an executor. This is a static method
   * to ensure it can be serialized and sent to executors.
   */
  private static DataFile mergeFilesForTask(MergeTaskInfo task, Configuration hadoopConf)
      throws IOException {
    // Convert file path strings to Hadoop Path objects
    List<Path> inputPaths =
        task.getFilePaths().stream().map(Path::new).collect(Collectors.toList());

    // Generate output file path - derive directory from first input file
    String outputFileName = String.format("%s-%s.parquet", task.getTaskId(), UUID.randomUUID());
    Path firstInputFilePath = new Path(task.getFilePaths().get(0));
    Path outputDir = firstInputFilePath.getParent();
    Path outputPath = new Path(outputDir, outputFileName);
    String outputLocation = outputPath.toString();

    // Create ParquetFileMerger and merge files
    ParquetFileMerger merger = new ParquetFileMerger(hadoopConf);
    merger.mergeFiles(inputPaths, outputPath);

    // Read the merged file's metrics
    HadoopFileIO fileIO = new HadoopFileIO(hadoopConf);
    MetricsConfig metricsConfig = MetricsConfig.getDefault();
    Metrics metrics = ParquetUtil.fileMetrics(fileIO.newInputFile(outputLocation), metricsConfig);

    // Get file size
    long fileSize = outputPath.getFileSystem(hadoopConf).getFileStatus(outputPath).getLen();

    // Create DataFile from the merged output
    return org.apache.iceberg.DataFiles.builder(task.getSpec())
        .withPath(outputLocation)
        .withFormat(FileFormat.PARQUET)
        .withPartition(task.getPartition())
        .withFileSizeInBytes(fileSize)
        .withMetrics(metrics)
        .build();
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
}
