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
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetFileMerger;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.spark.SparkUtil;
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
        mergeParquetFiles(groupId, group);
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

  private void mergeParquetFiles(String groupId, RewriteFileGroup group) throws IOException {
    // Convert Iceberg file paths to Hadoop Path objects
    List<Path> inputPaths =
        group.rewrittenFiles().stream()
            .map(file -> new Path(file.path().toString()))
            .collect(Collectors.toList());

    // Generate output file path
    String outputFileName = String.format("%s-%s.parquet", groupId, UUID.randomUUID());
    String outputLocation =
        table().locationProvider().newDataLocation(group.info().partition(), outputFileName);
    Path outputPath = new Path(outputLocation);

    // Get Hadoop configuration from Spark
    Configuration conf = SparkUtil.hadoopConfiguration(spark().sparkContext());

    // Create ParquetFileMerger and merge files
    ParquetFileMerger merger = new ParquetFileMerger(conf);
    merger.mergeFiles(inputPaths, outputPath);

    // Read the merged file's metrics using Parquet reader
    Metrics metrics = readParquetMetrics(outputPath, conf);

    // Create DataFile from the merged output
    PartitionSpec spec = table().specs().get(group.outputSpecId());
    DataFile mergedFile =
        org.apache.iceberg.DataFiles.builder(spec)
            .withPath(outputLocation)
            .withFormat(FileFormat.PARQUET)
            .withPartition(group.info().partition())
            .withFileSizeInBytes(getFileSize(outputPath, conf))
            .withMetrics(metrics)
            .build();

    // Register the merged file with the coordinator
    Set<DataFile> newFiles = Sets.newHashSet(mergedFile);
    coordinator.stageRewrite(table(), groupId, newFiles);

    LOG.info(
        "Successfully merged {} Parquet files into {} (size: {} bytes, group: {})",
        inputPaths.size(),
        outputLocation,
        mergedFile.fileSizeInBytes(),
        groupId);
  }

  private Metrics readParquetMetrics(Path filePath, Configuration conf) throws IOException {
    // Use Iceberg's Parquet reader to get metrics
    FileIO fileIO = table().io();
    if (fileIO instanceof HadoopFileIO) {
      return Parquet.fileMetrics(
          ((HadoopFileIO) fileIO).newInputFile(filePath.toString()), table().schema());
    } else {
      return Parquet.fileMetrics(fileIO.newInputFile(filePath.toString()), table().schema());
    }
  }

  private long getFileSize(Path filePath, Configuration conf) throws IOException {
    return filePath.getFileSystem(conf).getFileStatus(filePath).getLen();
  }
}
