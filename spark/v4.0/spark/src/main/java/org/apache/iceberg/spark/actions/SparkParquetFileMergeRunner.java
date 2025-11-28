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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.ParquetFileMerger;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.schema.MessageType;
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

  /**
   * Validates if the file group can use row-group level merging.
   *
   * <p>Requirements checked here:
   *
   * <ul>
   *   <li>Table must not have a sort order (no sorting or z-ordering)
   *   <li>Files must not have delete files or delete vectors
   *   <li>Partition spec must not be changing (row-group merge cannot repartition data)
   *   <li>Must not be splitting large files (row-group merge is optimized for merging, not
   *       splitting)
   * </ul>
   *
   * <p>Additional requirements checked by ParquetFileMerger.canMerge:
   *
   * <ul>
   *   <li>All files must be valid Parquet format
   *   <li>Files must have compatible schemas
   *   <li>Files must not be encrypted
   *   <li>If a physical _row_id column exists, all values must be non-null
   * </ul>
   *
   * @param group the file group to check
   * @return true if row-group merging can be used, false otherwise
   */
  @VisibleForTesting
  boolean canMerge(RewriteFileGroup group) {
    // Check if table has a sort order - row-group merge cannot preserve sort order
    if (table().sortOrder().isSorted()) {
      LOG.debug(
          "Cannot use row-group merge: table has a sort order ({}). "
              + "Row-group merging would not preserve the sort order.",
          table().sortOrder());
      return false;
    }

    // Check for delete files - row-group merge cannot apply deletes
    boolean hasDeletes = group.fileScanTasks().stream().anyMatch(task -> !task.deletes().isEmpty());

    if (hasDeletes) {
      LOG.debug(
          "Cannot use row-group merge: files have delete files or delete vectors. "
              + "Row-group merging cannot apply deletes.");
      return false;
    }

    // Check if partition spec is being changed - row-group merge cannot repartition data
    int outputSpecId = group.outputSpecId();
    boolean hasPartitionSpecChange =
        group.rewrittenFiles().stream().anyMatch(file -> file.specId() != outputSpecId);

    if (hasPartitionSpecChange) {
      LOG.debug(
          "Cannot use row-group merge: partition spec is changing. "
              + "Row-group merging cannot change data partitioning.");
      return false;
    }

    // Check if we're splitting large files - row-group merge is designed for file merging, not
    // splitting
    long maxOutputFileSize = group.maxOutputFileSize();
    boolean isSplittingFiles =
        group.rewrittenFiles().stream()
            .anyMatch(file -> file.fileSizeInBytes() > maxOutputFileSize);

    if (isSplittingFiles) {
      LOG.debug(
          "Cannot use row-group merge: compaction is splitting large files. "
              + "Row-group merging is optimized for merging files, not splitting them.");
      return false;
    }

    // Validate schema compatibility and other Parquet-specific requirements
    try {
      List<InputFile> inputFiles =
          group.rewrittenFiles().stream()
              .map(f -> table().io().newInputFile(f.path().toString()))
              .collect(Collectors.toList());

      // Validate files can be merged
      boolean canMerge = ParquetFileMerger.canMerge(inputFiles);

      if (!canMerge) {
        LOG.info(
            "Cannot use row-group merge for {} files. Falling back to standard rewrite. "
                + "Reason: Parquet validation failed",
            group.rewrittenFiles().size());
      }

      return canMerge;
    } catch (Exception e) {
      LOG.info("Cannot use row-group merge: validation failed", e);
      return false;
    }
  }

  /**
   * Merges Parquet files in the group, respecting the expected output file count determined by the
   * planner. Files are distributed evenly across the expected number of output files.
   */
  private void mergeParquetFilesDistributed(String groupId, RewriteFileGroup group) {
    PartitionSpec spec = table().specs().get(group.outputSpecId());
    StructLike partition = group.info().partition();
    int expectedOutputFiles = group.expectedOutputFiles();

    LOG.info(
        "Merging {} Parquet files into {} expected output files (group: {})",
        group.rewrittenFiles().size(),
        expectedOutputFiles,
        groupId);

    // Check if table supports row lineage
    boolean preserveRowLineage = TableUtil.supportsRowLineage(table());

    // Distribute files evenly across expected output files (planner already determined the count)
    List<List<DataFile>> fileBatches =
        distributeFilesEvenly(group.rewrittenFiles(), expectedOutputFiles);

    // Get row group size from table properties
    long rowGroupSize =
        PropertyUtil.propertyAsLong(
            table().properties(),
            TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES,
            TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT);

    // Get column index truncate length from Hadoop Configuration (same as ParquetWriter)
    Configuration hadoopConf = spark().sessionState().newHadoopConf();
    int columnIndexTruncateLength =
        hadoopConf.getInt(
            ParquetOutputFormat.COLUMN_INDEX_TRUNCATE_LENGTH,
            ParquetProperties.DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH);

    // Create merge tasks for each batch
    List<MergeTaskInfo> mergeTasks = Lists.newArrayList();
    int batchIndex = 0;
    for (List<DataFile> batch : fileBatches) {
      List<String> filePaths =
          batch.stream().map(f -> f.path().toString()).collect(Collectors.toList());

      // Extract firstRowIds and dataSequenceNumbers for row lineage preservation
      List<Long> firstRowIds = null;
      List<Long> dataSequenceNumbers = null;
      if (preserveRowLineage) {
        firstRowIds = batch.stream().map(DataFile::firstRowId).collect(Collectors.toList());
        dataSequenceNumbers =
            batch.stream().map(DataFile::dataSequenceNumber).collect(Collectors.toList());
        LOG.debug(
            "Batch {} will preserve row lineage with firstRowIds: {} and dataSequenceNumbers: {} "
                + "(group: {})",
            batchIndex,
            firstRowIds,
            dataSequenceNumbers,
            groupId);
      }

      mergeTasks.add(
          new MergeTaskInfo(
              filePaths,
              rowGroupSize,
              columnIndexTruncateLength,
              firstRowIds,
              dataSequenceNumbers,
              batchIndex,
              partition,
              spec,
              FileFormat.PARQUET));

      batchIndex++;
    }

    // Get FileIO for executors - table().io() is serializable
    FileIO fileIO = table().io();

    // Serialize table for executors (needed to create OutputFileFactory)
    Table serializableTable = table();

    // Execute merges on executors in parallel
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
    JavaRDD<MergeTaskInfo> taskRDD = jsc.parallelize(mergeTasks, mergeTasks.size());
    List<MergeResult> mergeResults =
        taskRDD.map(task -> mergeFilesForTask(task, fileIO, serializableTable)).collect();

    // Driver constructs DataFiles from metadata
    MetricsConfig metricsConfig = MetricsConfig.getDefault();
    Set<DataFile> newFiles = Sets.newHashSet();

    for (MergeResult mergeResult : mergeResults) {
      Metrics metrics =
          ParquetUtil.fileMetrics(table().io().newInputFile(mergeResult.path()), metricsConfig);

      DataFiles.Builder builder =
          DataFiles.builder(spec)
              .withPath(mergeResult.path())
              .withFormat(FileFormat.PARQUET)
              .withPartition(partition)
              .withFileSizeInBytes(mergeResult.fileSize())
              .withMetrics(metrics);

      // Extract firstRowId from Parquet column statistics (same as binpack approach)
      // For V3+ tables with row lineage, the min value of _row_id column becomes firstRowId
      if (preserveRowLineage && metrics.lowerBounds() != null) {
        ByteBuffer rowIdLowerBound = metrics.lowerBounds().get(MetadataColumns.ROW_ID.fieldId());
        if (rowIdLowerBound != null) {
          Long firstRowId = Conversions.fromByteBuffer(Types.LongType.get(), rowIdLowerBound);
          builder.withFirstRowId(firstRowId);
        }
      }

      newFiles.add(builder.build());
    }

    // Register merged files with coordinator
    coordinator.stageRewrite(table(), groupId, newFiles);

    LOG.info(
        "Successfully merged {} Parquet files into {} output files (group: {})",
        group.rewrittenFiles().size(),
        newFiles.size(),
        groupId);
  }

  /**
   * Distributes files across the expected number of output files using greedy bin-packing by size.
   *
   * <p>This ensures each output file gets approximately the same total size of input files, rather
   * than the same count of files.
   */
  private List<List<DataFile>> distributeFilesEvenly(Set<DataFile> files, int expectedOutputFiles) {
    if (expectedOutputFiles <= 0 || files.isEmpty()) {
      return Lists.newArrayList();
    }

    // Sort files by size (largest first) for better bin-packing
    List<DataFile> sortedFiles = Lists.newArrayList(files);
    sortedFiles.sort((f1, f2) -> Long.compare(f2.fileSizeInBytes(), f1.fileSizeInBytes()));

    // Create bins for output files
    List<List<DataFile>> bins = Lists.newArrayList();
    List<Long> binSizes = Lists.newArrayList();
    for (int i = 0; i < expectedOutputFiles; i++) {
      bins.add(Lists.newArrayList());
      binSizes.add(0L);
    }

    // Greedy bin-packing: assign each file to the bin with smallest current size
    for (DataFile file : sortedFiles) {
      int smallestBinIndex = 0;
      long smallestBinSize = binSizes.get(0);

      for (int i = 1; i < binSizes.size(); i++) {
        if (binSizes.get(i) < smallestBinSize) {
          smallestBinIndex = i;
          smallestBinSize = binSizes.get(i);
        }
      }

      bins.get(smallestBinIndex).add(file);
      binSizes.set(smallestBinIndex, smallestBinSize + file.fileSizeInBytes());
    }

    // Return only non-empty bins
    return bins.stream().filter(bin -> !bin.isEmpty()).collect(Collectors.toList());
  }

  /**
   * Performs the actual merge operation for a single task on an executor. Returns only metadata
   * (file path and size); DataFile construction happens on the driver.
   *
   * <p>IMPORTANT: OutputFileFactory is created here on the executor (not serialized from driver)
   * using TaskContext.taskAttemptId() to ensure unique filenames across task retry attempts.
   */
  private static MergeResult mergeFilesForTask(MergeTaskInfo task, FileIO fileIO, Table table)
      throws IOException {
    // Convert file path strings to Iceberg InputFile objects
    List<InputFile> inputFiles =
        task.filePaths().stream()
            .map(path -> fileIO.newInputFile(path))
            .collect(Collectors.toList());

    // Read schema and metadata from first input file (already validated on driver to be compatible)
    MessageType schema = ParquetFileMerger.readSchema(inputFiles.get(0));
    Map<String, String> metadata = ParquetFileMerger.readMetadata(inputFiles.get(0));

    // Create OutputFileFactory on executor using Spark's TaskContext.taskAttemptId()
    // This ensures unique filenames across retry attempts (taskAttemptId changes on each retry)
    TaskContext sparkContext = TaskContext.get();
    long taskAttemptId = sparkContext != null ? sparkContext.taskAttemptId() : 0L;

    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, task.batchIndex(), taskAttemptId)
            .defaultSpec(task.spec())
            .format(task.format())
            .build();

    // Use OutputFileFactory to generate output file with proper naming and partition handling
    // Encryption is handled internally based on table configuration
    EncryptedOutputFile encryptedOutputFile =
        task.partition() != null
            ? fileFactory.newOutputFile(task.partition())
            : fileFactory.newOutputFile();

    // Merge files using schema and metadata from first input file
    ParquetFileMerger.mergeFiles(
        inputFiles,
        encryptedOutputFile,
        schema,
        task.firstRowIds(),
        task.dataSequenceNumbers(),
        task.rowGroupSize(),
        task.columnIndexTruncateLength(),
        metadata);

    // Get file size from the output file
    String outputPath = encryptedOutputFile.encryptingOutputFile().location();
    long fileSize = fileIO.newInputFile(outputPath).getLength();

    // Return lightweight metadata - driver will construct DataFile with metrics
    // firstRowId will be extracted from Parquet column statistics on the driver
    return new MergeResult(outputPath, fileSize);
  }

  /**
   * Lightweight serializable task containing only the essential information needed for merging.
   * Uses simple types ({@code String}, {@code List<String>}) that serialize reliably.
   *
   * <p>NOTE: OutputFileFactory is NOT serialized to avoid concurrent write issues during Spark task
   * retries. Instead, it's created on the executor using TaskContext.taskAttemptId() which is
   * unique across retry attempts.
   */
  private static class MergeTaskInfo implements Serializable {
    private final List<String> filePaths;
    private final long rowGroupSize;
    private final int columnIndexTruncateLength;
    private final List<Long> firstRowIds;
    private final List<Long> dataSequenceNumbers;
    private final int batchIndex;
    private final StructLike partition;
    private final PartitionSpec spec;
    private final FileFormat format;

    MergeTaskInfo(
        List<String> filePaths,
        long rowGroupSize,
        int columnIndexTruncateLength,
        List<Long> firstRowIds,
        List<Long> dataSequenceNumbers,
        int batchIndex,
        StructLike partition,
        PartitionSpec spec,
        FileFormat format) {
      this.filePaths = filePaths;
      this.rowGroupSize = rowGroupSize;
      this.columnIndexTruncateLength = columnIndexTruncateLength;
      this.firstRowIds = firstRowIds;
      this.dataSequenceNumbers = dataSequenceNumbers;
      this.batchIndex = batchIndex;
      this.partition = partition;
      this.spec = spec;
      this.format = format;
    }

    List<String> filePaths() {
      return filePaths;
    }

    long rowGroupSize() {
      return rowGroupSize;
    }

    int columnIndexTruncateLength() {
      return columnIndexTruncateLength;
    }

    StructLike partition() {
      return partition;
    }

    List<Long> firstRowIds() {
      return firstRowIds;
    }

    List<Long> dataSequenceNumbers() {
      return dataSequenceNumbers;
    }

    int batchIndex() {
      return batchIndex;
    }

    PartitionSpec spec() {
      return spec;
    }

    FileFormat format() {
      return format;
    }
  }

  /**
   * Result of a merge operation on an executor. Contains only lightweight metadata; DataFile
   * construction with metrics happens on the driver.
   */
  private static class MergeResult implements Serializable {
    private final String path;
    private final long fileSize;

    MergeResult(String path, long fileSize) {
      this.path = path;
      this.fileSize = fileSize;
    }

    String path() {
      return path;
    }

    long fileSize() {
      return fileSize;
    }
  }
}
