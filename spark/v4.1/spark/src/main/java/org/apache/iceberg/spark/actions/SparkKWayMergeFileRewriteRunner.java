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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderComparators;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.spark.source.SerializableTableWithSize;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SortedMerge;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A file rewrite runner that performs K-way merge of pre-sorted data files.
 *
 * <p>This runner implements a K-way merge algorithm: for each file in the group, it opens a
 * streaming iterator and uses a priority queue (via {@link SortedMerge}) to merge all iterators in
 * sort-key order. Output is written to new files with size-based rotation.
 *
 * <p>All input files must be pre-sorted according to the table's sort order. The runner validates
 * this precondition before execution.
 *
 * <p>Unlike BinPack and Sort runners which use Spark SQL read/write paths, this runner operates
 * directly on Iceberg's generic reader/writer stack. This eliminates shuffle entirely, making it
 * optimal for tables with pre-sorted overlapping files.
 *
 * <p>Note: The parent class {@link SparkDataFileRewriteRunner#rewrite} stages tasks in {@link
 * org.apache.iceberg.spark.ScanTaskSetManager} for use by the Iceberg Spark datasource. K-way merge
 * bypasses that read path and instead broadcasts range assignments directly to executors.
 */
class SparkKWayMergeFileRewriteRunner extends SparkDataFileRewriteRunner {

  private static final Logger LOG = LoggerFactory.getLogger(SparkKWayMergeFileRewriteRunner.class);

  static final String RANGE_PARALLELISM_ENABLED = "range-parallelism-enabled";
  static final boolean RANGE_PARALLELISM_ENABLED_DEFAULT = true;

  static final String RANGES_PER_GROUP = "ranges-per-group";
  static final int RANGES_PER_GROUP_DEFAULT = 25;

  static final String MIN_FILES_FOR_RANGE_PARALLELISM = "min-files-for-range-parallelism";
  static final int MIN_FILES_FOR_RANGE_PARALLELISM_DEFAULT = 10;

  private boolean rangeParallelismEnabled;
  private int rangesPerGroup;
  private int minFilesForRangeParallelism;

  private volatile Broadcast<Table> cachedTableBroadcast = null;

  SparkKWayMergeFileRewriteRunner(SparkSession spark, Table table) {
    super(spark, table);
  }

  @Override
  public String description() {
    return "K-WAY-MERGE";
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.<String>builder()
        .addAll(super.validOptions())
        .add(RANGE_PARALLELISM_ENABLED)
        .add(RANGES_PER_GROUP)
        .add(MIN_FILES_FOR_RANGE_PARALLELISM)
        .build();
  }

  @Override
  public void init(Map<String, String> options) {
    super.init(options);
    this.rangeParallelismEnabled =
        PropertyUtil.propertyAsBoolean(
            options, RANGE_PARALLELISM_ENABLED, RANGE_PARALLELISM_ENABLED_DEFAULT);
    this.rangesPerGroup =
        PropertyUtil.propertyAsInt(options, RANGES_PER_GROUP, RANGES_PER_GROUP_DEFAULT);
    Preconditions.checkArgument(
        rangesPerGroup > 0, "'%s' must be > 0, got %s", RANGES_PER_GROUP, rangesPerGroup);
    this.minFilesForRangeParallelism =
        PropertyUtil.propertyAsInt(
            options, MIN_FILES_FOR_RANGE_PARALLELISM, MIN_FILES_FOR_RANGE_PARALLELISM_DEFAULT);
    Preconditions.checkArgument(
        minFilesForRangeParallelism > 0,
        "'%s' must be > 0, got %s",
        MIN_FILES_FOR_RANGE_PARALLELISM,
        minFilesForRangeParallelism);
  }

  @Override
  void doRewrite(String groupId, RewriteFileGroup group) {
    List<FileScanTask> tasks = group.fileScanTasks();
    validateSortOrder(groupId, tasks);

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark().sparkContext());
    Broadcast<Table> tableBroadcast = tableBroadcast(jsc);

    SortOrder fileSortOrder = resolveOutputSortOrder(table(), outputSortOrderId(tasks));
    long targetFileSize = group.maxOutputFileSize();
    int outputSpecId = group.outputSpecId();
    int numRanges = calculateNumRanges(tasks, targetFileSize);

    List<List<FileScanTask>> rangeAssignments = assignFilesBySize(tasks, numRanges);

    // Broadcast range assignments to executors. ScanTaskSetManager is driver-local and cannot
    // be accessed from executors. Broadcasting is safe here because range counts are bounded
    // by ranges-per-group (default 25) and each range contains only file metadata references.
    Broadcast<List<List<FileScanTask>>> broadcastAssignments = jsc.broadcast(rangeAssignments);

    try {
      List<Integer> rangeIndices = Lists.newArrayListWithCapacity(numRanges);
      for (int i = 0; i < numRanges; i++) {
        rangeIndices.add(i);
      }

      Set<DataFile> allOutputFiles =
          Sets.newHashSet(
              jsc.parallelize(rangeIndices, numRanges)
                  .flatMap(
                      rangeIndex -> {
                        List<FileScanTask> files = broadcastAssignments.value().get(rangeIndex);
                        if (files.isEmpty()) {
                          return Collections.<DataFile>emptyList().iterator();
                        }

                        Table table = tableBroadcast.value();
                        return mergeAndWriteFiles(
                                files, table, fileSortOrder, targetFileSize, outputSpecId)
                            .iterator();
                      })
                  .collect());

      FileRewriteCoordinator.get().stageRewrite(table(), groupId, allOutputFiles);

      LOG.info(
          "K-way merge completed for group {}: {} input files in {} ranges -> {} output files",
          groupId,
          tasks.size(),
          numRanges,
          allOutputFiles.size());
    } finally {
      broadcastAssignments.destroy();
    }
  }

  private Broadcast<Table> tableBroadcast(JavaSparkContext jsc) {
    if (cachedTableBroadcast == null) {
      synchronized (this) {
        if (cachedTableBroadcast == null) {
          cachedTableBroadcast = jsc.broadcast(SerializableTableWithSize.copyOf(table()));
        }
      }
    }
    return cachedTableBroadcast;
  }

  private static List<DataFile> mergeAndWriteFiles(
      List<FileScanTask> files,
      Table table,
      SortOrder fileSortOrder,
      long targetFileSize,
      int outputSpecId) {

    CloseableGroup resources = new CloseableGroup();
    resources.setSuppressCloseFailure(true);

    try {
      List<CloseableIterable<Record>> fileIterables = Lists.newArrayList();

      for (FileScanTask task : files) {
        FileScanTask readTask =
            new BaseFileScanTask(
                task.file(),
                task.deletes().toArray(new DeleteFile[0]),
                SchemaParser.toJson(table.schema()),
                PartitionSpecParser.toJson(table.spec()),
                ResidualEvaluator.unpartitioned(Expressions.alwaysTrue()));

        GenericDeleteFilter deleteFilter =
            new GenericDeleteFilter(table.io(), readTask, table.schema(), table.schema());

        Schema readSchema = deleteFilter.requiredSchema();

        EncryptedInputFile encryptedInput =
            EncryptedFiles.encryptedInput(
                table.io().newInputFile(task.file().location()), task.file().keyMetadata());
        InputFile input = table.encryption().decrypt(encryptedInput);

        CloseableIterable<Record> records = openFile(input, readSchema, readTask, task);
        resources.addCloseable(records);
        records = deleteFilter.filter(records);
        fileIterables.add(records);
      }

      @SuppressWarnings("unchecked")
      Comparator<Record> comparator =
          (Comparator<Record>)
              (Comparator<?>) SortOrderComparators.forSchema(table.schema(), fileSortOrder);

      SortedMerge<Record> sortedMerge = new SortedMerge<>(comparator, fileIterables);
      resources.addCloseable(sortedMerge);

      return writeOutputFiles(
          sortedMerge.iterator(), table, fileSortOrder, targetFileSize, outputSpecId);

    } catch (Exception e) {
      LOG.error("K-way merge failed on executor", e);
      throw new RuntimeException("K-way merge failed", e);
    } finally {
      try {
        resources.close();
      } catch (IOException closeException) {
        LOG.warn("Failed to close resources during cleanup", closeException);
      }
    }
  }

  private static CloseableIterable<Record> openFile(
      InputFile input, Schema readSchema, FileScanTask readTask, FileScanTask originalTask) {
    switch (originalTask.file().format()) {
      case PARQUET:
        return Parquet.read(input)
            .project(readSchema)
            .filter(readTask.residual())
            .createReaderFunc(
                fileSchema ->
                    GenericParquetReaders.buildReader(
                        readSchema, fileSchema, Collections.emptyMap()))
            .split(originalTask.start(), originalTask.length())
            .build();
      case AVRO:
        return Avro.read(input)
            .project(readSchema)
            .createReaderFunc(
                avroSchema -> DataReader.create(readSchema, avroSchema, Collections.emptyMap()))
            .split(originalTask.start(), originalTask.length())
            .build();
      case ORC:
        return ORC.read(input)
            .project(readSchema)
            .filter(readTask.residual())
            .createReaderFunc(
                fileSchema ->
                    GenericOrcReader.buildReader(readSchema, fileSchema, Collections.emptyMap()))
            .split(originalTask.start(), originalTask.length())
            .build();
      default:
        throw new IllegalArgumentException(
            "Unsupported file format: " + originalTask.file().format());
    }
  }

  private static List<DataFile> writeOutputFiles(
      CloseableIterator<Record> records,
      Table table,
      SortOrder outputSortOrder,
      long targetFileSize,
      int outputSpecId)
      throws IOException {

    PartitionSpec spec = table.specs().get(outputSpecId);
    Schema schema = table.schema();
    PartitionKey outputPartitionKey = new PartitionKey(spec, schema);

    TaskContext taskContext = TaskContext.get();
    int partitionId = taskContext != null ? taskContext.partitionId() : 0;
    long taskId = taskContext != null ? taskContext.taskAttemptId() : 0L;
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, partitionId, taskId).defaultSpec(spec).build();
    FileFormat outputFormat =
        FileFormat.fromString(
            table
                .properties()
                .getOrDefault(
                    TableProperties.DEFAULT_FILE_FORMAT,
                    TableProperties.DEFAULT_FILE_FORMAT_DEFAULT));

    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(schema, spec, null, null, null).setAll(table.properties());

    List<DataFile> writtenFiles = Lists.newArrayList();
    DataWriter<Record> writer = null;
    String currentOutputFilePath = null;
    StructLike currentPartition = null;

    try {
      while (records.hasNext()) {
        Record record = records.next();
        outputPartitionKey.partition(record);

        boolean partitionChanged =
            currentPartition != null && !currentPartition.equals(outputPartitionKey);
        if (writer == null || writer.length() >= targetFileSize || partitionChanged) {
          if (writer != null) {
            writer.close();
            writtenFiles.add(writer.toDataFile());
            writer = null;
            currentOutputFilePath = null;
          }

          currentPartition = outputPartitionKey.copy();
          EncryptedOutputFile encryptedOutputFile =
              fileFactory.newOutputFile(spec, currentPartition);
          currentOutputFilePath = encryptedOutputFile.encryptingOutputFile().location();

          writer =
              new DataWriter<>(
                  appenderFactory.newAppender(encryptedOutputFile, outputFormat),
                  outputFormat,
                  currentOutputFilePath,
                  spec,
                  currentPartition,
                  encryptedOutputFile.keyMetadata(),
                  outputSortOrder);
        }

        writer.write(record);
      }

      if (writer != null) {
        writer.close();
        writtenFiles.add(writer.toDataFile());
        writer = null;
        currentOutputFilePath = null;
      }
    } finally {
      try {
        records.close();
      } catch (IOException e) {
        LOG.warn("Failed to close records iterator during cleanup", e);
      }

      if (writer != null) {
        cleanupPartialWriter(writer, currentOutputFilePath, table);
      }
    }

    return writtenFiles;
  }

  private void validateSortOrder(String groupId, List<FileScanTask> group) {
    Preconditions.checkArgument(!group.isEmpty(), "Cannot rewrite empty file group: %s", groupId);

    Integer firstSortOrderId = group.get(0).file().sortOrderId();

    if (firstSortOrderId == null || firstSortOrderId == 0) {
      throw new ValidationException(
          "Group %s has files with missing or unsorted sort order. "
              + "K-way merge requires all files to be pre-sorted. "
              + "Use the SORT strategy first.",
          groupId);
    }

    for (FileScanTask task : group) {
      Integer sortOrderId = task.file().sortOrderId();
      if (!Objects.equals(sortOrderId, firstSortOrderId)) {
        throw new ValidationException(
            "Group %s has files with incompatible sort orders (found %s and %s). "
                + "K-way merge requires all files to have the same sort order. "
                + "Use the SORT strategy first.",
            groupId, firstSortOrderId, sortOrderId);
      }
    }
  }

  private int outputSortOrderId(List<FileScanTask> group) {
    Integer inputSortOrderId = group.get(0).file().sortOrderId();
    if (inputSortOrderId != null && inputSortOrderId > 0) {
      if (group.stream().allMatch(t -> Objects.equals(t.file().sortOrderId(), inputSortOrderId))) {
        return inputSortOrderId;
      }
    }
    return table().sortOrder().orderId();
  }

  private static SortOrder resolveOutputSortOrder(Table table, int sortOrderId) {
    if (sortOrderId > 0 && table.sortOrders().containsKey(sortOrderId)) {
      return table.sortOrders().get(sortOrderId);
    }

    Preconditions.checkArgument(
        !table.sortOrder().isUnsorted(),
        "Cannot resolve output sort order: table %s is unsorted and sort order ID %s is not in "
            + "the table's sort order map. Use the SORT strategy first.",
        table.name(),
        sortOrderId);

    return table.sortOrder();
  }

  private int calculateNumRanges(List<FileScanTask> files, long targetFileSize) {
    if (!rangeParallelismEnabled || files.size() < minFilesForRangeParallelism) {
      return 1;
    }

    long totalInputSize = files.stream().mapToLong(f -> f.file().fileSizeInBytes()).sum();

    if (totalInputSize < targetFileSize * 1.5) {
      return 1;
    }

    long expectedOutputFiles = (totalInputSize + targetFileSize - 1) / targetFileSize;
    int targetRanges = Math.min(rangesPerGroup, (int) expectedOutputFiles);
    return Math.max(1, targetRanges);
  }

  private static List<List<FileScanTask>> assignFilesBySize(
      List<FileScanTask> files, int numRanges) {
    if (numRanges <= 1) {
      return ImmutableList.of(Lists.newArrayList(files));
    }

    long totalInputSize = files.stream().mapToLong(f -> f.file().fileSizeInBytes()).sum();
    long targetSizePerRange = totalInputSize / numRanges;

    List<List<FileScanTask>> ranges = Lists.newArrayListWithCapacity(numRanges);
    for (int i = 0; i < numRanges; i++) {
      ranges.add(Lists.newArrayList());
    }

    int currentRange = 0;
    long currentRangeSize = 0;

    for (FileScanTask file : files) {
      if (currentRange < numRanges - 1
          && currentRangeSize > 0
          && currentRangeSize + file.file().fileSizeInBytes() > targetSizePerRange) {
        currentRange++;
        currentRangeSize = 0;
      }

      ranges.get(currentRange).add(file);
      currentRangeSize += file.file().fileSizeInBytes();
    }

    return ranges;
  }

  private static void cleanupPartialWriter(
      DataWriter<Record> writer, String outputFilePath, Table table) {
    try {
      writer.close();
    } catch (IOException closeException) {
      LOG.error("Failed to close writer during exception cleanup", closeException);
    }
    if (outputFilePath != null) {
      LOG.warn("Attempting to delete partial output file: {}", outputFilePath);
      try {
        table.io().deleteFile(outputFilePath);
      } catch (Exception deleteException) {
        LOG.warn(
            "Could not delete partial output file {} - may need manual cleanup",
            outputFilePath,
            deleteException);
      }
    }
  }
}
