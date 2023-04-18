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
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.FileRewriter;
import org.apache.iceberg.actions.ImmutableRewriteDataFiles;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteDataFilesCommitManager;
import org.apache.iceberg.actions.RewriteFileGroup;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.math.IntMath;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RewriteDataFilesSparkAction
    extends BaseSnapshotUpdateSparkAction<RewriteDataFilesSparkAction> implements RewriteDataFiles {

  private static final Logger LOG = LoggerFactory.getLogger(RewriteDataFilesSparkAction.class);
  private static final Set<String> VALID_OPTIONS =
      ImmutableSet.of(
          MAX_CONCURRENT_FILE_GROUP_REWRITES,
          MAX_FILE_GROUP_SIZE_BYTES,
          PARTIAL_PROGRESS_ENABLED,
          PARTIAL_PROGRESS_MAX_COMMITS,
          TARGET_FILE_SIZE_BYTES,
          USE_STARTING_SEQUENCE_NUMBER,
          REWRITE_JOB_ORDER);

  private final Table table;

  private Expression filter = Expressions.alwaysTrue();
  private int maxConcurrentFileGroupRewrites;
  private int maxCommits;
  private boolean partialProgressEnabled;
  private boolean useStartingSequenceNumber;
  private RewriteJobOrder rewriteJobOrder;
  private FileRewriter<FileScanTask, DataFile> rewriter = null;

  RewriteDataFilesSparkAction(SparkSession spark, Table table) {
    super(spark.cloneSession());
    // Disable Adaptive Query Execution as this may change the output partitioning of our write
    spark().conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), false);
    this.table = table;
  }

  @Override
  protected RewriteDataFilesSparkAction self() {
    return this;
  }

  @Override
  public RewriteDataFilesSparkAction binPack() {
    Preconditions.checkArgument(
        rewriter == null, "Must use only one rewriter type (bin-pack, sort, zorder)");
    this.rewriter = new SparkBinPackDataRewriter(spark(), table);
    return this;
  }

  @Override
  public RewriteDataFilesSparkAction sort(SortOrder sortOrder) {
    Preconditions.checkArgument(
        rewriter == null, "Must use only one rewriter type (bin-pack, sort, zorder)");
    this.rewriter = new SparkSortDataRewriter(spark(), table, sortOrder);
    return this;
  }

  @Override
  public RewriteDataFilesSparkAction sort() {
    Preconditions.checkArgument(
        rewriter == null, "Must use only one rewriter type (bin-pack, sort, zorder)");
    this.rewriter = new SparkSortDataRewriter(spark(), table);
    return this;
  }

  @Override
  public RewriteDataFilesSparkAction zOrder(String... columnNames) {
    Preconditions.checkArgument(
        rewriter == null, "Must use only one rewriter type (bin-pack, sort, zorder)");
    this.rewriter = new SparkZOrderDataRewriter(spark(), table, Arrays.asList(columnNames));
    return this;
  }

  @Override
  public RewriteDataFilesSparkAction filter(Expression expression) {
    filter = Expressions.and(filter, expression);
    return this;
  }

  @Override
  public RewriteDataFiles.Result execute() {
    if (table.currentSnapshot() == null) {
      return ImmutableRewriteDataFiles.Result.builder().rewriteResults(ImmutableList.of()).build();
    }

    long startingSnapshotId = table.currentSnapshot().snapshotId();

    // Default to BinPack if no strategy selected
    if (this.rewriter == null) {
      this.rewriter = new SparkBinPackDataRewriter(spark(), table);
    }

    validateAndInitOptions();

    Map<StructLike, List<List<FileScanTask>>> fileGroupsByPartition =
        planFileGroups(startingSnapshotId);
    RewriteExecutionContext ctx = new RewriteExecutionContext(fileGroupsByPartition);

    if (ctx.totalGroupCount() == 0) {
      LOG.info("Nothing found to rewrite in {}", table.name());
      return ImmutableRewriteDataFiles.Result.builder().rewriteResults(ImmutableList.of()).build();
    }

    Stream<RewriteFileGroup> groupStream = toGroupStream(ctx, fileGroupsByPartition);

    RewriteDataFilesCommitManager commitManager = commitManager(startingSnapshotId);
    if (partialProgressEnabled) {
      return doExecuteWithPartialProgress(ctx, groupStream, commitManager);
    } else {
      return doExecute(ctx, groupStream, commitManager);
    }
  }

  Map<StructLike, List<List<FileScanTask>>> planFileGroups(long startingSnapshotId) {
    CloseableIterable<FileScanTask> fileScanTasks =
        table
            .newScan()
            .useSnapshot(startingSnapshotId)
            .filter(filter)
            .ignoreResiduals()
            .planFiles();

    try {
      StructType partitionType = table.spec().partitionType();
      StructLikeMap<List<FileScanTask>> filesByPartition = StructLikeMap.create(partitionType);
      StructLike emptyStruct = GenericRecord.create(partitionType);

      fileScanTasks.forEach(
          task -> {
            // If a task uses an incompatible partition spec the data inside could contain values
            // which
            // belong to multiple partitions in the current spec. Treating all such files as
            // un-partitioned and
            // grouping them together helps to minimize new files made.
            StructLike taskPartition =
                task.file().specId() == table.spec().specId()
                    ? task.file().partition()
                    : emptyStruct;

            List<FileScanTask> files = filesByPartition.get(taskPartition);
            if (files == null) {
              files = Lists.newArrayList();
            }

            files.add(task);
            filesByPartition.put(taskPartition, files);
          });

      StructLikeMap<List<List<FileScanTask>>> fileGroupsByPartition =
          StructLikeMap.create(partitionType);

      filesByPartition.forEach(
          (partition, tasks) -> {
            Iterable<List<FileScanTask>> plannedFileGroups = rewriter.planFileGroups(tasks);
            List<List<FileScanTask>> fileGroups = ImmutableList.copyOf(plannedFileGroups);
            if (fileGroups.size() > 0) {
              fileGroupsByPartition.put(partition, fileGroups);
            }
          });

      return fileGroupsByPartition;
    } finally {
      try {
        fileScanTasks.close();
      } catch (IOException io) {
        LOG.error("Cannot properly close file iterable while planning for rewrite", io);
      }
    }
  }

  @VisibleForTesting
  RewriteFileGroup rewriteFiles(RewriteExecutionContext ctx, RewriteFileGroup fileGroup) {
    String desc = jobDesc(fileGroup, ctx);
    Set<DataFile> addedFiles =
        withJobGroupInfo(
            newJobGroupInfo("REWRITE-DATA-FILES", desc),
            () -> rewriter.rewrite(fileGroup.fileScans()));

    fileGroup.setOutputFiles(addedFiles);
    LOG.info("Rewrite Files Ready to be Committed - {}", desc);
    return fileGroup;
  }

  private ExecutorService rewriteService() {
    return MoreExecutors.getExitingExecutorService(
        (ThreadPoolExecutor)
            Executors.newFixedThreadPool(
                maxConcurrentFileGroupRewrites,
                new ThreadFactoryBuilder().setNameFormat("Rewrite-Service-%d").build()));
  }

  @VisibleForTesting
  RewriteDataFilesCommitManager commitManager(long startingSnapshotId) {
    return new RewriteDataFilesCommitManager(table, startingSnapshotId, useStartingSequenceNumber);
  }

  private Result doExecute(
      RewriteExecutionContext ctx,
      Stream<RewriteFileGroup> groupStream,
      RewriteDataFilesCommitManager commitManager) {
    ExecutorService rewriteService = rewriteService();

    ConcurrentLinkedQueue<RewriteFileGroup> rewrittenGroups = Queues.newConcurrentLinkedQueue();

    Tasks.Builder<RewriteFileGroup> rewriteTaskBuilder =
        Tasks.foreach(groupStream)
            .executeWith(rewriteService)
            .stopOnFailure()
            .noRetry()
            .onFailure(
                (fileGroup, exception) -> {
                  LOG.warn(
                      "Failure during rewrite process for group {}", fileGroup.info(), exception);
                });

    try {
      rewriteTaskBuilder.run(
          fileGroup -> {
            rewrittenGroups.add(rewriteFiles(ctx, fileGroup));
          });
    } catch (Exception e) {
      // At least one rewrite group failed, clean up all completed rewrites
      LOG.error(
          "Cannot complete rewrite, {} is not enabled and one of the file set groups failed to "
              + "be rewritten. This error occurred during the writing of new files, not during the commit process. This "
              + "indicates something is wrong that doesn't involve conflicts with other Iceberg operations. Enabling "
              + "{} may help in this case but the root cause should be investigated. Cleaning up {} groups which finished "
              + "being written.",
          PARTIAL_PROGRESS_ENABLED,
          PARTIAL_PROGRESS_ENABLED,
          rewrittenGroups.size(),
          e);

      Tasks.foreach(rewrittenGroups)
          .suppressFailureWhenFinished()
          .run(group -> commitManager.abortFileGroup(group));
      throw e;
    } finally {
      rewriteService.shutdown();
    }

    try {
      commitManager.commitOrClean(Sets.newHashSet(rewrittenGroups));
    } catch (ValidationException | CommitFailedException e) {
      String errorMessage =
          String.format(
              "Cannot commit rewrite because of a ValidationException or CommitFailedException. This usually means that "
                  + "this rewrite has conflicted with another concurrent Iceberg operation. To reduce the likelihood of "
                  + "conflicts, set %s which will break up the rewrite into multiple smaller commits controlled by %s. "
                  + "Separate smaller rewrite commits can succeed independently while any commits that conflict with "
                  + "another Iceberg operation will be ignored. This mode will create additional snapshots in the table "
                  + "history, one for each commit.",
              PARTIAL_PROGRESS_ENABLED, PARTIAL_PROGRESS_MAX_COMMITS);
      throw new RuntimeException(errorMessage, e);
    }

    List<FileGroupRewriteResult> rewriteResults =
        rewrittenGroups.stream().map(RewriteFileGroup::asResult).collect(Collectors.toList());
    return ImmutableRewriteDataFiles.Result.builder().rewriteResults(rewriteResults).build();
  }

  private Result doExecuteWithPartialProgress(
      RewriteExecutionContext ctx,
      Stream<RewriteFileGroup> groupStream,
      RewriteDataFilesCommitManager commitManager) {
    ExecutorService rewriteService = rewriteService();

    // Start Commit Service
    int groupsPerCommit = IntMath.divide(ctx.totalGroupCount(), maxCommits, RoundingMode.CEILING);
    RewriteDataFilesCommitManager.CommitService commitService =
        commitManager.service(groupsPerCommit);
    commitService.start();

    // Start rewrite tasks
    Tasks.foreach(groupStream)
        .suppressFailureWhenFinished()
        .executeWith(rewriteService)
        .noRetry()
        .onFailure(
            (fileGroup, exception) ->
                LOG.error("Failure during rewrite group {}", fileGroup.info(), exception))
        .run(fileGroup -> commitService.offer(rewriteFiles(ctx, fileGroup)));
    rewriteService.shutdown();

    // Stop Commit service
    commitService.close();
    List<RewriteFileGroup> commitResults = commitService.results();
    if (commitResults.size() == 0) {
      LOG.error(
          "{} is true but no rewrite commits succeeded. Check the logs to determine why the individual "
              + "commits failed. If this is persistent it may help to increase {} which will break the rewrite operation "
              + "into smaller commits.",
          PARTIAL_PROGRESS_ENABLED,
          PARTIAL_PROGRESS_MAX_COMMITS);
    }

    List<FileGroupRewriteResult> rewriteResults =
        commitResults.stream().map(RewriteFileGroup::asResult).collect(Collectors.toList());
    return ImmutableRewriteDataFiles.Result.builder().rewriteResults(rewriteResults).build();
  }

  Stream<RewriteFileGroup> toGroupStream(
      RewriteExecutionContext ctx,
      Map<StructLike, List<List<FileScanTask>>> fileGroupsByPartition) {
    Stream<RewriteFileGroup> rewriteFileGroupStream =
        fileGroupsByPartition.entrySet().stream()
            .flatMap(
                e -> {
                  StructLike partition = e.getKey();
                  List<List<FileScanTask>> fileGroups = e.getValue();
                  return fileGroups.stream()
                      .map(
                          tasks -> {
                            int globalIndex = ctx.currentGlobalIndex();
                            int partitionIndex = ctx.currentPartitionIndex(partition);
                            FileGroupInfo info =
                                ImmutableRewriteDataFiles.FileGroupInfo.builder()
                                    .globalIndex(globalIndex)
                                    .partitionIndex(partitionIndex)
                                    .partition(partition)
                                    .build();
                            return new RewriteFileGroup(info, tasks);
                          });
                });

    return rewriteFileGroupStream.sorted(rewriteGroupComparator());
  }

  private Comparator<RewriteFileGroup> rewriteGroupComparator() {
    switch (rewriteJobOrder) {
      case BYTES_ASC:
        return Comparator.comparing(RewriteFileGroup::sizeInBytes);
      case BYTES_DESC:
        return Comparator.comparing(RewriteFileGroup::sizeInBytes, Comparator.reverseOrder());
      case FILES_ASC:
        return Comparator.comparing(RewriteFileGroup::numFiles);
      case FILES_DESC:
        return Comparator.comparing(RewriteFileGroup::numFiles, Comparator.reverseOrder());
      default:
        return (fileGroupOne, fileGroupTwo) -> 0;
    }
  }

  void validateAndInitOptions() {
    Set<String> validOptions = Sets.newHashSet(rewriter.validOptions());
    validOptions.addAll(VALID_OPTIONS);

    Set<String> invalidKeys = Sets.newHashSet(options().keySet());
    invalidKeys.removeAll(validOptions);

    Preconditions.checkArgument(
        invalidKeys.isEmpty(),
        "Cannot use options %s, they are not supported by the action or the rewriter %s",
        invalidKeys,
        rewriter.description());

    rewriter.init(options());

    maxConcurrentFileGroupRewrites =
        PropertyUtil.propertyAsInt(
            options(),
            MAX_CONCURRENT_FILE_GROUP_REWRITES,
            MAX_CONCURRENT_FILE_GROUP_REWRITES_DEFAULT);

    maxCommits =
        PropertyUtil.propertyAsInt(
            options(), PARTIAL_PROGRESS_MAX_COMMITS, PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT);

    partialProgressEnabled =
        PropertyUtil.propertyAsBoolean(
            options(), PARTIAL_PROGRESS_ENABLED, PARTIAL_PROGRESS_ENABLED_DEFAULT);

    useStartingSequenceNumber =
        PropertyUtil.propertyAsBoolean(
            options(), USE_STARTING_SEQUENCE_NUMBER, USE_STARTING_SEQUENCE_NUMBER_DEFAULT);

    rewriteJobOrder =
        RewriteJobOrder.fromName(
            PropertyUtil.propertyAsString(options(), REWRITE_JOB_ORDER, REWRITE_JOB_ORDER_DEFAULT));

    Preconditions.checkArgument(
        maxConcurrentFileGroupRewrites >= 1,
        "Cannot set %s to %s, the value must be positive.",
        MAX_CONCURRENT_FILE_GROUP_REWRITES,
        maxConcurrentFileGroupRewrites);

    Preconditions.checkArgument(
        !partialProgressEnabled || maxCommits > 0,
        "Cannot set %s to %s, the value must be positive when %s is true",
        PARTIAL_PROGRESS_MAX_COMMITS,
        maxCommits,
        PARTIAL_PROGRESS_ENABLED);
  }

  private String jobDesc(RewriteFileGroup group, RewriteExecutionContext ctx) {
    StructLike partition = group.info().partition();
    if (partition.size() > 0) {
      return String.format(
          "Rewriting %d files (%s, file group %d/%d, %s (%d/%d)) in %s",
          group.rewrittenFiles().size(),
          rewriter.description(),
          group.info().globalIndex(),
          ctx.totalGroupCount(),
          partition,
          group.info().partitionIndex(),
          ctx.groupsInPartition(partition),
          table.name());
    } else {
      return String.format(
          "Rewriting %d files (%s, file group %d/%d) in %s",
          group.rewrittenFiles().size(),
          rewriter.description(),
          group.info().globalIndex(),
          ctx.totalGroupCount(),
          table.name());
    }
  }

  @VisibleForTesting
  static class RewriteExecutionContext {
    private final Map<StructLike, Integer> numGroupsByPartition;
    private final int totalGroupCount;
    private final Map<StructLike, Integer> partitionIndexMap;
    private final AtomicInteger groupIndex;

    RewriteExecutionContext(Map<StructLike, List<List<FileScanTask>>> fileGroupsByPartition) {
      this.numGroupsByPartition =
          fileGroupsByPartition.entrySet().stream()
              .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
      this.totalGroupCount = numGroupsByPartition.values().stream().reduce(Integer::sum).orElse(0);
      this.partitionIndexMap = Maps.newConcurrentMap();
      this.groupIndex = new AtomicInteger(1);
    }

    public int currentGlobalIndex() {
      return groupIndex.getAndIncrement();
    }

    public int currentPartitionIndex(StructLike partition) {
      return partitionIndexMap.merge(partition, 1, Integer::sum);
    }

    public int groupsInPartition(StructLike partition) {
      return numGroupsByPartition.get(partition);
    }

    public int totalGroupCount() {
      return totalGroupCount;
    }
  }
}
