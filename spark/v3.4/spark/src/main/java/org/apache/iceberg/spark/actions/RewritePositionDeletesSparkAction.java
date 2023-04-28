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
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ImmutablePositionDeleteGroupInfo;
import org.apache.iceberg.actions.ImmutableResult;
import org.apache.iceberg.actions.RewritePositionDeleteFiles;
import org.apache.iceberg.actions.RewritePositionDeleteGroup;
import org.apache.iceberg.actions.RewritePositionDeletesCommitManager;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
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

/** Spark implementation of {@link org.apache.iceberg.actions.RewritePositionDeleteFiles}. */
public class RewritePositionDeletesSparkAction
    extends BaseSnapshotUpdateSparkAction<RewritePositionDeletesSparkAction>
    implements RewritePositionDeleteFiles {

  private static final Logger LOG =
      LoggerFactory.getLogger(RewritePositionDeletesSparkAction.class);
  private static final Set<String> VALID_OPTIONS =
      ImmutableSet.of(
          MAX_CONCURRENT_FILE_GROUP_REWRITES,
          PARTIAL_PROGRESS_ENABLED,
          PARTIAL_PROGRESS_MAX_COMMITS,
          REWRITE_JOB_ORDER);

  private final Table table;
  private final SparkPositionDeletesRewriter rewriter;

  private int maxConcurrentFileGroupRewrites;
  private int maxCommits;
  private boolean partialProgressEnabled;
  private RewriteJobOrder rewriteJobOrder;

  RewritePositionDeletesSparkAction(SparkSession spark, Table table) {
    super(spark.cloneSession());

    // Disable Adaptive Query Execution as this may change the output partitioning of our write
    spark().conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), false);
    this.table = table;
    this.rewriter = new SparkPositionDeletesRewriter(spark, table);
  }

  @Override
  protected RewritePositionDeletesSparkAction self() {
    return this;
  }

  @Override
  public RewritePositionDeletesSparkAction filter(Expression expression) {
    throw new UnsupportedOperationException("Regular filters not supported yet.");
  }

  @Override
  public RewritePositionDeleteFiles.Result execute() {
    if (table.currentSnapshot() == null) {
      LOG.info("Nothing found to rewrite in empty table {}", table.name());
      return ImmutableResult.builder()
          .rewrittenDeleteFilesCount(0)
          .addedDeleteFilesCount(0)
          .rewrittenBytesCount(0)
          .addedBytesCount(0)
          .build();
    }

    validateAndInitOptions();

    Map<StructLike, List<List<PositionDeletesScanTask>>> fileGroupsByPartition = planFileGroups();
    RewriteExecutionContext ctx = new RewriteExecutionContext(fileGroupsByPartition);

    if (ctx.totalGroupCount() == 0) {
      LOG.info("Nothing found to rewrite in {}", table.name());
      return ImmutableResult.builder()
          .rewrittenDeleteFilesCount(0)
          .addedDeleteFilesCount(0)
          .rewrittenBytesCount(0)
          .addedBytesCount(0)
          .build();
    }

    Stream<RewritePositionDeleteGroup> groupStream = toGroupStream(ctx, fileGroupsByPartition);

    RewritePositionDeletesCommitManager commitManager = commitManager();
    if (partialProgressEnabled) {
      return doExecuteWithPartialProgress(ctx, groupStream, commitManager);
    } else {
      return doExecute(ctx, groupStream, commitManager);
    }
  }

  Map<StructLike, List<List<PositionDeletesScanTask>>> planFileGroups() {
    Table deletesTable =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.POSITION_DELETES);
    CloseableIterable<PositionDeletesScanTask> scanTasks =
        CloseableIterable.transform(
            deletesTable.newBatchScan().ignoreResiduals().planFiles(),
            t -> (PositionDeletesScanTask) t);

    try {
      StructType partitionType = table.spec().partitionType();
      StructLikeMap<List<PositionDeletesScanTask>> filesByPartition =
          StructLikeMap.create(partitionType);
      StructLike emptyStruct = GenericRecord.create(partitionType);

      scanTasks.forEach(
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

            List<PositionDeletesScanTask> files = filesByPartition.get(taskPartition);
            if (files == null) {
              files = Lists.newArrayList();
            }

            files.add(task);
            filesByPartition.put(taskPartition, files);
          });

      StructLikeMap<List<List<PositionDeletesScanTask>>> fileGroupsByPartition =
          StructLikeMap.create(partitionType);

      filesByPartition.forEach(
          (partition, tasks) -> {
            Iterable<List<PositionDeletesScanTask>> plannedFileGroups =
                rewriter.planFileGroups(tasks);
            List<List<PositionDeletesScanTask>> fileGroups =
                ImmutableList.copyOf(plannedFileGroups);
            if (fileGroups.size() > 0) {
              fileGroupsByPartition.put(partition, fileGroups);
            }
          });

      return fileGroupsByPartition;
    } finally {
      try {
        scanTasks.close();
      } catch (IOException io) {
        LOG.error("Cannot properly close file iterable while planning for rewrite", io);
      }
    }
  }

  @VisibleForTesting
  RewritePositionDeleteGroup rewriteDeleteFiles(
      RewriteExecutionContext ctx, RewritePositionDeleteGroup fileGroup) {
    String desc = jobDesc(fileGroup, ctx);
    Set<DeleteFile> addedFiles =
        withJobGroupInfo(
            newJobGroupInfo("REWRITE-POSITION-DELETES", desc),
            () -> rewriter.rewrite(fileGroup.scans()));

    fileGroup.setOutputFiles(addedFiles);
    LOG.info("Rewrite Position Deletes Ready to be Committed - {}", desc);
    return fileGroup;
  }

  private ExecutorService rewriteService() {
    return MoreExecutors.getExitingExecutorService(
        (ThreadPoolExecutor)
            Executors.newFixedThreadPool(
                maxConcurrentFileGroupRewrites,
                new ThreadFactoryBuilder()
                    .setNameFormat("Rewrite-Position-Delete-Service-%d")
                    .build()));
  }

  @VisibleForTesting
  RewritePositionDeletesCommitManager commitManager() {
    return new RewritePositionDeletesCommitManager(table);
  }

  private Result doExecute(
      RewriteExecutionContext ctx,
      Stream<RewritePositionDeleteGroup> groupStream,
      RewritePositionDeletesCommitManager commitManager) {
    ExecutorService rewriteService = rewriteService();

    ConcurrentLinkedQueue<RewritePositionDeleteGroup> rewrittenGroups =
        Queues.newConcurrentLinkedQueue();

    Tasks.Builder<RewritePositionDeleteGroup> rewriteTaskBuilder =
        Tasks.foreach(groupStream)
            .executeWith(rewriteService)
            .stopOnFailure()
            .noRetry()
            .onFailure(
                (fileGroup, exception) ->
                    LOG.warn(
                        "Failure during rewrite process for group {}",
                        fileGroup.info(),
                        exception));

    try {
      rewriteTaskBuilder.run(fileGroup -> rewrittenGroups.add(rewriteDeleteFiles(ctx, fileGroup)));
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
          .run(commitManager::abortFileGroup);
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

    List<PositionDeleteGroupRewriteResult> rewriteResults =
        rewrittenGroups.stream()
            .map(RewritePositionDeleteGroup::asResult)
            .collect(Collectors.toList());
    int addedDeletes = rewrittenGroups.stream().mapToInt(g -> g.addedDeleteFiles().size()).sum();
    long addedBytes =
        rewrittenGroups.stream().mapToLong(RewritePositionDeleteGroup::addedBytes).sum();
    int rewrittenDeletes =
        rewrittenGroups.stream().mapToInt(g -> g.rewrittenDeleteFiles().size()).sum();
    long rewrittenBytes =
        rewrittenGroups.stream().mapToLong(RewritePositionDeleteGroup::rewrittenBytes).sum();

    return ImmutableResult.builder()
        .rewriteResults(rewriteResults)
        .addedDeleteFilesCount(addedDeletes)
        .addedBytesCount(addedBytes)
        .rewrittenBytesCount(rewrittenBytes)
        .rewrittenDeleteFilesCount(rewrittenDeletes)
        .rewriteResults(rewriteResults)
        .build();
  }

  private Result doExecuteWithPartialProgress(
      RewriteExecutionContext ctx,
      Stream<RewritePositionDeleteGroup> groupStream,
      RewritePositionDeletesCommitManager commitManager) {
    ExecutorService rewriteService = rewriteService();

    // Start Commit Service
    int groupsPerCommit = IntMath.divide(ctx.totalGroupCount(), maxCommits, RoundingMode.CEILING);
    RewritePositionDeletesCommitManager.CommitService commitService =
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
        .run(fileGroup -> commitService.offer(rewriteDeleteFiles(ctx, fileGroup)));
    rewriteService.shutdown();

    // Stop Commit service
    commitService.close();
    List<RewritePositionDeleteGroup> commitResults = commitService.results();
    if (commitResults.size() == 0) {
      LOG.error(
          "{} is true but no rewrite commits succeeded. Check the logs to determine why the individual "
              + "commits failed. If this is persistent it may help to increase {} which will break the rewrite operation "
              + "into smaller commits.",
          PARTIAL_PROGRESS_ENABLED,
          PARTIAL_PROGRESS_MAX_COMMITS);
    }

    List<PositionDeleteGroupRewriteResult> rewriteResults =
        commitResults.stream()
            .map(RewritePositionDeleteGroup::asResult)
            .collect(Collectors.toList());
    return ImmutableResult.builder().rewriteResults(rewriteResults).build();
  }

  Stream<RewritePositionDeleteGroup> toGroupStream(
      RewriteExecutionContext ctx,
      Map<StructLike, List<List<PositionDeletesScanTask>>> fileGroupsByPartition) {
    Stream<RewritePositionDeleteGroup> rewriteFileGroupStream =
        fileGroupsByPartition.entrySet().stream()
            .flatMap(
                e -> {
                  StructLike partition = e.getKey();
                  List<List<PositionDeletesScanTask>> scanGroups = e.getValue();
                  return scanGroups.stream()
                      .map(
                          tasks -> {
                            int globalIndex = ctx.currentGlobalIndex();
                            int partitionIndex = ctx.currentPartitionIndex(partition);
                            PositionDeleteGroupInfo info =
                                ImmutablePositionDeleteGroupInfo.builder()
                                    .globalIndex(globalIndex)
                                    .partitionIndex(partitionIndex)
                                    .partition(partition)
                                    .build();
                            return new RewritePositionDeleteGroup(info, tasks);
                          });
                });

    return rewriteFileGroupStream.sorted(rewriteGroupComparator());
  }

  private Comparator<RewritePositionDeleteGroup> rewriteGroupComparator() {
    switch (rewriteJobOrder) {
      case BYTES_ASC:
        return Comparator.comparing(RewritePositionDeleteGroup::rewrittenBytes);
      case BYTES_DESC:
        return Comparator.comparing(
            RewritePositionDeleteGroup::rewrittenBytes, Comparator.reverseOrder());
      case FILES_ASC:
        return Comparator.comparing(RewritePositionDeleteGroup::numDeleteFiles);
      case FILES_DESC:
        return Comparator.comparing(
            RewritePositionDeleteGroup::numDeleteFiles, Comparator.reverseOrder());
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

  private String jobDesc(RewritePositionDeleteGroup group, RewriteExecutionContext ctx) {
    StructLike partition = group.info().partition();
    if (partition.size() > 0) {
      return String.format(
          "Rewriting %d position delete files (%s, file group %d/%d, %s (%d/%d)) in %s",
          group.rewrittenDeleteFiles().size(),
          rewriter.description(),
          group.info().globalIndex(),
          ctx.totalGroupCount(),
          partition,
          group.info().partitionIndex(),
          ctx.groupsInPartition(partition),
          table.name());
    } else {
      return String.format(
          "Rewriting %d position files (%s, file group %d/%d) in %s",
          group.rewrittenDeleteFiles().size(),
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

    RewriteExecutionContext(
        Map<StructLike, List<List<PositionDeletesScanTask>>> fileGroupsByPartition) {
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
