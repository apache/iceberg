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

import java.math.RoundingMode;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.PositionDeletesTable;
import org.apache.iceberg.PositionDeletesTable.PositionDeletesBatchScan;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.actions.BinPackRewritePositionDeletePlanner;
import org.apache.iceberg.actions.FileRewritePlan;
import org.apache.iceberg.actions.ImmutableRewritePositionDeleteFiles;
import org.apache.iceberg.actions.RewritePositionDeleteFiles;
import org.apache.iceberg.actions.RewritePositionDeletesCommitManager;
import org.apache.iceberg.actions.RewritePositionDeletesCommitManager.CommitService;
import org.apache.iceberg.actions.RewritePositionDeletesGroup;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.math.IntMath;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Spark implementation of {@link RewritePositionDeleteFiles}. */
public class RewritePositionDeleteFilesSparkAction
    extends BaseSnapshotUpdateSparkAction<RewritePositionDeleteFilesSparkAction>
    implements RewritePositionDeleteFiles {

  private static final Logger LOG =
      LoggerFactory.getLogger(RewritePositionDeleteFilesSparkAction.class);
  private static final Set<String> VALID_OPTIONS =
      ImmutableSet.of(
          MAX_CONCURRENT_FILE_GROUP_REWRITES,
          PARTIAL_PROGRESS_ENABLED,
          PARTIAL_PROGRESS_MAX_COMMITS,
          REWRITE_JOB_ORDER);
  private static final Result EMPTY_RESULT =
      ImmutableRewritePositionDeleteFiles.Result.builder().build();

  private final Table table;
  private BinPackRewritePositionDeletePlanner planner;
  private final SparkRewritePositionDeleteRunner runner;
  private Expression filter = Expressions.alwaysTrue();

  private int maxConcurrentFileGroupRewrites;
  private int maxCommits;
  private boolean partialProgressEnabled;
  private boolean caseSensitive;

  RewritePositionDeleteFilesSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
    this.runner = new SparkRewritePositionDeleteRunner(spark(), table);
    this.caseSensitive = SparkUtil.caseSensitive(spark);
  }

  @Override
  protected RewritePositionDeleteFilesSparkAction self() {
    return this;
  }

  @Override
  public RewritePositionDeleteFilesSparkAction filter(Expression expression) {
    filter = Expressions.and(filter, expression);
    return this;
  }

  @Override
  public RewritePositionDeleteFiles.Result execute() {
    if (table.currentSnapshot() == null) {
      LOG.info("Nothing found to rewrite in empty table {}", table.name());
      return EMPTY_RESULT;
    }

    this.planner = new BinPackRewritePositionDeletePlanner(table, filter, caseSensitive);

    validateAndInitOptions();

    if (TableUtil.formatVersion(table) >= 3 && !requiresRewriteToDVs()) {
      LOG.info("v2 deletes in {} have already been rewritten to v3 DVs", table.name());
      return EMPTY_RESULT;
    }

    FileRewritePlan<FileGroupInfo, PositionDeletesScanTask, DeleteFile, RewritePositionDeletesGroup>
        plan = planner.plan();

    if (plan.totalGroupCount() == 0) {
      LOG.info("Nothing found to rewrite in {}", table.name());
      return EMPTY_RESULT;
    }

    if (partialProgressEnabled) {
      return doExecuteWithPartialProgress(plan, commitManager());
    } else {
      return doExecute(plan, commitManager());
    }
  }

  private boolean requiresRewriteToDVs() {
    PositionDeletesBatchScan scan =
        (PositionDeletesBatchScan)
            MetadataTableUtils.createMetadataTableInstance(
                    table, MetadataTableType.POSITION_DELETES)
                .newBatchScan();
    try (CloseableIterator<PositionDeletesScanTask> it =
        CloseableIterable.filter(
                CloseableIterable.transform(
                    scan.baseTableFilter(filter)
                        .caseSensitive(caseSensitive)
                        .select(PositionDeletesTable.DELETE_FILE_PATH)
                        .ignoreResiduals()
                        .planFiles(),
                    task -> (PositionDeletesScanTask) task),
                t -> t.file().format() != FileFormat.PUFFIN)
            .iterator()) {
      return it.hasNext();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private RewritePositionDeletesGroup rewriteDeleteFiles(
      FileRewritePlan<
              FileGroupInfo, PositionDeletesScanTask, DeleteFile, RewritePositionDeletesGroup>
          plan,
      RewritePositionDeletesGroup fileGroup) {
    String desc = jobDesc(fileGroup, plan);
    Set<DeleteFile> addedFiles =
        withJobGroupInfo(
            newJobGroupInfo("REWRITE-POSITION-DELETES", desc), () -> runner.rewrite(fileGroup));

    fileGroup.setOutputFiles(addedFiles);
    LOG.info("Rewrite position deletes ready to be committed - {}", desc);
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

  private RewritePositionDeletesCommitManager commitManager() {
    return new RewritePositionDeletesCommitManager(table, commitSummary());
  }

  private Result doExecute(
      FileRewritePlan<
              FileGroupInfo, PositionDeletesScanTask, DeleteFile, RewritePositionDeletesGroup>
          plan,
      RewritePositionDeletesCommitManager commitManager) {
    ExecutorService rewriteService = rewriteService();

    ConcurrentLinkedQueue<RewritePositionDeletesGroup> rewrittenGroups =
        Queues.newConcurrentLinkedQueue();

    Tasks.Builder<RewritePositionDeletesGroup> rewriteTaskBuilder =
        Tasks.foreach(plan.groups())
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
      rewriteTaskBuilder.run(fileGroup -> rewrittenGroups.add(rewriteDeleteFiles(plan, fileGroup)));
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

      Tasks.foreach(rewrittenGroups).suppressFailureWhenFinished().run(commitManager::abort);
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
        rewrittenGroups.stream()
            .map(RewritePositionDeletesGroup::asResult)
            .collect(Collectors.toList());

    return ImmutableRewritePositionDeleteFiles.Result.builder()
        .rewriteResults(rewriteResults)
        .build();
  }

  private Result doExecuteWithPartialProgress(
      FileRewritePlan<
              FileGroupInfo, PositionDeletesScanTask, DeleteFile, RewritePositionDeletesGroup>
          plan,
      RewritePositionDeletesCommitManager commitManager) {
    ExecutorService rewriteService = rewriteService();

    // start commit service
    int groupsPerCommit = IntMath.divide(plan.totalGroupCount(), maxCommits, RoundingMode.CEILING);
    CommitService commitService = commitManager.service(groupsPerCommit);
    commitService.start();

    // start rewrite tasks
    Tasks.foreach(plan.groups())
        .suppressFailureWhenFinished()
        .executeWith(rewriteService)
        .noRetry()
        .onFailure(
            (fileGroup, exception) ->
                LOG.error("Failure during rewrite group {}", fileGroup.info(), exception))
        .run(fileGroup -> commitService.offer(rewriteDeleteFiles(plan, fileGroup)));
    rewriteService.shutdown();

    // stop commit service
    commitService.close();
    List<RewritePositionDeletesGroup> commitResults = commitService.results();
    if (commitResults.isEmpty()) {
      LOG.error(
          "{} is true but no rewrite commits succeeded. Check the logs to determine why the individual "
              + "commits failed. If this is persistent it may help to increase {} which will break the rewrite operation "
              + "into smaller commits.",
          PARTIAL_PROGRESS_ENABLED,
          PARTIAL_PROGRESS_MAX_COMMITS);
    }

    List<FileGroupRewriteResult> rewriteResults =
        commitResults.stream()
            .map(RewritePositionDeletesGroup::asResult)
            .collect(Collectors.toList());
    return ImmutableRewritePositionDeleteFiles.Result.builder()
        .rewriteResults(rewriteResults)
        .build();
  }

  private void validateAndInitOptions() {
    Set<String> validOptions = Sets.newHashSet(planner.validOptions());
    validOptions.addAll(VALID_OPTIONS);
    validOptions.addAll(planner.validOptions());

    Set<String> invalidKeys = Sets.newHashSet(options().keySet());
    invalidKeys.removeAll(validOptions);

    Preconditions.checkArgument(
        invalidKeys.isEmpty(),
        "Cannot use options %s, they are not supported by the action or the rewriter %s",
        invalidKeys,
        runner.description());

    planner.init(options());
    runner.init(options());

    this.maxConcurrentFileGroupRewrites =
        PropertyUtil.propertyAsInt(
            options(),
            MAX_CONCURRENT_FILE_GROUP_REWRITES,
            MAX_CONCURRENT_FILE_GROUP_REWRITES_DEFAULT);

    this.maxCommits =
        PropertyUtil.propertyAsInt(
            options(), PARTIAL_PROGRESS_MAX_COMMITS, PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT);

    this.partialProgressEnabled =
        PropertyUtil.propertyAsBoolean(
            options(), PARTIAL_PROGRESS_ENABLED, PARTIAL_PROGRESS_ENABLED_DEFAULT);

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

  private String jobDesc(
      RewritePositionDeletesGroup group,
      FileRewritePlan<
              FileGroupInfo, PositionDeletesScanTask, DeleteFile, RewritePositionDeletesGroup>
          plan) {
    StructLike partition = group.info().partition();
    if (partition.size() > 0) {
      return String.format(
          Locale.ROOT,
          "Rewriting %d position delete files (%s, file group %d/%d, %s (%d/%d)) in %s",
          group.rewrittenDeleteFiles().size(),
          runner.description(),
          group.info().globalIndex(),
          plan.totalGroupCount(),
          partition,
          group.info().partitionIndex(),
          plan.groupsInPartition(partition),
          table.name());
    } else {
      return String.format(
          Locale.ROOT,
          "Rewriting %d position files (%s, file group %d/%d) in %s",
          group.rewrittenDeleteFiles().size(),
          runner.description(),
          group.info().globalIndex(),
          plan.totalGroupCount(),
          table.name());
    }
  }
}
