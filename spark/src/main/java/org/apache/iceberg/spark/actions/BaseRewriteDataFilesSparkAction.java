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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BinPackStrategy;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteStrategy;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.relocated.com.google.common.math.IntMath;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class BaseRewriteDataFilesSparkAction
    extends BaseSnapshotUpdateSparkAction<RewriteDataFiles, RewriteDataFiles.Result> implements RewriteDataFiles {

  private static final Logger LOG = LoggerFactory.getLogger(BaseRewriteDataFilesSparkAction.class);
  private static final Set<String> VALID_OPTIONS = ImmutableSet.of(
      MAX_CONCURRENT_FILE_GROUP_REWRITES,
      MAX_FILE_GROUP_SIZE_BYTES,
      PARTIAL_PROGRESS_ENABLED,
      PARTIAL_PROGRESS_MAX_COMMITS,
      TARGET_FILE_SIZE_BYTES
  );

  private final Table table;

  private Expression filter = Expressions.alwaysTrue();
  private int maxConcurrentFileGroupRewrites;
  private int maxCommits;
  private boolean partialProgressEnabled;
  private RewriteStrategy strategy;

  protected BaseRewriteDataFilesSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
    this.strategy = binPackStrategy();
  }

  protected Table table() {
    return table;
  }

  /**
   * Perform a commit operation on the table adding and removing files as
   * required for this set of file groups
   * @param completedGroupIDs fileSets to commit
   */
  protected abstract void commitFileGroups(Set<String> completedGroupIDs);

  /**
   * Clean up a specified file set by removing any files created for that operation, should
   * not throw any exceptions
   * @param groupID fileSet to clean
   */
  protected abstract void abortFileGroup(String groupID);

  /**
   * The framework specific {@link BinPackStrategy}
   */
  protected abstract BinPackStrategy binPackStrategy();

  @Override
  public RewriteDataFiles binPack() {
    this.strategy = binPackStrategy();
    return this;
  }

  @Override
  public RewriteDataFiles filter(Expression expression) {
    filter = Expressions.and(filter, expression);
    return this;
  }

  @Override
  public RewriteDataFiles.Result execute() {
    validateOptions();
    strategy = strategy.options(options());

    Map<StructLike, List<List<FileScanTask>>> fileGroupsByPartition = planFileGroups();
    RewriteExecutionContext ctx = new RewriteExecutionContext(fileGroupsByPartition);
    Stream<FileGroup> groupStream = toGroupStream(ctx, fileGroupsByPartition);

    if (ctx.totalGroupCount() == 0) {
      LOG.info("Nothing found to rewrite in {}", table.name());
      return new Result(Collections.emptyMap());
    }

    if (partialProgressEnabled) {
      return doExecuteWithPartialProgress(ctx, groupStream);
    } else {
      return doExecute(ctx, groupStream);
    }
  }

  private Map<StructLike, List<List<FileScanTask>>> planFileGroups() {
    CloseableIterable<FileScanTask> fileScanTasks = table.newScan()
        .filter(filter)
        .ignoreResiduals()
        .planFiles();

    try {
      Map<StructLike, List<FileScanTask>> filesByPartition = Streams.stream(fileScanTasks)
          .collect(Collectors.groupingBy(task -> task.file().partition()));

      Map<StructLike, List<List<FileScanTask>>> fileGroupsByPartition = Maps.newHashMap();

      filesByPartition.forEach((partition, tasks) -> {
        List<List<FileScanTask>> fileGroups = toFileGroups(tasks);
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

  private List<List<FileScanTask>> toFileGroups(List<FileScanTask> tasks) {
    Iterable<FileScanTask> filtered = strategy.selectFilesToRewrite(tasks);
    Iterable<List<FileScanTask>> groupedTasks = strategy.planFileGroups(filtered);
    return ImmutableList.copyOf(groupedTasks);
  }

  @VisibleForTesting
  void rewriteFiles(RewriteExecutionContext ctx, FileGroup fileGroup,
                    ConcurrentLinkedQueue<String> rewrittenIDs,
                    ConcurrentHashMap<String, Pair<FileGroupInfo, FileGroupRewriteResult>> results) {

    String groupID = fileGroup.groupID();
    int filesInGroup = fileGroup.numFiles();

    String desc = jobDesc(fileGroup, ctx);

    Set<DataFile> addedFiles =
        withJobGroupInfo(
            newJobGroupInfo("REWRITE-DATA-FILES", desc),
            () -> strategy.rewriteFiles(groupID, fileGroup.files()));

    rewrittenIDs.offer(groupID);
    FileGroupRewriteResult fileGroupResult = new FileGroupRewriteResult(addedFiles.size(), filesInGroup);

    results.put(groupID, Pair.of(fileGroup.info(), fileGroupResult));
  }

  private void commitOrClean(Set<String> rewrittenIDs) {
    try {
      commitFileGroups(rewrittenIDs);
    } catch (CommitStateUnknownException e) {
      LOG.error("Commit state unknown for {}, cannot clean up files because they may have been committed successfully.",
          rewrittenIDs, e);
      throw e;
    } catch (Exception e) {
      LOG.error("Cannot commit groups {}, attempting to clean up written files", rewrittenIDs, e);
      Tasks.foreach(rewrittenIDs)
          .suppressFailureWhenFinished()
          .run(this::abortFileGroup);
      throw e;
    }
  }

  private ExecutorService rewriteService() {
    return MoreExecutors.getExitingExecutorService(
        (ThreadPoolExecutor) Executors.newFixedThreadPool(
            maxConcurrentFileGroupRewrites,
            new ThreadFactoryBuilder()
                .setNameFormat("Rewrite-Service-%d")
                .build())
    );
  }

  private Result doExecute(RewriteExecutionContext ctx, Stream<FileGroup> groupStream) {

    ExecutorService rewriteService = rewriteService();

    ConcurrentLinkedQueue<String> rewrittenIDs = new ConcurrentLinkedQueue<>();
    ConcurrentHashMap<String, Pair<FileGroupInfo, FileGroupRewriteResult>> results = new ConcurrentHashMap<>();

    Tasks.Builder<FileGroup> rewriteTaskBuilder = Tasks.foreach(groupStream)
        .executeWith(rewriteService)
        .stopOnFailure()
        .noRetry()
        .onFailure((fileGroup, exception) -> {
          LOG.error("Failure during rewrite process for group {}", fileGroup.info, exception);
        });

    try {
      rewriteTaskBuilder
          .run(fileGroup -> rewriteFiles(ctx, fileGroup, rewrittenIDs, results));
    } catch (Exception e) {
      // At least one rewrite group failed, clean up all completed rewrites
      LOG.error("Cannot complete rewrite, {} is not enabled and one of the file set groups failed to " +
          "be rewritten. This error occurred during the writing of new files, not during the commit process. This" +
          "indicates something is wrong that doesn't involve conflicts with other Iceberg operations. Enabling" +
          "{} may help in this case but the root cause should be investigated. Cleaning up {} groups which finished " +
          "being written.", PARTIAL_PROGRESS_ENABLED, PARTIAL_PROGRESS_ENABLED, rewrittenIDs.size(), e);

      Tasks.foreach(rewrittenIDs)
          .suppressFailureWhenFinished()
          .run(this::abortFileGroup);
      throw e;
    } finally {
      rewriteService.shutdown();
    }

    try {
      commitOrClean(ImmutableSet.copyOf(rewrittenIDs));
    } catch (ValidationException | CommitFailedException e) {
      String errorMessage = String.format(
          "Cannot commit rewrite because of a ValidationException or CommitFailedException. This usually means that" +
              "this rewrite has conflicted with another concurrent Iceberg operation. To reduce the likelihood of" +
              "conflicts, set %s which will break up the rewrite into multiple smaller commits controlled by %s." +
              "Separate smaller rewrite commits can succeed independently while any commits that conflict with" +
              "another Iceberg operation will be ignored. This mode will create additional snapshots in the table " +
              "history, one for each commit.",
          PARTIAL_PROGRESS_ENABLED, PARTIAL_PROGRESS_MAX_COMMITS);
      throw new RuntimeException(errorMessage, e);
    } catch (Exception e) {
      throw e;
    }

    return new Result(results.values().stream().collect(Collectors.toMap(Pair::first, Pair::second)));
  }

  private Result doExecuteWithPartialProgress(RewriteExecutionContext ctx, Stream<FileGroup> groupStream) {

    ExecutorService rewriteService = rewriteService();

    ExecutorService committerService =  Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("Committer-Service")
                .build());

    int groupsPerCommit = IntMath.divide(ctx.totalGroupCount(), maxCommits, RoundingMode.CEILING);

    AtomicBoolean stillRewriting = new AtomicBoolean(true);
    ConcurrentHashMap<String, Pair<FileGroupInfo, FileGroupRewriteResult>> results = new ConcurrentHashMap<>();
    ConcurrentLinkedQueue<String> completedRewriteIds = new ConcurrentLinkedQueue<>();

    // Partial progress commit service
    committerService.execute(() -> {
      while (stillRewriting.get() || completedRewriteIds.size() > 0) {
        Thread.yield();
        // Either we have a full commit group, or we have completed writing and need to commit what is left over
        if (completedRewriteIds.size() >= groupsPerCommit ||
            (!stillRewriting.get() && completedRewriteIds.size() > 0)) {

          Set<String> batch = Sets.newHashSetWithExpectedSize(groupsPerCommit);
          for (int i = 0; i < groupsPerCommit && !completedRewriteIds.isEmpty(); i++) {
            batch.add(completedRewriteIds.poll());
          }

          try {
            commitOrClean(batch);
          } catch (Exception e) {
            batch.forEach(results::remove);
            LOG.error("Failure during rewrite commit process, partial progress enabled. Ignoring", e);
          }
        }
      }
    });

    // Start rewrite tasks
    Tasks.foreach(groupStream.iterator())
        .suppressFailureWhenFinished()
        .executeWith(rewriteService)
        .noRetry()
        .onFailure((fileGroup, exception) -> {
          LOG.error("Failure during rewrite process for group {}", fileGroup.info, exception);
          abortFileGroup(fileGroup.groupID());
        })
        .run(infoListPair -> rewriteFiles(ctx, infoListPair, completedRewriteIds, results));

    stillRewriting.set(false);
    rewriteService.shutdown();
    committerService.shutdown();

    try {
      committerService.awaitTermination(10, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      throw new RuntimeException("Cannot complete commit for rewrite, commit service interrupted", e);
    }

    if (results.size() == 0) {
      LOG.error("{} is true but no rewrite commits succeeded. Check the logs to determine why the individual" +
          "commits failed. If this is persistent it may help to increase {} which will break the rewrite operation" +
          "into smaller commits.", PARTIAL_PROGRESS_ENABLED, PARTIAL_PROGRESS_MAX_COMMITS);
    }

    return new Result(results.values().stream().collect(Collectors.toMap(Pair::first, Pair::second)));
  }

  private Stream<FileGroup> toGroupStream(RewriteExecutionContext ctx,
                                          Map<StructLike, List<List<FileScanTask>>> fileGroupsByPartition) {

    // Todo Add intelligence to the order in which we do rewrites instead of just using partition order
    return fileGroupsByPartition.entrySet().stream()
        .flatMap(
            e -> e.getValue().stream().map(tasks -> {
              int globalIndex = ctx.currentGlobalIndex();
              int partitionIndex = ctx.currentPartitionIndex(e.getKey());
              String groupID = UUID.randomUUID().toString();
              return new FileGroup(new FileGroupInfo(groupID, globalIndex, partitionIndex, e.getKey()), tasks);
            }));
  }

  private void validateOptions() {
    Set<String> validOptions = Sets.newHashSet(strategy.validOptions());
    validOptions.addAll(VALID_OPTIONS);

    Set<String> invalidKeys = Sets.newHashSet(options().keySet());
    invalidKeys.removeAll(validOptions);

    Preconditions.checkArgument(invalidKeys.isEmpty(),
        "Cannot use options %s, they are not supported by RewriteDatafiles or the strategy %s",
        invalidKeys, strategy.name());

    maxConcurrentFileGroupRewrites = PropertyUtil.propertyAsInt(options(),
        MAX_CONCURRENT_FILE_GROUP_REWRITES,
        MAX_CONCURRENT_FILE_GROUP_REWRITES_DEFAULT);

    maxCommits = PropertyUtil.propertyAsInt(options(),
        PARTIAL_PROGRESS_MAX_COMMITS,
        PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT);

    partialProgressEnabled = PropertyUtil.propertyAsBoolean(options(),
        PARTIAL_PROGRESS_ENABLED,
        PARTIAL_PROGRESS_ENABLED_DEFAULT);

    Preconditions.checkArgument(maxConcurrentFileGroupRewrites >= 1,
        "Cannot set %s to %s, the value must be positive.",
        MAX_CONCURRENT_FILE_GROUP_REWRITES, maxConcurrentFileGroupRewrites);

    Preconditions.checkArgument(!partialProgressEnabled || partialProgressEnabled && maxCommits > 0,
        "Cannot set %s to %s, the value must be positive when %s is true",
        PARTIAL_PROGRESS_MAX_COMMITS, maxCommits, PARTIAL_PROGRESS_ENABLED);
  }

  private String jobDesc(FileGroup group, RewriteExecutionContext ctx) {
    StructLike partition = group.partition();
    if (partition.size() > 0) {
      return String.format("Rewriting %d files (%s, file group %d/%d, %s (%d/%d)) in %s",
          group.numFiles(), strategy.name(), group.globalIndex(), ctx.totalGroupCount(), partition,
          group.partitionIndex(), ctx.groupsInPartition(partition), table.name());
    } else {
      return String.format("Rewriting %d files (%s, file group %d/%d) in %s",
          group.numFiles(), strategy.name(), group.globalIndex(), ctx.totalGroupCount(), table.name());
    }
  }

  class Result implements RewriteDataFiles.Result {
    private final Map<RewriteDataFiles.FileGroupInfo, RewriteDataFiles.FileGroupRewriteResult> resultMap;

    Result(
        Map<RewriteDataFiles.FileGroupInfo, RewriteDataFiles.FileGroupRewriteResult> resultMap) {
      this.resultMap = resultMap;
    }

    @Override
    public Map<RewriteDataFiles.FileGroupInfo, RewriteDataFiles.FileGroupRewriteResult> resultMap() {
      return resultMap;
    }
  }

  class FileGroupInfo implements RewriteDataFiles.FileGroupInfo {

    private final String groupID;
    private final int globalIndex;
    private final int partitionIndex;
    private final StructLike partition;

    FileGroupInfo(String groupID, int globalIndex, int partitionIndex, StructLike partition) {
      this.groupID = groupID;
      this.globalIndex = globalIndex;
      this.partitionIndex = partitionIndex;
      this.partition = partition;
    }

    @Override
    public int globalIndex() {
      return globalIndex;
    }

    @Override
    public int partitionIndex() {
      return partitionIndex;
    }

    @Override
    public StructLike partition() {
      return partition;
    }

    @Override
    public String toString() {
      return "FileGroupInfo{" +
          "groupID=" + groupID +
          ", globalIndex=" + globalIndex +
          ", partitionIndex=" + partitionIndex +
          ", partition=" + partition +
          '}';
    }

    public String groupID() {
      return groupID;
    }
  }

  @VisibleForTesting
  class FileGroupRewriteResult implements RewriteDataFiles.FileGroupRewriteResult {
    private final int addedDataFilesCount;
    private final int rewrittenDataFilesCount;

    FileGroupRewriteResult(int addedDataFilesCount, int rewrittenDataFilesCount) {
      this.addedDataFilesCount = addedDataFilesCount;
      this.rewrittenDataFilesCount = rewrittenDataFilesCount;
    }

    @Override
    public int addedDataFilesCount() {
      return this.addedDataFilesCount;
    }

    @Override
    public int rewrittenDataFilesCount() {
      return this.rewrittenDataFilesCount;
    }
  }

  static class FileGroup {
    private final FileGroupInfo info;
    private final List<FileScanTask> files;
    private final int numFiles;

    FileGroup(FileGroupInfo info, List<FileScanTask> files) {
      this.info = info;
      this.files = files;
      this.numFiles = files.size();
    }

    public int numFiles() {
      return numFiles;
    }

    public StructLike partition() {
      return info.partition();
    }

    public String groupID() {
      return info.groupID();
    }

    public Integer globalIndex() {
      return info.globalIndex();
    }

    public Integer partitionIndex() {
      return info.partitionIndex();
    }

    public FileGroupInfo info() {
      return info;
    }

    public List<FileScanTask> files() {
      return files;
    }
  }

  @VisibleForTesting
  static class RewriteExecutionContext {
    private final Map<StructLike, Integer> numGroupsByPartition;
    private final int totalGroupCount;
    private final Map<StructLike, Integer> partitionIndexMap;
    private final AtomicInteger groupIndex;

    RewriteExecutionContext(Map<StructLike, List<List<FileScanTask>>> fileGroupsByPartition) {
      this.numGroupsByPartition = fileGroupsByPartition.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
      this.totalGroupCount = numGroupsByPartition.values().stream()
          .reduce(Integer::sum)
          .orElse(0);
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
