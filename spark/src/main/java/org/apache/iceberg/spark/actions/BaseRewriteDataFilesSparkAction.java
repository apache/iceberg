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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.BinPackStrategy;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteStrategy;
import org.apache.iceberg.actions.SortStrategy;
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

  /**
   * The framework specific {@link SortStrategy}
   */
  protected abstract SortStrategy sortStrategy();

  @Override
  public RewriteDataFiles sort(SortOrder sortOrder) {
    this.strategy = sortStrategy().sortOrder(sortOrder);
    return this;
  }

  @Override
  public RewriteDataFiles sort() {
    this.strategy = sortStrategy();
    return this;
  }

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
  public Result execute() {
    validateOptions();
    strategy = strategy.options(options());

    Map<StructLike, List<List<FileScanTask>>> fileGroupsByPartition = planFileGroups();
    RewriteExecutionContext ctx = new RewriteExecutionContext(fileGroupsByPartition);
    Stream<FileGroupForRewrite> groupStream = toGroupStream(ctx, fileGroupsByPartition);

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
  void rewriteFiles(RewriteExecutionContext ctx, FileGroupForRewrite fileGroupForRewrite,
                    ConcurrentLinkedQueue<String> completedRewrite,
                    ConcurrentHashMap<String, Pair<FileGroupInfo, FileGroupRewriteResult>> results) {

    String groupID = fileGroupForRewrite.groupID();
    int filesInGroup = fileGroupForRewrite.numFiles();

    String desc = jobDesc(fileGroupForRewrite, ctx);

    Set<DataFile> addedFiles =
        withJobGroupInfo(newJobGroupInfo("REWRITE-DATA-FILES", desc),
            () -> strategy.rewriteFiles(groupID, fileGroupForRewrite.files()));

    completedRewrite.offer(groupID);
    FileGroupRewriteResult fileGroupResult = new FileGroupRewriteResult(addedFiles.size(), filesInGroup);

    results.put(groupID, Pair.of(fileGroupForRewrite.info(), fileGroupResult));
  }

  private void commitOrClean(Set<String> completedGroupIDs) {
    try {
      commitFileGroups(completedGroupIDs);
    } catch (Exception e) {
      LOG.error("Cannot commit groups {}, attempting to clean up written files", completedGroupIDs, e);
      Tasks.foreach(completedGroupIDs)
          .suppressFailureWhenFinished()
          .run(this::abortFileGroup);
      throw e;
    }
  }

  private Result doExecute(RewriteExecutionContext ctx, Stream<FileGroupForRewrite> groupStream) {

    ExecutorService rewriteService = Executors.newFixedThreadPool(maxConcurrentFileGroupRewrites,
        new ThreadFactoryBuilder().setNameFormat("Rewrite-Service-%d").build());

    ConcurrentLinkedQueue<String> completedRewrite = new ConcurrentLinkedQueue<>();
    ConcurrentHashMap<String, Pair<FileGroupInfo, FileGroupRewriteResult>> results = new ConcurrentHashMap<>();

    Tasks.Builder<FileGroupForRewrite> rewriteTaskBuilder = Tasks.foreach(groupStream.iterator())
        .executeWith(rewriteService)
        .stopOnFailure()
        .noRetry()
        .onFailure((fileGroupForRewrite, exception) -> {
          LOG.error("Failure during rewrite process for group {}", fileGroupForRewrite.info, exception);
        });

    try {
      rewriteTaskBuilder
          .run(fileGroupForRewrite -> rewriteFiles(ctx, fileGroupForRewrite, completedRewrite, results));
    } catch (Exception e) {
      // At least one rewrite group failed, clean up all completed rewrites
      LOG.error("Cannot complete rewrite, partial progress is not enabled and one of the file set groups failed to " +
          "be rewritten. Cleaning up {} groups which finished being written.", completedRewrite.size(), e);
      Tasks.foreach(completedRewrite)
          .suppressFailureWhenFinished()
          .run(this::abortFileGroup);
      throw e;
    }

    commitOrClean(ImmutableSet.copyOf(completedRewrite));

    return new Result(results.values().stream().collect(Collectors.toMap(Pair::first, Pair::second)));
  }

  private Result doExecuteWithPartialProgress(RewriteExecutionContext ctx, Stream<FileGroupForRewrite> groupStream) {

    ExecutorService rewriteService = Executors.newFixedThreadPool(maxConcurrentFileGroupRewrites,
        new ThreadFactoryBuilder().setNameFormat("Rewrite-Service-%d").build());

    ExecutorService committerService = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat("Committer-Service").build());

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
        .onFailure((fileGroupForRewrite, exception) -> {
          LOG.error("Failure during rewrite process for group {}", fileGroupForRewrite.info, exception);
          abortFileGroup(fileGroupForRewrite.groupID()); })
        .run(infoListPair -> rewriteFiles(ctx, infoListPair, completedRewriteIds, results));

    stillRewriting.set(false);
    committerService.shutdown();

    try {
      committerService.awaitTermination(10, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      throw new RuntimeException("Cannot complete commit for rewrite, commit service interrupted", e);
    }

    return new Result(results.values().stream().collect(Collectors.toMap(Pair::first, Pair::second)));
  }

  private Stream<FileGroupForRewrite> toGroupStream(RewriteExecutionContext ctx,
      Map<StructLike, List<List<FileScanTask>>> fileGroupsByPartition) {

    // Todo Add intelligence to the order in which we do rewrites instead of just using partition order
    return fileGroupsByPartition.entrySet().stream()
        .flatMap(
            e -> e.getValue().stream().map(tasks -> {
              int myJobIndex = ctx.currentGlobalIndex();
              int myPartIndex = ctx.currentPartitionIndex(e.getKey());
              String groupID = UUID.randomUUID().toString();
              return new FileGroupForRewrite(new FileGroupInfo(groupID, myJobIndex, myPartIndex, e.getKey()), tasks);
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

  private String jobDesc(FileGroupForRewrite group, RewriteExecutionContext ctx) {
    StructLike partition = group.partition();

    return String.format("Rewrite %s, FileGroup %d/%d : Partition %s:%d/%d : Rewriting %d files : Table %s",
        strategy.name(), group.globalIndex(), ctx.totalGroupCount(), partition, group.partitionIndex(),
        ctx.groupsInPartition(partition), group.numFiles(), table.name());
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

  static class FileGroupForRewrite {
    private final FileGroupInfo info;
    private final List<FileScanTask> files;
    private final int numFiles;

    FileGroupForRewrite(FileGroupInfo info, List<FileScanTask> files) {
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
