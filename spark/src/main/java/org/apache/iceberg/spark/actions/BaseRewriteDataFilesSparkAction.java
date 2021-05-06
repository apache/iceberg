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

import java.util.HashMap;
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
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteStrategy;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
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

  private RewriteDataFiles.Strategy strategyType = Strategy.BINPACK;
  private Table table;
  private Expression filter = Expressions.alwaysTrue();

  /**
   * returns the Spark version specific strategy
   */
  protected abstract RewriteStrategy rewriteStrategy(Strategy type);

  protected BaseRewriteDataFilesSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
  }

  @Override
  public RewriteDataFiles strategy(Strategy type) {
    strategyType = type;
    return this;
  }

  @Override
  public RewriteDataFiles filter(Expression expression) {
    filter = Expressions.and(filter, expression);
    return this;
  }

  @Override
  public Result execute() {
    RewriteStrategy strategy =  rewriteStrategy(strategyType).options(this.options());

    CloseableIterable<FileScanTask> files = table.newScan()
        .filter(filter)
        .ignoreResiduals()
        .planFiles();

    Map<StructLike, List<FileScanTask>> filesByPartition =
        Streams.stream(files).collect(Collectors.groupingBy(task -> task.file().partition()));

    Map<StructLike, List<List<FileScanTask>>> fileGroupsByPartition =
        filesByPartition.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> {
          Iterable<FileScanTask> filtered = strategy.selectFilesToRewrite(e.getValue());
          Iterable<List<FileScanTask>> groupedTasks = strategy.planFileGroups(filtered);
          return ImmutableList.copyOf(groupedTasks);
        }));

    Map<StructLike, Integer> numGroupsPerPartition = fileGroupsByPartition.entrySet().stream().collect(Collectors.toMap(
        e -> e.getKey(),
        e -> e.getValue().size()
    ));

    Integer totalGroups = numGroupsPerPartition.values().stream().reduce(Integer::sum).orElse(0);
    Integer totalPartitions = numGroupsPerPartition.keySet().size();

    Map<StructLike, Integer> partitionIndex = new HashMap<>();
    AtomicInteger jobIndex = new AtomicInteger(1);

    // Todo Check if we need to randomize the order in which we do jobs, instead of being partition centric
    Stream<Pair<FileGroupInfo, List<FileScanTask>>> jobStream = fileGroupsByPartition.entrySet().stream().flatMap(
        e -> e.getValue().stream().map(tasks -> {
          int myJobIndex = jobIndex.getAndIncrement();
          int myPartIndex = partitionIndex.merge(e.getKey(), 1, Integer::sum);
          return Pair.of(new FileGroupInfo(UUID.randomUUID().toString(), myJobIndex, myPartIndex, e.getKey()), tasks);
        })
    );

    int maxConcurrentFileGroupActions = PropertyUtil.propertyAsInt(options(),
        RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_ACTIONS,
        RewriteDataFiles.MAX_CONCURRENT_FILE_GROUP_ACTIONS_DEFAULT);

    int maxCommits = PropertyUtil.propertyAsInt(options(),
        RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS,
        RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT);

    boolean partialProgressEnabled = PropertyUtil.propertyAsBoolean(options(),
        RewriteDataFiles.PARTIAL_PROGRESS_ENABLED,
        RewriteDataFiles.PARTIAL_PROGRESS_ENABLED_DEFAULT);

    int groupsPerCommit = partialProgressEnabled ? totalGroups / maxCommits : totalGroups;

    AtomicBoolean stillRewriting = new AtomicBoolean(true);
    ConcurrentLinkedQueue<String> completedRewrite = new ConcurrentLinkedQueue<>();
    ConcurrentHashMap<String, Pair<FileGroupInfo, FileGroupRewriteResult>> results = new ConcurrentHashMap<>();
    ConcurrentLinkedQueue<String> completedCommits = new ConcurrentLinkedQueue<>();

    ExecutorService rewriteService = Executors.newFixedThreadPool(maxConcurrentFileGroupActions,
        new ThreadFactoryBuilder().setNameFormat("Rewrite-Service-%d").build());

    ExecutorService commiterService = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat("Committer-Service").build());

    commiterService.execute(() -> {
      try {
        while (stillRewriting.get() || completedRewrite.size() > groupsPerCommit) {
          Thread.yield();
          if (completedRewrite.size() > groupsPerCommit) {
            // Gather a group of rewrites to do a commit
            Set<String> batch = Sets.newHashSetWithExpectedSize(groupsPerCommit);
            for (int i = 0; i < groupsPerCommit; i++) {
              batch.add(completedRewrite.poll());
            }
            // Running commit on the group we gathered
            try {
              commitFileGroups(batch);
            } catch (Exception e) {
              if (!partialProgressEnabled) {
                LOG.error("Failure during rewrite commit process, partial progress not enabled. Rethrowing", e);
                throw e;
              } else {
                LOG.error("Failure during rewrite commit process, partial progress enabled. Ignoring", e);
              }
            }
            completedCommits.addAll(batch);
          }
        }
        Set<String> finalBatch = Sets.newHashSetWithExpectedSize(groupsPerCommit);
        while (completedRewrite.size() != 0) {
          finalBatch.add(completedRewrite.poll());
        }
        if (finalBatch.size() != 0) {
          // Final Commit
          commitFileGroups(finalBatch);
          completedCommits.addAll(finalBatch);
        }
      } catch (Exception e) {
        if (!partialProgressEnabled) {
          LOG.error("Cannot commit rewrite and partial progress not enabled, removing all completed work", e);
          completedCommits.forEach(this::abortFileGroup);
        }
      }
    });

    // Start rewrite tasks
    Tasks.Builder<Pair<FileGroupInfo, List<FileScanTask>>> rewriteTaskBuilder = Tasks.foreach(jobStream.iterator())
        .suppressFailureWhenFinished()
        .executeWith(rewriteService)
        .noRetry()
        .onFailure((info, exception) -> {
          LOG.error("Failure during rewrite process for group {}", info.first(), exception);
          abortFileGroup(info.first().setId);
        });

    if (partialProgressEnabled) {
      rewriteTaskBuilder = rewriteTaskBuilder.suppressFailureWhenFinished();
    } else {
      rewriteTaskBuilder = rewriteTaskBuilder.stopOnFailure();
    }

    rewriteTaskBuilder
        .run(i -> {
          String setId = i.first().setId();
          Set<DataFile> addedFiles = strategy.rewriteFiles(setId, i.second());
          completedRewrite.offer(setId);
          results.put(setId, Pair.of(i.first(), new FileGroupRewriteResult(addedFiles.size(), i.second().size())));
        });
    stillRewriting.set(false);
    commiterService.shutdown();

    try {
      commiterService.awaitTermination(10, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    return new Result(results.values().stream().collect(Collectors.toMap(p -> p.first(), p -> p.second())));
  }

  protected Table table() {
    return table;
  }

  /**
   * Perform a commit operation on the table adding and removing files as required for this set of file groups,
   * on failure should clean up and rethrow exception
   * @param completedSetIds fileSets to commit
   */
  protected abstract void commitFileGroups(Set<String> completedSetIds);

  /**
   * Clean up a specified file set by removing any files created for that operation
   * @param setId fileSet to clean
   */
  protected abstract void abortFileGroup(String setId);

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

    private final String setId;
    private final int globalIndex;
    private final int partitionIndex;
    private final StructLike partition;

    FileGroupInfo(String setId, int globalIndex, int partitionIndex, StructLike partition) {
      this.setId = setId;
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
          "setId=" + setId +
          ", globalIndex=" + globalIndex +
          ", partitionIndex=" + partitionIndex +
          ", partition=" + partition +
          '}';
    }

    public String setId() {
      return setId;
    }
  }

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
}
