/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.actions;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.actions.compaction.DataCompactionStrategy;
import org.apache.iceberg.actions.compaction.SparkBinningCompactionStrategy;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.spark.actions.BaseSparkAction;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.util.StreamUtil.streamOf;

// TODO change most of this to interfaces for code reuse
public class CompactDataFilesAction extends BaseSparkAction<CompactDataFiles, CompactDataFiles.Result> implements CompactDataFiles {
  private static final Logger LOG = LoggerFactory.getLogger(CompactDataFilesAction.class);

  private final Table table;

  private DataCompactionStrategy compactionStrategy;
  private int parallelism = 10;
  private Expression filters = Expressions.alwaysTrue();

  public CompactDataFilesAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
    this.compactionStrategy = new SparkBinningCompactionStrategy(spark);
  }

  public CompactDataFilesAction parallelism(int par) {
    parallelism = par;
    return this;
  }

  public CompactDataFilesAction compactionStrategy(DataCompactionStrategy strategy) {
    compactionStrategy = strategy;
    return this;
  }

  // Todo Do we want to allow generic class loading here? I think yes this will probably be framework specific
  public CompactDataFilesAction compactionStrategy(String strategyName) {
    compactionStrategy = (DataCompactionStrategy) DynConstructors.builder().impl(strategyName).build().newInstance();
    return this;
  }

  @Override
  public CompactDataFiles filter(Expression expression) {
    this.filters = Expressions.and(this.filters, expression);
    return this;
  }

  @Override
  public CompactDataFiles.Result execute() {
    CloseableIterable<FileScanTask> files = table.newScan()
        .filter(Expressions.and(filters, compactionStrategy.preFilter()))
        .ignoreResiduals()
        .planFiles();

    Map<StructLike, List<FileScanTask>> tasksByPartition =
        streamOf(files).collect(Collectors.groupingBy(task -> task.file().partition()));

    Map<StructLike, List<List<FileScanTask>>> compactionJobs =
        tasksByPartition.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> {
          Iterator<FileScanTask> filtered = compactionStrategy.filesToCompact(e.getValue().iterator());
          Iterator<List<FileScanTask>> groupedTasks = compactionStrategy.groupsFilesIntoJobs(filtered);
          return ImmutableList.copyOf(groupedTasks);
        }));

    Map<StructLike, Integer> jobsPerPartition = compactionJobs.entrySet().stream().collect(Collectors.toMap(
        e -> e.getKey(),
        e -> e.getValue().size()
    ));

    Integer totalJobs = jobsPerPartition.values().stream().reduce(Integer::sum).orElse(0);
    Integer totalPartitions = jobsPerPartition.keySet().size();

    Map<StructLike, Integer> partitionIndex = new HashMap<>();
    AtomicInteger jobIndex = new AtomicInteger(1);

    // Todo Check if we need to randomize the order in which we do jobs, instead of being partition centric
    Stream<Pair<CompactionJobInfo, List<FileScanTask>>> jobStream = compactionJobs.entrySet().stream().flatMap(
        e -> e.getValue().stream().map(tasks -> {
          int myJobIndex = jobIndex.getAndIncrement();
          int myPartIndex = partitionIndex.merge(e.getKey(), 1, Integer::sum);
          return Pair.of(new CompactionJobInfo(myJobIndex, myPartIndex, e.getKey()), tasks);
        })
    );

    ExecutorService executorService = Executors.newFixedThreadPool(parallelism,
        new ThreadFactoryBuilder().setNameFormat("Compaction-Pool-%d").build());

    LOG.info("Beginning compaction with job parallelism of {} - Total Jobs {} - Total Partitions {}",
        parallelism, totalJobs, totalPartitions);

    // Concurrent Map here to collect results?
    ConcurrentHashMap<StructLike, Integer> completedJobs = new ConcurrentHashMap();

    try {
      Tasks.foreach(jobStream.iterator())
          .executeWith(executorService)
          .throwFailureWhenFinished()
          .noRetry() // Debatable
          .run(job -> {
            CompactionJobInfo info = job.first();
            final String jobDescription = String.format("Compaction Job (%d / %d) - Partition %s - " +
                            "Partition Job (%d / %d) - %d Files To Compact",
                    info.jobIndex(), totalJobs, info.partition(), info.partitionIndex(),
                    jobsPerPartition.get(info.partition()), job.second().size());

            LOG.info("Compaction Job ({} / {}) - Partition {} -  Partition Job ({} / {}) - {} Files To Compact",
                    info.jobIndex(), totalJobs, info.partition(), info.partitionIndex(),
                    jobsPerPartition.get(info.partition()), job.second().size());
            LOG.trace("Job {} will compact files:\n{}",
                job.second().stream().map(t -> t.file().path()).collect(Collectors.joining("\n")));

            compactionStrategy.rewriteFiles(table, job.second(), jobDescription);
          });
    } finally {
      executorService.shutdown();
    }

    return null;
  }

  @Override
  protected CompactDataFilesAction self() {
    return this;
  }

}
