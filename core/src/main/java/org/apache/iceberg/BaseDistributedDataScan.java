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
package org.apache.iceberg;

import static org.apache.iceberg.PlanningMode.AUTO;
import static org.apache.iceberg.TableProperties.DATA_PLANNING_MODE;
import static org.apache.iceberg.TableProperties.DELETE_PLANNING_MODE;
import static org.apache.iceberg.TableProperties.PLANNING_MODE_DEFAULT;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.ScanMetricsUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ParallelIterable;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract class for batch data scans that can utilize cluster resources for planning.
 *
 * <p>This class provides common logic to create data scans that are capable of reading and
 * filtering manifests remotely when the metadata size exceeds the threshold for local processing.
 * Also, it takes care of planning tasks locally if remote planning is not considered beneficial.
 *
 * <p>Note that this class is evolving and is subject to change even in minor releases.
 */
abstract class BaseDistributedDataScan
    extends DataScan<BatchScan, ScanTask, ScanTaskGroup<ScanTask>> implements BatchScan {

  private static final Logger LOG = LoggerFactory.getLogger(BaseDistributedDataScan.class);
  private static final long LOCAL_PLANNING_MAX_SLOT_SIZE = 128L * 1024 * 1024; // 128 MB
  private static final int MONITOR_POOL_SIZE = 2;

  private final int localParallelism;
  private final long localPlanningSizeThreshold;

  protected BaseDistributedDataScan(Table table, Schema schema, TableScanContext context) {
    super(table, schema, context);
    this.localParallelism = PLAN_SCANS_WITH_WORKER_POOL ? ThreadPools.WORKER_THREAD_POOL_SIZE : 1;
    this.localPlanningSizeThreshold = localParallelism * LOCAL_PLANNING_MAX_SLOT_SIZE;
  }

  /**
   * Returns the cluster parallelism.
   *
   * <p>This value indicates the maximum number of manifests that can be processed concurrently by
   * the cluster. Implementations should take into account both the currently available processing
   * slots and potential dynamic allocation, if applicable.
   *
   * <p>The remote parallelism is compared against the size of the thread pool available locally to
   * determine the feasibility of remote planning. This value is ignored if the planning mode is set
   * explicitly as local or distributed.
   */
  protected abstract int remoteParallelism();

  /** Returns which planning mode to use for data. */
  protected PlanningMode dataPlanningMode() {
    Map<String, String> properties = table().properties();
    String modeName = properties.getOrDefault(DATA_PLANNING_MODE, PLANNING_MODE_DEFAULT);
    return PlanningMode.fromName(modeName);
  }

  /**
   * Controls whether defensive copies are created for remotely planned data files.
   *
   * <p>By default, this class creates defensive copies for each data file that is planned remotely,
   * assuming the provided iterable can be lazy and may reuse objects. If unnecessary and data file
   * objects can be safely added into a collection, implementations can override this behavior.
   */
  protected boolean shouldCopyRemotelyPlannedDataFiles() {
    return true;
  }

  /**
   * Plans data remotely.
   *
   * <p>Implementations are encouraged to return groups of matching data files, enabling this class
   * to process multiple groups concurrently to speed up the remaining work. This is particularly
   * useful when dealing with equality deletes, as delete index lookups with such delete files
   * require comparing bounds and typically benefit from parallelization.
   *
   * <p>If the result iterable reuses objects, {@link #shouldCopyRemotelyPlannedDataFiles()} must
   * return true.
   *
   * <p>The input data manifests have been already filtered to include only potential matches based
   * on the scan filter. Implementations are expected to further filter these manifests and only
   * return files that may hold data matching the scan filter.
   *
   * @param dataManifests data manifests that may contain files matching the scan filter
   * @param withColumnStats a flag whether to load column stats
   * @return groups of data files planned remotely
   */
  protected abstract Iterable<CloseableIterable<DataFile>> planDataRemotely(
      List<ManifestFile> dataManifests, boolean withColumnStats);

  /** Returns which planning mode to use for deletes. */
  protected PlanningMode deletePlanningMode() {
    Map<String, String> properties = table().properties();
    String modeName = properties.getOrDefault(DELETE_PLANNING_MODE, PLANNING_MODE_DEFAULT);
    return PlanningMode.fromName(modeName);
  }

  /**
   * Plans deletes remotely.
   *
   * <p>The input delete manifests have been already filtered to include only potential matches
   * based on the scan filter. Implementations are expected to further filter these manifests and
   * return files that may hold deletes matching the scan filter.
   *
   * @param deleteManifests delete manifests that may contain files matching the scan filter
   * @return a delete file index planned remotely
   */
  protected abstract DeleteFileIndex planDeletesRemotely(List<ManifestFile> deleteManifests);

  @Override
  protected CloseableIterable<ScanTask> doPlanFiles() {
    Snapshot snapshot = snapshot();

    List<ManifestFile> deleteManifests = findMatchingDeleteManifests(snapshot);
    boolean mayHaveEqualityDeletes = !deleteManifests.isEmpty() && mayHaveEqualityDeletes(snapshot);
    boolean planDeletesLocally = shouldPlanDeletesLocally(deleteManifests, mayHaveEqualityDeletes);

    List<ManifestFile> dataManifests = findMatchingDataManifests(snapshot);
    boolean loadColumnStats = mayHaveEqualityDeletes || shouldReturnColumnStats();
    boolean planDataLocally = shouldPlanDataLocally(dataManifests, loadColumnStats);
    boolean copyDataFiles = shouldCopyDataFiles(planDataLocally, loadColumnStats);

    if (planDataLocally && planDeletesLocally) {
      return planFileTasksLocally(dataManifests, deleteManifests);
    }

    ExecutorService monitorPool = newMonitorPool();

    CompletableFuture<DeleteFileIndex> deletesFuture =
        newDeletesFuture(deleteManifests, planDeletesLocally, monitorPool);

    CompletableFuture<Iterable<CloseableIterable<DataFile>>> dataFuture =
        newDataFuture(dataManifests, planDataLocally, loadColumnStats, monitorPool);

    try {
      Iterable<CloseableIterable<ScanTask>> fileTasks =
          toFileTasks(dataFuture, deletesFuture, copyDataFiles);

      if (shouldPlanWithExecutor() && (planDataLocally || mayHaveEqualityDeletes)) {
        return new ParallelIterable<>(fileTasks, planExecutor());
      } else {
        return CloseableIterable.concat(fileTasks);
      }

    } catch (CompletionException e) {
      deletesFuture.cancel(true /* may interrupt */);
      dataFuture.cancel(true /* may interrupt */);
      throw new RuntimeException("Failed to plan files", e);

    } finally {
      monitorPool.shutdown();
    }
  }

  @Override
  public CloseableIterable<ScanTaskGroup<ScanTask>> planTasks() {
    return TableScanUtil.planTaskGroups(
        planFiles(), targetSplitSize(), splitLookback(), splitOpenFileCost());
  }

  private List<ManifestFile> findMatchingDataManifests(Snapshot snapshot) {
    List<ManifestFile> dataManifests = snapshot.dataManifests(io());
    scanMetrics().totalDataManifests().increment(dataManifests.size());

    List<ManifestFile> matchingDataManifests = filterManifests(dataManifests);
    int skippedDataManifestsCount = dataManifests.size() - matchingDataManifests.size();
    scanMetrics().skippedDataManifests().increment(skippedDataManifestsCount);

    return matchingDataManifests;
  }

  private List<ManifestFile> findMatchingDeleteManifests(Snapshot snapshot) {
    List<ManifestFile> deleteManifests = snapshot.deleteManifests(io());
    scanMetrics().totalDeleteManifests().increment(deleteManifests.size());

    List<ManifestFile> matchingDeleteManifests = filterManifests(deleteManifests);
    int skippedDeleteManifestsCount = deleteManifests.size() - matchingDeleteManifests.size();
    scanMetrics().skippedDeleteManifests().increment(skippedDeleteManifestsCount);

    return matchingDeleteManifests;
  }

  private List<ManifestFile> filterManifests(List<ManifestFile> manifests) {
    Map<Integer, ManifestEvaluator> evalCache = specCache(this::newManifestEvaluator);

    return manifests.stream()
        .filter(manifest -> manifest.hasAddedFiles() || manifest.hasExistingFiles())
        .filter(manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest))
        .collect(Collectors.toList());
  }

  private boolean shouldPlanDeletesLocally(
      List<ManifestFile> deleteManifests, boolean mayHaveEqualityDeletes) {
    PlanningMode mode = deletePlanningMode();
    return (mode == AUTO && mayHaveEqualityDeletes) || shouldPlanLocally(mode, deleteManifests);
  }

  private boolean shouldPlanDataLocally(List<ManifestFile> dataManifests, boolean loadColumnStats) {
    PlanningMode mode = dataPlanningMode();
    return (mode == AUTO && loadColumnStats) || shouldPlanLocally(mode, dataManifests);
  }

  private boolean shouldPlanLocally(PlanningMode mode, List<ManifestFile> manifests) {
    if (context().planWithCustomizedExecutor()) {
      return true;
    }

    switch (mode) {
      case LOCAL:
        return true;

      case DISTRIBUTED:
        return manifests.isEmpty();

      case AUTO:
        return remoteParallelism() <= localParallelism
            || manifests.size() <= 2 * localParallelism
            || totalSize(manifests) <= localPlanningSizeThreshold;

      default:
        throw new IllegalArgumentException("Unknown planning mode: " + mode);
    }
  }

  private long totalSize(List<ManifestFile> manifests) {
    return manifests.stream().mapToLong(ManifestFile::length).sum();
  }

  private boolean shouldCopyDataFiles(boolean planDataLocally, boolean loadColumnStats) {
    return planDataLocally
        || shouldCopyRemotelyPlannedDataFiles()
        || (loadColumnStats && !shouldReturnColumnStats());
  }

  @SuppressWarnings("unchecked")
  private CloseableIterable<ScanTask> planFileTasksLocally(
      List<ManifestFile> dataManifests, List<ManifestFile> deleteManifests) {
    LOG.info("Planning file tasks locally for table {}", table().name());
    ManifestGroup manifestGroup = newManifestGroup(dataManifests, deleteManifests);
    CloseableIterable<? extends ScanTask> fileTasks = manifestGroup.planFiles();
    return (CloseableIterable<ScanTask>) fileTasks;
  }

  private CompletableFuture<DeleteFileIndex> newDeletesFuture(
      List<ManifestFile> deleteManifests, boolean planLocally, ExecutorService monitorPool) {

    return CompletableFuture.supplyAsync(
        () -> {
          if (planLocally) {
            LOG.info("Planning deletes locally for table {}", table().name());
            return planDeletesLocally(deleteManifests);
          } else {
            LOG.info("Planning deletes remotely for table {}", table().name());
            return planDeletesRemotely(deleteManifests);
          }
        },
        monitorPool);
  }

  private DeleteFileIndex planDeletesLocally(List<ManifestFile> deleteManifests) {
    DeleteFileIndex.Builder builder = DeleteFileIndex.builderFor(io(), deleteManifests);

    if (shouldPlanWithExecutor() && deleteManifests.size() > 1) {
      builder.planWith(planExecutor());
    }

    return builder
        .specsById(table().specs())
        .filterData(filter())
        .caseSensitive(isCaseSensitive())
        .scanMetrics(scanMetrics())
        .build();
  }

  private CompletableFuture<Iterable<CloseableIterable<DataFile>>> newDataFuture(
      List<ManifestFile> dataManifests,
      boolean planLocally,
      boolean withColumnStats,
      ExecutorService monitorPool) {

    return CompletableFuture.supplyAsync(
        () -> {
          if (planLocally) {
            LOG.info("Planning data locally for table {}", table().name());
            ManifestGroup manifestGroup = newManifestGroup(dataManifests, withColumnStats);
            return manifestGroup.fileGroups();
          } else {
            LOG.info("Planning data remotely for table {}", table().name());
            return planDataRemotely(dataManifests, withColumnStats);
          }
        },
        monitorPool);
  }

  private Iterable<CloseableIterable<ScanTask>> toFileTasks(
      CompletableFuture<Iterable<CloseableIterable<DataFile>>> dataFuture,
      CompletableFuture<DeleteFileIndex> deletesFuture,
      boolean copyDataFiles) {

    String schemaString = SchemaParser.toJson(tableSchema());
    Map<Integer, String> specStringCache = specCache(PartitionSpecParser::toJson);
    Map<Integer, ResidualEvaluator> residualCache = specCache(this::newResidualEvaluator);

    Iterable<CloseableIterable<DataFile>> dataFileGroups = dataFuture.join();

    return Iterables.transform(
        dataFileGroups,
        dataFiles ->
            toFileTasks(
                dataFiles,
                deletesFuture,
                copyDataFiles,
                schemaString,
                specStringCache,
                residualCache));
  }

  private CloseableIterable<ScanTask> toFileTasks(
      CloseableIterable<DataFile> dataFiles,
      CompletableFuture<DeleteFileIndex> deletesFuture,
      boolean copyDataFiles,
      String schemaString,
      Map<Integer, String> specStringCache,
      Map<Integer, ResidualEvaluator> residualCache) {

    return CloseableIterable.transform(
        dataFiles,
        dataFile -> {
          DeleteFile[] deleteFiles = deletesFuture.join().forDataFile(dataFile);

          String specString = specStringCache.get(dataFile.specId());
          ResidualEvaluator residuals = residualCache.get(dataFile.specId());

          ScanMetricsUtil.fileTask(scanMetrics(), dataFile, deleteFiles);

          return new BaseFileScanTask(
              copyDataFiles ? dataFile.copy(shouldReturnColumnStats()) : dataFile,
              deleteFiles,
              schemaString,
              specString,
              residuals);
        });
  }

  private ManifestEvaluator newManifestEvaluator(PartitionSpec spec) {
    Expression projection = Projections.inclusive(spec, isCaseSensitive()).project(filter());
    return ManifestEvaluator.forPartitionFilter(projection, spec, isCaseSensitive());
  }

  private ResidualEvaluator newResidualEvaluator(PartitionSpec spec) {
    return ResidualEvaluator.of(spec, residualFilter(), isCaseSensitive());
  }

  private <R> Map<Integer, R> specCache(Function<PartitionSpec, R> load) {
    Map<Integer, R> cache = Maps.newHashMap();
    table().specs().forEach((specId, spec) -> cache.put(specId, load.apply(spec)));
    return cache;
  }

  private boolean mayHaveEqualityDeletes(Snapshot snapshot) {
    String count = snapshot.summary().get(SnapshotSummary.TOTAL_EQ_DELETES_PROP);
    return count == null || !count.equals("0");
  }

  // a monitor pool that enables planing data and deletes concurrently if remote planning is used
  private ExecutorService newMonitorPool() {
    return ThreadPools.newWorkerPool("iceberg-planning-monitor-service", MONITOR_POOL_SIZE);
  }
}
