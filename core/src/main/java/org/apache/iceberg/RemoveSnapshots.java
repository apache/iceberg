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

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.TableProperties.GC_ENABLED_DEFAULT;
import static org.apache.iceberg.TableProperties.MAX_REF_AGE_MS;
import static org.apache.iceberg.TableProperties.MAX_REF_AGE_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.MAX_SNAPSHOT_AGE_MS;
import static org.apache.iceberg.TableProperties.MAX_SNAPSHOT_AGE_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.MIN_SNAPSHOTS_TO_KEEP;
import static org.apache.iceberg.TableProperties.MIN_SNAPSHOTS_TO_KEEP_DEFAULT;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("UnnecessaryAnonymousClass")
class RemoveSnapshots implements ExpireSnapshots {
  private static final Logger LOG = LoggerFactory.getLogger(RemoveSnapshots.class);

  // Creates an executor service that runs each task in the thread that invokes execute/submit.
  private static final ExecutorService DEFAULT_DELETE_EXECUTOR_SERVICE =
      MoreExecutors.newDirectExecutorService();

  private final Consumer<String> defaultDelete =
      new Consumer<String>() {
        @Override
        public void accept(String file) {
          ops.io().deleteFile(file);
        }
      };

  private final TableOperations ops;
  private final Set<Long> idsToRemove = Sets.newHashSet();
  private final long now;
  private final long defaultMaxRefAgeMs;
  private boolean cleanExpiredFiles = true;
  private TableMetadata base;
  private long defaultExpireOlderThan;
  private int defaultMinNumSnapshots;
  private Consumer<String> deleteFunc = defaultDelete;
  private ExecutorService deleteExecutorService = DEFAULT_DELETE_EXECUTOR_SERVICE;
  private ExecutorService planExecutorService = ThreadPools.getWorkerPool();
  private Boolean incrementalCleanup;

  private Set<Snapshot> expiredSnapshots;

  RemoveSnapshots(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current();
    ValidationException.check(
        PropertyUtil.propertyAsBoolean(base.properties(), GC_ENABLED, GC_ENABLED_DEFAULT),
        "Cannot expire snapshots: GC is disabled (deleting files may corrupt other tables)");

    long defaultMaxSnapshotAgeMs =
        PropertyUtil.propertyAsLong(
            base.properties(), MAX_SNAPSHOT_AGE_MS, MAX_SNAPSHOT_AGE_MS_DEFAULT);

    this.now = System.currentTimeMillis();
    this.defaultExpireOlderThan = now - defaultMaxSnapshotAgeMs;
    this.defaultMinNumSnapshots =
        PropertyUtil.propertyAsInt(
            base.properties(), MIN_SNAPSHOTS_TO_KEEP, MIN_SNAPSHOTS_TO_KEEP_DEFAULT);

    this.defaultMaxRefAgeMs =
        PropertyUtil.propertyAsLong(base.properties(), MAX_REF_AGE_MS, MAX_REF_AGE_MS_DEFAULT);

    this.expiredSnapshots = Sets.newHashSet();
  }

  @Override
  public ExpireSnapshots cleanExpiredFiles(boolean clean) {
    this.cleanExpiredFiles = clean;
    return this;
  }

  @Override
  public ExpireSnapshots expireSnapshotId(long expireSnapshotId) {
    LOG.info("Expiring snapshot with id: {}", expireSnapshotId);
    idsToRemove.add(expireSnapshotId);
    return this;
  }

  @Override
  public ExpireSnapshots expireOlderThan(long timestampMillis) {
    LOG.info(
        "Expiring snapshots older than: {} ({})",
        DateTimeUtil.formatTimestampMillis(timestampMillis),
        timestampMillis);
    this.defaultExpireOlderThan = timestampMillis;
    return this;
  }

  @Override
  public ExpireSnapshots retainLast(int numSnapshots) {
    Preconditions.checkArgument(
        1 <= numSnapshots,
        "Number of snapshots to retain must be at least 1, cannot be: %s",
        numSnapshots);
    this.defaultMinNumSnapshots = numSnapshots;
    return this;
  }

  @Override
  public ExpireSnapshots deleteWith(Consumer<String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
    return this;
  }

  @Override
  public ExpireSnapshots executeDeleteWith(ExecutorService executorService) {
    this.deleteExecutorService = executorService;
    return this;
  }

  @Override
  public ExpireSnapshots planWith(ExecutorService executorService) {
    this.planExecutorService = executorService;
    return this;
  }

  @Override
  public List<Snapshot> apply() {
    internalApply();
    return Lists.newArrayList(expiredSnapshots);
  }

  private TableMetadata internalApply() {
    this.base = ops.refresh();
    if (base.snapshots().isEmpty()) {
      return base;
    }

    Set<Long> idsToRetain = Sets.newHashSet();
    // Identify refs that should be removed
    Map<String, SnapshotRef> retainedRefs = computeRetainedRefs(base.refs());
    Map<Long, List<String>> retainedIdToRefs = Maps.newHashMap();
    for (Map.Entry<String, SnapshotRef> retainedRefEntry : retainedRefs.entrySet()) {
      long snapshotId = retainedRefEntry.getValue().snapshotId();
      retainedIdToRefs.putIfAbsent(snapshotId, Lists.newArrayList());
      retainedIdToRefs.get(snapshotId).add(retainedRefEntry.getKey());
      idsToRetain.add(snapshotId);
    }

    for (long idToRemove : idsToRemove) {
      List<String> refsForId = retainedIdToRefs.get(idToRemove);
      Preconditions.checkArgument(
          refsForId == null,
          "Cannot expire %s. Still referenced by refs: %s",
          idToRemove,
          refsForId);
    }

    idsToRetain.addAll(computeAllBranchSnapshotsToRetain(retainedRefs.values()));
    idsToRetain.addAll(unreferencedSnapshotsToRetain(retainedRefs.values()));

    TableMetadata.Builder updatedMetaBuilder = TableMetadata.buildFrom(base);

    base.refs().keySet().stream()
        .filter(ref -> !retainedRefs.containsKey(ref))
        .forEach(updatedMetaBuilder::removeRef);

    base.snapshots().stream()
        .map(Snapshot::snapshotId)
        .filter(snapshot -> !idsToRetain.contains(snapshot))
        .forEach(idsToRemove::add);
    updatedMetaBuilder.removeSnapshots(idsToRemove);

    TableMetadata updated = updatedMetaBuilder.build();
    expiredSnapshots.addAll(base.snapshots());
    updated.snapshots().forEach(expiredSnapshots::remove);
    return updated;
  }

  private Map<String, SnapshotRef> computeRetainedRefs(Map<String, SnapshotRef> refs) {
    Map<String, SnapshotRef> retainedRefs = Maps.newHashMap();
    for (Map.Entry<String, SnapshotRef> refEntry : refs.entrySet()) {
      String name = refEntry.getKey();
      SnapshotRef ref = refEntry.getValue();
      if (name.equals(SnapshotRef.MAIN_BRANCH)) {
        retainedRefs.put(name, ref);
        continue;
      }

      Snapshot snapshot = base.snapshot(ref.snapshotId());
      long maxRefAgeMs = ref.maxRefAgeMs() != null ? ref.maxRefAgeMs() : defaultMaxRefAgeMs;
      if (snapshot != null) {
        long refAgeMs = now - snapshot.timestampMillis();
        if (refAgeMs <= maxRefAgeMs) {
          retainedRefs.put(name, ref);
        }
      } else {
        LOG.warn("Removing invalid ref {}: snapshot {} does not exist", name, ref.snapshotId());
      }
    }

    return retainedRefs;
  }

  private Set<Long> computeAllBranchSnapshotsToRetain(Collection<SnapshotRef> refs) {
    Set<Long> branchSnapshotsToRetain = Sets.newHashSet();
    for (SnapshotRef ref : refs) {
      if (ref.isBranch()) {
        long expireSnapshotsOlderThan =
            ref.maxSnapshotAgeMs() != null ? now - ref.maxSnapshotAgeMs() : defaultExpireOlderThan;
        int minSnapshotsToKeep =
            ref.minSnapshotsToKeep() != null ? ref.minSnapshotsToKeep() : defaultMinNumSnapshots;
        branchSnapshotsToRetain.addAll(
            computeBranchSnapshotsToRetain(
                ref.snapshotId(), expireSnapshotsOlderThan, minSnapshotsToKeep));
      }
    }

    return branchSnapshotsToRetain;
  }

  private Set<Long> computeBranchSnapshotsToRetain(
      long snapshot, long expireSnapshotsOlderThan, int minSnapshotsToKeep) {
    Set<Long> idsToRetain = Sets.newHashSet();
    for (Snapshot ancestor : SnapshotUtil.ancestorsOf(snapshot, base::snapshot)) {
      if (idsToRetain.size() < minSnapshotsToKeep
          || ancestor.timestampMillis() >= expireSnapshotsOlderThan) {
        idsToRetain.add(ancestor.snapshotId());
      } else {
        return idsToRetain;
      }
    }

    return idsToRetain;
  }

  private Set<Long> unreferencedSnapshotsToRetain(Collection<SnapshotRef> refs) {
    Set<Long> referencedSnapshots = Sets.newHashSet();
    for (SnapshotRef ref : refs) {
      if (ref.isBranch()) {
        for (Snapshot snapshot : SnapshotUtil.ancestorsOf(ref.snapshotId(), base::snapshot)) {
          referencedSnapshots.add(snapshot.snapshotId());
        }
      } else {
        referencedSnapshots.add(ref.snapshotId());
      }
    }

    Set<Long> snapshotsToRetain = Sets.newHashSet();
    for (Snapshot snapshot : base.snapshots()) {
      if (!referencedSnapshots.contains(snapshot.snapshotId())
          && // unreferenced
          snapshot.timestampMillis() >= defaultExpireOlderThan) { // not old enough to expire
        snapshotsToRetain.add(snapshot.snapshotId());
      }
    }

    return snapshotsToRetain;
  }

  @Override
  public void commit() {
    Tasks.foreach(ops)
        .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */)
        .onlyRetryOn(CommitFailedException.class)
        .run(
            item -> {
              TableMetadata updated = internalApply();
              ops.commit(base, updated);
            });
    LOG.info("Committed snapshot changes");

    if (cleanExpiredFiles) {
      cleanExpiredSnapshots();
    }
  }

  @Override
  public long expiredSnapshotsCount() {
    return expiredSnapshots.size();
  }

  ExpireSnapshots withIncrementalCleanup(boolean useIncrementalCleanup) {
    this.incrementalCleanup = useIncrementalCleanup;
    return this;
  }

  private void cleanExpiredSnapshots() {
    TableMetadata current = ops.refresh();

    if (incrementalCleanup == null) {
      incrementalCleanup = current.refs().size() == 1;
    }

    LOG.info(
        "Cleaning up expired files (local, {})", incrementalCleanup ? "incremental" : "reachable");

    FileCleanupStrategy cleanupStrategy =
        incrementalCleanup
            ? new IncrementalFileCleanup(
                ops.io(), deleteExecutorService, planExecutorService, deleteFunc)
            : new ReachableFileCleanup(
                ops.io(), deleteExecutorService, planExecutorService, deleteFunc);

    cleanupStrategy.cleanFiles(base, current);
  }
}
