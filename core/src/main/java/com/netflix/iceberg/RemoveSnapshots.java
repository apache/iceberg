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

package com.netflix.iceberg;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.iceberg.exceptions.CommitFailedException;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.util.Tasks;
import com.netflix.iceberg.util.ThreadPools;
import io.netty.util.internal.ConcurrentSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static com.netflix.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static com.netflix.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static com.netflix.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static com.netflix.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static com.netflix.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static com.netflix.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static com.netflix.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static com.netflix.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

class RemoveSnapshots implements ExpireSnapshots {
  private static final Logger LOG = LoggerFactory.getLogger(RemoveSnapshots.class);

  private final Consumer<String> defaultDelete = new Consumer<String>() {
    @Override
    public void accept(String file) {
      ops.io().deleteFile(file);
    }
  };

  private final TableOperations ops;
  private final Set<Long> idsToRemove = Sets.newHashSet();
  private TableMetadata base;
  private Long expireOlderThan = null;
  private Consumer<String> deleteFunc = defaultDelete;

  RemoveSnapshots(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current();
  }

  @Override
  public ExpireSnapshots expireSnapshotId(long snapshotId) {
    LOG.info("Expiring snapshot with id: {}", snapshotId);
    idsToRemove.add(snapshotId);
    return this;
  }

  @Override
  public ExpireSnapshots expireOlderThan(long timestampMillis) {
    LOG.info("Expiring snapshots older than: {} ({})", new Date(timestampMillis), timestampMillis);
    this.expireOlderThan = timestampMillis;
    return this;
  }

  @Override
  public ExpireSnapshots deleteWith(Consumer<String> deleteFunc) {
    this.deleteFunc = deleteFunc;
    return this;
  }

  @Override
  public List<Snapshot> apply() {
    TableMetadata updated = internalApply();
    List<Snapshot> removed = Lists.newArrayList(base.snapshots());
    removed.removeAll(updated.snapshots());

    return removed;
  }

  private TableMetadata internalApply() {
    this.base = ops.refresh();

    return base.removeSnapshotsIf(snapshot -> (
        idsToRemove.contains(snapshot.snapshotId()) ||
        (expireOlderThan != null && snapshot.timestampMillis() < expireOlderThan)
    ));
  }

  @Override
  public void commit() {
    Tasks.foreach(ops)
        .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */ )
        .onlyRetryOn(CommitFailedException.class)
        .run(item -> {
          TableMetadata updated = internalApply();
          // only commit the updated metadata if at least one snapshot was removed
          if (updated.snapshots().size() != base.snapshots().size()) {
            ops.commit(base, updated);
          }
        });

    LOG.info("Committed snapshot changes; cleaning up expired manifests and data files.");

    // clean up the expired snapshots:
    // 1. Get a list of the snapshots that were removed
    // 2. Delete any data files that were deleted by those snapshots and are not in the table
    // 3. Delete any manifests that are no longer used by current snapshots
    // 4. Delete the manifest lists

    // Reads and deletes are done using Tasks.foreach(...).suppressFailureWhenFinished to complete
    // as much of the delete work as possible and avoid orphaned data or manifest files.

    TableMetadata current = ops.refresh();
    Set<Long> currentIds = Sets.newHashSet();
    Set<ManifestFile> currentManifests = Sets.newHashSet();
    for (Snapshot snapshot : current.snapshots()) {
      currentIds.add(snapshot.snapshotId());
      currentManifests.addAll(snapshot.manifests());
    }

    Set<String> manifestListsToDelete = Sets.newHashSet();
    Set<ManifestFile> allManifests = Sets.newHashSet(currentManifests);
    Set<String> manifestsToDelete = Sets.newHashSet();
    for (Snapshot snapshot : base.snapshots()) {
      long snapshotId = snapshot.snapshotId();
      if (!currentIds.contains(snapshotId)) {
        // the snapshot was expired
        LOG.info("Expired snapshot: {}", snapshot);
        // find any manifests that are no longer needed
        for (ManifestFile manifest : snapshot.manifests()) {
          if (!currentManifests.contains(manifest)) {
            manifestsToDelete.add(manifest.path());
            allManifests.add(manifest);
          }
        }
        // add the manifest list to the delete set, if present
        if (snapshot.manifestListLocation() != null) {
          manifestListsToDelete.add(snapshot.manifestListLocation());
        }
      }
    }

    Set<String> filesToDelete = new ConcurrentSet<>();
    Tasks.foreach(allManifests)
        .noRetry().suppressFailureWhenFinished()
        .executeWith(ThreadPools.getWorkerPool())
        .onFailure((item, exc) ->
            LOG.warn("Failed to get deleted files: this may cause orphaned data files", exc)
        ).run(manifest -> {
          if (manifest.deletedFilesCount() != null && manifest.deletedFilesCount() == 0) {
            return;
          }

          // the manifest has deletes, scan it to find files to delete
          try (ManifestReader reader = ManifestReader.read(ops.io().newInputFile(manifest.path()))) {
            for (ManifestEntry entry : reader.entries()) {
              // if the snapshot ID of the DELETE entry is no longer valid, the data can be deleted
              if (entry.status() == ManifestEntry.Status.DELETED &&
                  !currentIds.contains(entry.snapshotId())) {
                // use toString to ensure the path will not change (Utf8 is reused)
                filesToDelete.add(entry.file().path().toString());
              }
            }
          } catch (IOException e) {
            throw new RuntimeIOException(e, "Failed to read manifest file: " + manifest.path());
          }
    });

    LOG.warn("Manifests to delete: {}", Joiner.on(", ").join(manifestsToDelete));

    Tasks.foreach(filesToDelete)
        .noRetry().suppressFailureWhenFinished()
        .onFailure((file, exc) -> LOG.warn("Delete failed for data file: " + file, exc))
        .run(file -> deleteFunc.accept(file));

    Tasks.foreach(manifestsToDelete)
        .noRetry().suppressFailureWhenFinished()
        .onFailure((manifest, exc) -> LOG.warn("Delete failed for manifest: " + manifest, exc))
        .run(deleteFunc::accept);

    Tasks.foreach(manifestListsToDelete)
        .noRetry().suppressFailureWhenFinished()
        .onFailure((list, exc) -> LOG.warn("Delete failed for manifest list: " + list, exc))
        .run(deleteFunc::accept);
  }
}
