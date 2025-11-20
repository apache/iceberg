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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * API for removing old {@link Snapshot snapshots} from a table.
 *
 * <p>This API accumulates snapshot deletions and commits the new list to the table. This API does
 * not allow deleting the current snapshot.
 *
 * <p>When committing, these changes will be applied to the latest table metadata. Commit conflicts
 * will be resolved by applying the changes to the new latest metadata and reattempting the commit.
 *
 * <p>Manifest files that are no longer used by valid snapshots will be deleted. Data files that
 * were deleted by snapshots that are expired will be deleted. {@link #deleteWith(Consumer)} can be
 * used to pass an alternative deletion method.
 *
 * <p>{@link #apply()} returns a list of the snapshots that will be removed.
 */
public interface ExpireSnapshots extends PendingUpdate<List<Snapshot>> {
  /** An enum representing possible clean up levels used in snapshot expiration. */
  enum CleanupLevel {
    /** Skip all file cleanup, only remove snapshot metadata. */
    NONE(0),
    /** Clean up only metadata files (manifests, manifest lists, statistics), retain data files. */
    METADATA_ONLY(1),
    /** Clean up both metadata and data files (default). */
    ALL(2);

    CleanupLevel(int id) {
      this.id = id;
    }

    private final int id;

    public int id() {
      return id;
    }

    public boolean skipFiles() {
      return this.id == 0;
    }

    public boolean cleanContentFiles() {
      return this.id == 2;
    }
  }

  /**
   * Expires a specific {@link Snapshot} identified by id.
   *
   * @param snapshotId long id of the snapshot to expire
   * @return this for method chaining
   */
  ExpireSnapshots expireSnapshotId(long snapshotId);

  /**
   * Expires all snapshots older than the given timestamp.
   *
   * @param timestampMillis a long timestamp, as returned by {@link System#currentTimeMillis()}
   * @return this for method chaining
   */
  ExpireSnapshots expireOlderThan(long timestampMillis);

  /**
   * Retains the most recent ancestors of the current snapshot.
   *
   * <p>If a snapshot would be expired because it is older than the expiration timestamp, but is one
   * of the {@code numSnapshots} most recent ancestors of the current state, it will be retained.
   * This will not cause snapshots explicitly identified by id from expiring.
   *
   * <p>This may keep more than {@code numSnapshots} ancestors if snapshots are added concurrently.
   * This may keep less than {@code numSnapshots} ancestors if the current table state does not have
   * that many.
   *
   * @param numSnapshots the number of snapshots to retain
   * @return this for method chaining
   */
  ExpireSnapshots retainLast(int numSnapshots);

  /**
   * Passes an alternative delete implementation that will be used for manifests and data files.
   *
   * <p>Manifest files that are no longer used by valid snapshots will be deleted. Data files that
   * were deleted by snapshots that are expired will be deleted.
   *
   * <p>If this method is not called, unnecessary manifests and data files will still be deleted.
   *
   * @param deleteFunc a function that will be called to delete manifests and data files
   * @return this for method chaining
   */
  ExpireSnapshots deleteWith(Consumer<String> deleteFunc);

  /**
   * Passes an alternative executor service that will be used for manifests and data files deletion.
   *
   * <p>Manifest files that are no longer used by valid snapshots will be deleted. Data files that
   * were deleted by snapshots that are expired will be deleted.
   *
   * <p>If this method is not called, unnecessary manifests and data files will still be deleted
   * using a single threaded executor service.
   *
   * @param executorService an executor service to parallelize tasks to delete manifests and data
   *     files
   * @return this for method chaining
   */
  ExpireSnapshots executeDeleteWith(ExecutorService executorService);

  /**
   * Passes an alternative executor service that will be used for planning. If this method is not
   * called, the default worker pool will be used.
   *
   * @param executorService an executor service to plan
   * @return this for method chaining
   */
  ExpireSnapshots planWith(ExecutorService executorService);

  /**
   * Allows expiration of snapshots without any cleanup of underlying manifest or data files.
   *
   * <p>Allows control in removing data and manifest files which may be more efficiently removed
   * using a distributed framework through the actions API.
   *
   * @param clean setting this to false will skip deleting expired manifests and files
   * @return this for method chaining
   * @deprecated since 1.11.0, will be removed in 2.0.0; use {@link #cleanupLevel(CleanupLevel)}
   *     instead.
   */
  @Deprecated
  ExpireSnapshots cleanExpiredFiles(boolean clean);

  /**
   * Configures the cleanup level for expired files.
   *
   * <p>This method provides fine-grained control over which files are cleaned up during snapshot
   * expiration.
   *
   * <p>Consider {@link CleanupLevel#METADATA_ONLY} when data files are shared across tables or when
   * using procedures like add-files that may reference the same data files.
   *
   * <p>Consider {@link CleanupLevel#NONE} when data and metadata files may be more efficiently
   * removed using a distributed framework through the actions API.
   *
   * @param level the cleanup level to use for expired snapshots
   * @return this for method chaining
   */
  default ExpireSnapshots cleanupLevel(CleanupLevel level) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " doesn't implement cleanupLevel");
  }

  /**
   * Enable cleaning up unused metadata, such as partition specs, schemas, etc.
   *
   * @param clean remove unused partition specs, schemas, or other metadata when true
   * @return this for method chaining
   */
  default ExpireSnapshots cleanExpiredMetadata(boolean clean) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " doesn't implement cleanExpiredMetadata");
  }
}
