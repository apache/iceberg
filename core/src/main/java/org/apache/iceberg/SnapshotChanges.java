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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.ParallelIterable;

/**
 * Helper class for retrieving file changes in a snapshot.
 *
 * <p>Two access modes are provided. The cached accessors ({@link #addedDataFiles()}, {@link
 * #removedDataFiles()}, {@link #addedDeleteFiles()}, {@link #removedDeleteFiles()}) eagerly
 * materialize file changes into in-memory lists and cache them, making it efficient to query the
 * same change type multiple times. Added and removed files of the same content type are cached by a
 * single pass over the snapshot's manifests, so reading both costs one read per manifest. The
 * streaming accessors ({@link #addedDataFilesIterable()}, {@link #removedDataFilesIterable()},
 * {@link #addedDeleteFilesIterable()}, {@link #removedDeleteFilesIterable()}) return
 * lazily-evaluated {@link CloseableIterable}s that the caller must close and that are not cached.
 *
 * <p>By default, manifests are read single-threaded. Use {@link
 * Builder#executeWith(ExecutorService)} to enable parallel manifest reading.
 */
public class SnapshotChanges {
  private static final Predicate<ManifestEntry.Status> ADDED_OR_DELETED =
      status -> status != ManifestEntry.Status.EXISTING;

  private final Snapshot snapshot;
  private final FileIO io;
  private final Map<Integer, PartitionSpec> specsById;
  private final ExecutorService executorService;

  private List<DataFile> addedDataFiles = null;
  private List<DataFile> removedDataFiles = null;
  private List<DeleteFile> addedDeleteFiles = null;
  private List<DeleteFile> removedDeleteFiles = null;

  private SnapshotChanges(
      Snapshot snapshot,
      FileIO io,
      Map<Integer, PartitionSpec> specsById,
      ExecutorService executorService) {
    Preconditions.checkArgument(snapshot != null, "Snapshot cannot be null");
    Preconditions.checkArgument(io != null, "FileIO cannot be null");
    Preconditions.checkArgument(specsById != null, "Partition specs cannot be null");
    this.snapshot = snapshot;
    this.io = io;
    this.specsById = specsById;
    this.executorService = executorService;
  }

  /**
   * Create a builder for SnapshotChanges using the table's current snapshot.
   *
   * @param table the table to detect file changes for
   * @return a new Builder
   */
  public static Builder builderFor(Table table) {
    return new Builder(table.currentSnapshot(), table.io(), table.specs());
  }

  static Builder builderFor(Snapshot snapshot, FileIO io, Map<Integer, PartitionSpec> specsById) {
    return new Builder(snapshot, io, specsById);
  }

  private <T> CloseableIterable<T> iterate(Iterable<CloseableIterable<T>> tasks) {
    if (executorService != null) {
      return new ParallelIterable<>(tasks, executorService);
    } else {
      return CloseableIterable.concat(tasks);
    }
  }

  /**
   * Returns all data files added to the table in this snapshot.
   *
   * <p>The result is materialized into memory and cached, so repeated calls return the same list.
   * The same pass also caches {@link #removedDataFiles()}. For a lazily-evaluated, streaming view,
   * use {@link #addedDataFilesIterable()}.
   */
  public Iterable<DataFile> addedDataFiles() {
    if (addedDataFiles == null) {
      cacheDataFileChanges();
    }

    return addedDataFiles;
  }

  /**
   * Returns all data files removed from the table in this snapshot.
   *
   * <p>The result is materialized into memory and cached, so repeated calls return the same list.
   * The same pass also caches {@link #addedDataFiles()}. For a lazily-evaluated, streaming view,
   * use {@link #removedDataFilesIterable()}.
   */
  public Iterable<DataFile> removedDataFiles() {
    if (removedDataFiles == null) {
      cacheDataFileChanges();
    }

    return removedDataFiles;
  }

  /**
   * Returns all delete files added to the table in this snapshot.
   *
   * <p>The result is materialized into memory and cached, so repeated calls return the same list.
   * The same pass also caches {@link #removedDeleteFiles()}. For a lazily-evaluated, streaming
   * view, use {@link #addedDeleteFilesIterable()}.
   */
  public Iterable<DeleteFile> addedDeleteFiles() {
    if (addedDeleteFiles == null) {
      cacheDeleteFileChanges();
    }

    return addedDeleteFiles;
  }

  /**
   * Returns all delete files removed from the table in this snapshot.
   *
   * <p>The result is materialized into memory and cached, so repeated calls return the same list.
   * The same pass also caches {@link #addedDeleteFiles()}. For a lazily-evaluated, streaming view,
   * use {@link #removedDeleteFilesIterable()}.
   */
  public Iterable<DeleteFile> removedDeleteFiles() {
    if (removedDeleteFiles == null) {
      cacheDeleteFileChanges();
    }

    return removedDeleteFiles;
  }

  /**
   * Returns a lazily-evaluated, streaming view of all data files added to the table in this
   * snapshot.
   *
   * <p>Unlike {@link #addedDataFiles()}, the result is not cached: each invocation returns a fresh
   * {@link CloseableIterable} that reads manifests on demand. Manifests are read single-threaded
   * unless an executor was configured via {@link Builder#executeWith(ExecutorService)}, in which
   * case manifests are read in parallel with a bounded queue.
   *
   * <p>The caller is responsible for closing the returned iterable. Returned {@link DataFile}
   * instances are defensive copies that retain column statistics.
   *
   * @return a closeable iterable over data files added in this snapshot
   */
  public CloseableIterable<DataFile> addedDataFilesIterable() {
    return CloseableIterable.transform(
        changedDataFiles(only(ManifestEntry.Status.ADDED)), Pair::second);
  }

  /**
   * Returns a lazily-evaluated, streaming view of all data files removed from the table in this
   * snapshot.
   *
   * <p>Unlike {@link #removedDataFiles()}, the result is not cached: each invocation returns a
   * fresh {@link CloseableIterable} that reads manifests on demand. Manifests are read
   * single-threaded unless an executor was configured via {@link
   * Builder#executeWith(ExecutorService)}, in which case manifests are read in parallel with a
   * bounded queue.
   *
   * <p>The caller is responsible for closing the returned iterable. Returned {@link DataFile}
   * instances are defensive copies without column statistics.
   *
   * @return a closeable iterable over data files removed in this snapshot
   */
  public CloseableIterable<DataFile> removedDataFilesIterable() {
    return CloseableIterable.transform(
        changedDataFiles(only(ManifestEntry.Status.DELETED)), Pair::second);
  }

  /**
   * Returns a lazily-evaluated, streaming view of all delete files added to the table in this
   * snapshot.
   *
   * <p>Unlike {@link #addedDeleteFiles()}, the result is not cached: each invocation returns a
   * fresh {@link CloseableIterable} that reads manifests on demand. Manifests are read
   * single-threaded unless an executor was configured via {@link
   * Builder#executeWith(ExecutorService)}, in which case manifests are read in parallel with a
   * bounded queue.
   *
   * <p>The caller is responsible for closing the returned iterable. Returned {@link DeleteFile}
   * instances are defensive copies that retain column statistics.
   *
   * @return a closeable iterable over delete files added in this snapshot
   */
  public CloseableIterable<DeleteFile> addedDeleteFilesIterable() {
    return CloseableIterable.transform(
        changedDeleteFiles(only(ManifestEntry.Status.ADDED)), Pair::second);
  }

  /**
   * Returns a lazily-evaluated, streaming view of all delete files removed from the table in this
   * snapshot.
   *
   * <p>Unlike {@link #removedDeleteFiles()}, the result is not cached: each invocation returns a
   * fresh {@link CloseableIterable} that reads manifests on demand. Manifests are read
   * single-threaded unless an executor was configured via {@link
   * Builder#executeWith(ExecutorService)}, in which case manifests are read in parallel with a
   * bounded queue.
   *
   * <p>The caller is responsible for closing the returned iterable. Returned {@link DeleteFile}
   * instances are defensive copies without column statistics.
   *
   * @return a closeable iterable over delete files removed in this snapshot
   */
  public CloseableIterable<DeleteFile> removedDeleteFilesIterable() {
    return CloseableIterable.transform(
        changedDeleteFiles(only(ManifestEntry.Status.DELETED)), Pair::second);
  }

  private static Predicate<ManifestEntry.Status> only(ManifestEntry.Status status) {
    return entryStatus -> entryStatus == status;
  }

  private void cacheDataFileChanges() {
    ImmutableList.Builder<DataFile> adds = ImmutableList.builder();
    ImmutableList.Builder<DataFile> removes = ImmutableList.builder();
    partition(changedDataFiles(ADDED_OR_DELETED), adds, removes);
    this.addedDataFiles = adds.build();
    this.removedDataFiles = removes.build();
  }

  private void cacheDeleteFileChanges() {
    ImmutableList.Builder<DeleteFile> adds = ImmutableList.builder();
    ImmutableList.Builder<DeleteFile> removes = ImmutableList.builder();
    partition(changedDeleteFiles(ADDED_OR_DELETED), adds, removes);
    this.addedDeleteFiles = adds.build();
    this.removedDeleteFiles = removes.build();
  }

  // drains the changes in a single pass, so that both caches are filled by one read per manifest
  private static <F extends ContentFile<F>> void partition(
      CloseableIterable<Pair<ManifestEntry.Status, F>> changes,
      ImmutableList.Builder<F> adds,
      ImmutableList.Builder<F> removes) {
    try (CloseableIterable<Pair<ManifestEntry.Status, F>> closeable = changes) {
      for (Pair<ManifestEntry.Status, F> change : closeable) {
        if (change.first() == ManifestEntry.Status.ADDED) {
          adds.add(change.second());
        } else {
          removes.add(change.second());
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close iterable", e);
    }
  }

  private CloseableIterable<Pair<ManifestEntry.Status, DataFile>> changedDataFiles(
      Predicate<ManifestEntry.Status> statusFilter) {
    return changedFiles(
        snapshot.dataManifests(io),
        manifest -> ManifestFiles.read(manifest, io, specsById),
        statusFilter);
  }

  private CloseableIterable<Pair<ManifestEntry.Status, DeleteFile>> changedDeleteFiles(
      Predicate<ManifestEntry.Status> statusFilter) {
    return changedFiles(
        snapshot.deleteManifests(io),
        manifest -> ManifestFiles.readDeleteManifest(manifest, io, specsById),
        statusFilter);
  }

  private <F extends ContentFile<F>> CloseableIterable<Pair<ManifestEntry.Status, F>> changedFiles(
      Iterable<ManifestFile> allManifests,
      Function<ManifestFile, ManifestReader<F>> readerFunction,
      Predicate<ManifestEntry.Status> statusFilter) {
    Iterable<ManifestFile> relevantManifests =
        Iterables.filter(
            allManifests, manifest -> Objects.equals(manifest.snapshotId(), snapshot.snapshotId()));

    Iterable<CloseableIterable<Pair<ManifestEntry.Status, F>>> manifestReadTasks =
        Iterables.transform(
            relevantManifests, manifest -> readManifest(manifest, readerFunction, statusFilter));

    return iterate(manifestReadTasks);
  }

  private <F extends ContentFile<F>> CloseableIterable<Pair<ManifestEntry.Status, F>> readManifest(
      ManifestFile manifest,
      Function<ManifestFile, ManifestReader<F>> readerFunction,
      Predicate<ManifestEntry.Status> statusFilter) {
    CloseableIterable<ManifestEntry<F>> entries = readerFunction.apply(manifest).entries();

    // filter before copying so that entries the caller did not ask for are never copied
    CloseableIterable<ManifestEntry<F>> matching =
        CloseableIterable.filter(entries, entry -> statusFilter.test(entry.status()));

    return CloseableIterable.transform(
        matching,
        entry -> {
          if (entry.status() == ManifestEntry.Status.ADDED) {
            return Pair.of(ManifestEntry.Status.ADDED, entry.file().copy());
          } else {
            return Pair.of(ManifestEntry.Status.DELETED, entry.file().copyWithoutStats());
          }
        });
  }

  public static class Builder {
    private Snapshot snapshot;
    private final FileIO io;
    private final Map<Integer, PartitionSpec> specsById;
    private ExecutorService executorService = null;

    private Builder(Snapshot snapshot, FileIO io, Map<Integer, PartitionSpec> specsById) {
      this.snapshot = snapshot;
      this.io = io;
      this.specsById = specsById;
    }

    /**
     * Set the snapshot to detect file changes for, overriding the default.
     *
     * @param snapshotOverride the snapshot to use
     * @return this builder for method chaining
     */
    public Builder snapshot(Snapshot snapshotOverride) {
      this.snapshot = snapshotOverride;
      return this;
    }

    /**
     * Configure an executor service to use for parallel manifest reading.
     *
     * @param executor the executor service to use for parallel execution
     * @return this builder for method chaining
     */
    public Builder executeWith(ExecutorService executor) {
      this.executorService = executor;
      return this;
    }

    /**
     * Build the SnapshotChanges instance.
     *
     * @return a new SnapshotChanges instance
     */
    public SnapshotChanges build() {
      return new SnapshotChanges(snapshot, io, specsById, executorService);
    }
  }
}
