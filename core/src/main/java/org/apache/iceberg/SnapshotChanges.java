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
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.ParallelIterable;

/**
 * Helper class for retrieving file changes in a snapshot.
 *
 * <p>Two access modes are provided. The cached accessors ({@link #addedDataFiles()}, {@link
 * #removedDataFiles()}, {@link #addedDeleteFiles()}, {@link #removedDeleteFiles()}) eagerly
 * materialize file changes into in-memory lists and cache them, making it efficient to query the
 * same change type multiple times. The streaming accessors ({@link #addedDataFilesIterable()},
 * {@link #removedDataFilesIterable()}, {@link #addedDeleteFilesIterable()}, {@link
 * #removedDeleteFilesIterable()}) return lazily-evaluated {@link CloseableIterable}s that the
 * caller must close and that are not cached.
 *
 * <p>By default, manifests are read single-threaded. Use {@link
 * Builder#executeWith(ExecutorService)} to enable parallel manifest reading.
 */
public class SnapshotChanges {
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
   * For a lazily-evaluated, streaming view, use {@link #addedDataFilesIterable()}.
   */
  public Iterable<DataFile> addedDataFiles() {
    if (addedDataFiles == null) {
      this.addedDataFiles = materialize(addedDataFilesIterable());
    }

    return addedDataFiles;
  }

  /**
   * Returns all data files removed from the table in this snapshot.
   *
   * <p>The result is materialized into memory and cached, so repeated calls return the same list.
   * For a lazily-evaluated, streaming view, use {@link #removedDataFilesIterable()}.
   */
  public Iterable<DataFile> removedDataFiles() {
    if (removedDataFiles == null) {
      this.removedDataFiles = materialize(removedDataFilesIterable());
    }

    return removedDataFiles;
  }

  /**
   * Returns all delete files added to the table in this snapshot.
   *
   * <p>The result is materialized into memory and cached, so repeated calls return the same list.
   * For a lazily-evaluated, streaming view, use {@link #addedDeleteFilesIterable()}.
   */
  public Iterable<DeleteFile> addedDeleteFiles() {
    if (addedDeleteFiles == null) {
      this.addedDeleteFiles = materialize(addedDeleteFilesIterable());
    }

    return addedDeleteFiles;
  }

  /**
   * Returns all delete files removed from the table in this snapshot.
   *
   * <p>The result is materialized into memory and cached, so repeated calls return the same list.
   * For a lazily-evaluated, streaming view, use {@link #removedDeleteFilesIterable()}.
   */
  public Iterable<DeleteFile> removedDeleteFiles() {
    if (removedDeleteFiles == null) {
      this.removedDeleteFiles = materialize(removedDeleteFilesIterable());
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
    return changedDataFiles(ManifestEntry.Status.ADDED);
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
    return changedDataFiles(ManifestEntry.Status.DELETED);
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
    return changedDeleteFiles(ManifestEntry.Status.ADDED);
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
    return changedDeleteFiles(ManifestEntry.Status.DELETED);
  }

  // materializes the iterable and releases resources so that the result can be cached
  private <T> List<T> materialize(CloseableIterable<T> iterable) {
    try (CloseableIterable<T> closeable = iterable) {
      return ImmutableList.copyOf(closeable);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close iterable", e);
    }
  }

  private CloseableIterable<DataFile> changedDataFiles(ManifestEntry.Status targetStatus) {
    return changedFiles(
        snapshot.dataManifests(io),
        manifest -> ManifestFiles.read(manifest, io, specsById),
        targetStatus);
  }

  private CloseableIterable<DeleteFile> changedDeleteFiles(ManifestEntry.Status targetStatus) {
    return changedFiles(
        snapshot.deleteManifests(io),
        manifest -> ManifestFiles.readDeleteManifest(manifest, io, specsById),
        targetStatus);
  }

  private <F extends ContentFile<F>> CloseableIterable<F> changedFiles(
      Iterable<ManifestFile> allManifests,
      Function<ManifestFile, ManifestReader<F>> readerFunction,
      ManifestEntry.Status targetStatus) {
    Iterable<ManifestFile> relevantManifests =
        Iterables.filter(
            allManifests, manifest -> Objects.equals(manifest.snapshotId(), snapshot.snapshotId()));

    Iterable<CloseableIterable<F>> manifestReadTasks =
        Iterables.transform(
            relevantManifests, manifest -> readManifest(manifest, readerFunction, targetStatus));

    return iterate(manifestReadTasks);
  }

  private <F extends ContentFile<F>> CloseableIterable<F> readManifest(
      ManifestFile manifest,
      Function<ManifestFile, ManifestReader<F>> readerFunction,
      ManifestEntry.Status targetStatus) {
    CloseableIterable<ManifestEntry<F>> entries = readerFunction.apply(manifest).entries();

    // keep only entries matching the requested change type; this also excludes EXISTING entries
    CloseableIterable<ManifestEntry<F>> matching =
        CloseableIterable.filter(entries, entry -> entry.status() == targetStatus);

    return CloseableIterable.transform(
        matching,
        entry ->
            targetStatus == ManifestEntry.Status.ADDED
                ? entry.file().copy()
                : entry.file().copyWithoutStats());
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
