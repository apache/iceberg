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
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.ParallelIterable;
import org.apache.iceberg.util.ThreadPools;

/**
 * Helper class for retrieving file changes across one or more snapshots.
 *
 * <p>The {@code read*} accessors return streaming {@link CloseableIterable}s and must be closed by
 * callers. The non-{@code read} accessors materialize and cache file changes, making it efficient
 * to query multiple file change types for the same set of snapshots. All accessors return the union
 * of changes across the configured snapshots.
 *
 * <p>By default, manifests are read single-threaded when only one snapshot is configured. When more
 * than one snapshot is configured the shared {@link ThreadPools#getWorkerPool()} is used so that
 * manifest reads are parallelised across snapshot boundaries. Use {@link
 * Builder#executeWith(ExecutorService)} to supply a custom executor in either case.
 *
 * <p>Each manifest is attributed to exactly one snapshot via {@link ManifestFile#snapshotId()}, so
 * the multi-snapshot path never reads the same manifest twice even when the configured snapshots
 * share an ancestor chain.
 */
public class SnapshotChanges {
  private final List<Snapshot> snapshots;
  private final FileIO io;
  private final Map<Integer, PartitionSpec> specsById;
  private final ExecutorService executorService;

  private List<DataFile> addedDataFiles = null;
  private List<DataFile> removedDataFiles = null;
  private List<DeleteFile> addedDeleteFiles = null;
  private List<DeleteFile> removedDeleteFiles = null;

  private SnapshotChanges(
      List<Snapshot> snapshots,
      FileIO io,
      Map<Integer, PartitionSpec> specsById,
      ExecutorService executorService) {
    Preconditions.checkArgument(snapshots != null, "Snapshots cannot be null");
    Preconditions.checkArgument(!snapshots.isEmpty(), "Snapshots cannot be empty");
    Preconditions.checkArgument(io != null, "FileIO cannot be null");
    Preconditions.checkArgument(specsById != null, "Partition specs cannot be null");
    for (Snapshot snapshot : snapshots) {
      Preconditions.checkArgument(snapshot != null, "Snapshot cannot be null");
    }
    this.snapshots = ImmutableList.copyOf(snapshots);
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
    Builder builder = new Builder(table.io(), table.specs());
    if (table.currentSnapshot() != null) {
      builder.snapshot(table.currentSnapshot());
    }
    return builder;
  }

  /**
   * Create a builder for SnapshotChanges over a fixed set of snapshots.
   *
   * @param table the table the snapshots belong to (used for {@link FileIO} and partition specs)
   * @param snapshots the snapshots to detect file changes for; must be non-empty
   * @return a new Builder
   */
  public static Builder builderFor(Table table, Iterable<Snapshot> snapshots) {
    return new Builder(table.io(), table.specs()).snapshots(snapshots);
  }

  static Builder builderFor(Snapshot snapshot, FileIO io, Map<Integer, PartitionSpec> specsById) {
    return new Builder(io, specsById).snapshot(snapshot);
  }

  static Builder builderFor(
      Iterable<Snapshot> snapshots, FileIO io, Map<Integer, PartitionSpec> specsById) {
    return new Builder(io, specsById).snapshots(snapshots);
  }

  private <T> CloseableIterable<T> iterate(Iterable<CloseableIterable<T>> tasks) {
    if (executorService != null) {
      return new ParallelIterable<>(tasks, executorService);
    } else if (snapshots.size() > 1) {
      // Multi-snapshot mode defaults to the shared worker pool to keep manifest reads
      // saturated across snapshot boundaries. Single-snapshot mode keeps the historical
      // serial-by-default behaviour to avoid surprising existing callers.
      return new ParallelIterable<>(tasks, ThreadPools.getWorkerPool());
    } else {
      return CloseableIterable.concat(tasks);
    }
  }

  /**
   * Returns a closeable iterable of data files added across the configured snapshots.
   *
   * <p>Callers are responsible for closing the returned iterable.
   */
  public CloseableIterable<DataFile> readAddedDataFiles() {
    return readDataFiles(ManifestEntry.Status.ADDED);
  }

  /**
   * Returns a closeable iterable of data files removed across the configured snapshots.
   *
   * <p>Callers are responsible for closing the returned iterable.
   */
  public CloseableIterable<DataFile> readRemovedDataFiles() {
    return readDataFiles(ManifestEntry.Status.DELETED);
  }

  /**
   * Returns a closeable iterable of delete files added across the configured snapshots.
   *
   * <p>Callers are responsible for closing the returned iterable.
   */
  public CloseableIterable<DeleteFile> readAddedDeleteFiles() {
    return readDeleteFiles(ManifestEntry.Status.ADDED);
  }

  /**
   * Returns a closeable iterable of delete files removed across the configured snapshots.
   *
   * <p>Callers are responsible for closing the returned iterable.
   */
  public CloseableIterable<DeleteFile> readRemovedDeleteFiles() {
    return readDeleteFiles(ManifestEntry.Status.DELETED);
  }

  /** Returns all data files added across the configured snapshots. */
  public Iterable<DataFile> addedDataFiles() {
    if (addedDataFiles == null) {
      cacheDataFileChanges();
    }

    return addedDataFiles;
  }

  /** Returns all data files removed across the configured snapshots. */
  public Iterable<DataFile> removedDataFiles() {
    if (removedDataFiles == null) {
      cacheDataFileChanges();
    }

    return removedDataFiles;
  }

  /** Returns all delete files added across the configured snapshots. */
  public Iterable<DeleteFile> addedDeleteFiles() {
    if (addedDeleteFiles == null) {
      cacheDeleteFileChanges();
    }

    return addedDeleteFiles;
  }

  /** Returns all delete files removed across the configured snapshots. */
  public Iterable<DeleteFile> removedDeleteFiles() {
    if (removedDeleteFiles == null) {
      cacheDeleteFileChanges();
    }

    return removedDeleteFiles;
  }

  private void cacheDataFileChanges() {
    ImmutableList.Builder<DataFile> adds = ImmutableList.builder();
    ImmutableList.Builder<DataFile> deletes = ImmutableList.builder();

    try (CloseableIterable<Pair<ManifestEntry.Status, DataFile>> changedDataFiles =
        readDataFileChanges()) {
      for (Pair<ManifestEntry.Status, DataFile> pair : changedDataFiles) {
        switch (pair.first()) {
          case ADDED:
            adds.add(pair.second());
            break;
          case DELETED:
            deletes.add(pair.second());
            break;
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close manifest reader", e);
    }

    this.addedDataFiles = adds.build();
    this.removedDataFiles = deletes.build();
  }

  private CloseableIterable<DataFile> readDataFiles(ManifestEntry.Status status) {
    CloseableIterable<Pair<ManifestEntry.Status, DataFile>> changes = readDataFileChanges();
    CloseableIterable<Pair<ManifestEntry.Status, DataFile>> matching =
        CloseableIterable.filter(changes, pair -> pair.first() == status);
    return CloseableIterable.transform(matching, Pair::second);
  }

  private CloseableIterable<Pair<ManifestEntry.Status, DataFile>> readDataFileChanges() {
    Iterable<CloseableIterable<Pair<ManifestEntry.Status, DataFile>>> manifestReadTasks =
        Iterables.concat(
            Iterables.transform(
                snapshots,
                snapshot ->
                    Iterables.transform(
                        Iterables.filter(
                            snapshot.dataManifests(io),
                            manifest ->
                                Objects.equals(manifest.snapshotId(), snapshot.snapshotId())),
                        this::readDataManifest)));

    return iterate(manifestReadTasks);
  }

  private CloseableIterable<Pair<ManifestEntry.Status, DataFile>> readDataManifest(
      ManifestFile manifest) {
    CloseableIterable<ManifestEntry<DataFile>> entries =
        ManifestFiles.read(manifest, io, specsById).entries();

    CloseableIterable<ManifestEntry<DataFile>> relevant =
        CloseableIterable.filter(entries, e -> e.status() != ManifestEntry.Status.EXISTING);

    return CloseableIterable.transform(
        relevant,
        entry -> {
          if (entry.status() == ManifestEntry.Status.ADDED) {
            return Pair.of(ManifestEntry.Status.ADDED, entry.file().copy());
          } else {
            return Pair.of(ManifestEntry.Status.DELETED, entry.file().copyWithoutStats());
          }
        });
  }

  private void cacheDeleteFileChanges() {
    ImmutableList.Builder<DeleteFile> adds = ImmutableList.builder();
    ImmutableList.Builder<DeleteFile> deletes = ImmutableList.builder();

    try (CloseableIterable<Pair<ManifestEntry.Status, DeleteFile>> changedDeleteFiles =
        readDeleteFileChanges()) {
      for (Pair<ManifestEntry.Status, DeleteFile> pair : changedDeleteFiles) {
        switch (pair.first()) {
          case ADDED:
            adds.add(pair.second());
            break;
          case DELETED:
            deletes.add(pair.second());
            break;
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close manifest reader", e);
    }

    this.addedDeleteFiles = adds.build();
    this.removedDeleteFiles = deletes.build();
  }

  private CloseableIterable<DeleteFile> readDeleteFiles(ManifestEntry.Status status) {
    CloseableIterable<Pair<ManifestEntry.Status, DeleteFile>> changes = readDeleteFileChanges();
    CloseableIterable<Pair<ManifestEntry.Status, DeleteFile>> matching =
        CloseableIterable.filter(changes, pair -> pair.first() == status);
    return CloseableIterable.transform(matching, Pair::second);
  }

  private CloseableIterable<Pair<ManifestEntry.Status, DeleteFile>> readDeleteFileChanges() {
    Iterable<CloseableIterable<Pair<ManifestEntry.Status, DeleteFile>>> manifestReadTasks =
        Iterables.concat(
            Iterables.transform(
                snapshots,
                snapshot ->
                    Iterables.transform(
                        Iterables.filter(
                            snapshot.deleteManifests(io),
                            manifest ->
                                Objects.equals(manifest.snapshotId(), snapshot.snapshotId())),
                        this::readDeleteManifest)));

    return iterate(manifestReadTasks);
  }

  private CloseableIterable<Pair<ManifestEntry.Status, DeleteFile>> readDeleteManifest(
      ManifestFile manifest) {
    CloseableIterable<ManifestEntry<DeleteFile>> entries =
        ManifestFiles.readDeleteManifest(manifest, io, specsById).entries();

    CloseableIterable<ManifestEntry<DeleteFile>> relevant =
        CloseableIterable.filter(entries, e -> e.status() != ManifestEntry.Status.EXISTING);

    return CloseableIterable.transform(
        relevant,
        entry -> {
          if (entry.status() == ManifestEntry.Status.ADDED) {
            return Pair.of(ManifestEntry.Status.ADDED, entry.file().copy());
          } else {
            return Pair.of(ManifestEntry.Status.DELETED, entry.file().copyWithoutStats());
          }
        });
  }

  public static class Builder {
    private final FileIO io;
    private final Map<Integer, PartitionSpec> specsById;
    private final List<Snapshot> snapshots = Lists.newArrayList();
    private ExecutorService executorService = null;

    private Builder(FileIO io, Map<Integer, PartitionSpec> specsById) {
      this.io = io;
      this.specsById = specsById;
    }

    /**
     * Set the snapshot to detect file changes for, replacing any previously configured snapshots.
     *
     * @param snapshot the snapshot to use
     * @return this builder for method chaining
     */
    public Builder snapshot(Snapshot snapshot) {
      this.snapshots.clear();
      this.snapshots.add(snapshot);
      return this;
    }

    /**
     * Set the snapshots to detect file changes for, replacing any previously configured snapshots.
     * The accessors on the resulting {@link SnapshotChanges} return the union of changes across all
     * of these snapshots.
     *
     * @param newSnapshots the snapshots to use; must be non-empty
     * @return this builder for method chaining
     */
    public Builder snapshots(Iterable<Snapshot> newSnapshots) {
      Preconditions.checkArgument(newSnapshots != null, "Snapshots cannot be null");
      this.snapshots.clear();
      Iterables.addAll(this.snapshots, newSnapshots);
      return this;
    }

    /**
     * Configure an executor service to use for parallel manifest reading. When unset and more than
     * one snapshot is configured, the shared {@link ThreadPools#getWorkerPool()} is used.
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
      return new SnapshotChanges(snapshots, io, specsById, executorService);
    }
  }
}
