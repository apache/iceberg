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
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.ParallelIterable;

/**
 * Helper class for retrieving file changes in a snapshot with caching.
 *
 * <p>This class caches the results of file change detection operations, making it efficient to
 * query multiple file change types for the same snapshot. By default, manifests are read
 * single-threaded. Use {@link Builder#executeWith(ExecutorService)} to enable parallel manifest
 * reading.
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

  /** Returns all data files added to the table in this snapshot */
  public Iterable<DataFile> addedDataFiles() {
    if (addedDataFiles == null) {
      cacheDataFileChanges();
    }

    return addedDataFiles;
  }

  /** Returns all data files removed from the table in this snapshot. */
  public Iterable<DataFile> removedDataFiles() {
    if (removedDataFiles == null) {
      cacheDataFileChanges();
    }

    return removedDataFiles;
  }

  /** Returns all delete files added to the table in this snapshot. */
  public Iterable<DeleteFile> addedDeleteFiles() {
    if (addedDeleteFiles == null) {
      cacheDeleteFileChanges();
    }

    return addedDeleteFiles;
  }

  /** Returns all delete files removed from the table in this snapshot. */
  public Iterable<DeleteFile> removedDeleteFiles() {
    if (removedDeleteFiles == null) {
      cacheDeleteFileChanges();
    }

    return removedDeleteFiles;
  }

  private void cacheDataFileChanges() {
    ImmutableList.Builder<DataFile> adds = ImmutableList.builder();
    ImmutableList.Builder<DataFile> deletes = ImmutableList.builder();

    Iterable<ManifestFile> relevantDataManifests =
        Iterables.filter(
            snapshot.dataManifests(io),
            manifest -> Objects.equals(manifest.snapshotId(), snapshot.snapshotId()));

    Iterable<CloseableIterable<Pair<ManifestEntry.Status, DataFile>>> manifestReadTasks =
        Iterables.transform(relevantDataManifests, this::readDataManifest);

    try (CloseableIterable<Pair<ManifestEntry.Status, DataFile>> changedDataFiles =
        iterate(manifestReadTasks)) {
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

    Iterable<ManifestFile> relevantDeleteManifests =
        Iterables.filter(
            snapshot.deleteManifests(io),
            manifest -> Objects.equals(manifest.snapshotId(), snapshot.snapshotId()));

    Iterable<CloseableIterable<Pair<ManifestEntry.Status, DeleteFile>>> manifestReadTasks =
        Iterables.transform(relevantDeleteManifests, this::readDeleteManifest);

    try (CloseableIterable<Pair<ManifestEntry.Status, DeleteFile>> changedDeleteFiles =
        iterate(manifestReadTasks)) {
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
