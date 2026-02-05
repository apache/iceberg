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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;
import org.apache.iceberg.util.Tasks;

/**
 * Helper class for retrieving file changes in a snapshot with caching.
 *
 * <p>This class caches the results of file change detection operations, making it efficient to
 * query multiple file change types for the same snapshot. By default, manifests are read
 * sequentially. Use {@link Builder#executeWith(ExecutorService)} to enable parallel reading.
 */
public class SnapshotFileChanges {
  private final Snapshot snapshot;
  private final FileIO io;
  private final Map<Integer, PartitionSpec> specsById;
  private final ExecutorService executorService;

  private List<DataFile> addedDataFiles = null;
  private List<DataFile> removedDataFiles = null;
  private List<DeleteFile> addedDeleteFiles = null;
  private List<DeleteFile> removedDeleteFiles = null;

  private SnapshotFileChanges(
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
   * Create a builder for SnapshotFileChanges.
   *
   * @param snapshot the snapshot to detect file changes for
   * @param io a {@link FileIO} instance used for reading files from storage
   * @param specsById a map of partition spec IDs to partition specs
   * @return a new Builder
   */
  public static Builder builder(
      Snapshot snapshot, FileIO io, Map<Integer, PartitionSpec> specsById) {
    return new Builder(snapshot, io, specsById);
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
    List<ManifestFile> changedManifests =
        Lists.newArrayList(
            Iterables.filter(
                Iterables.filter(
                    snapshot.allManifests(io),
                    manifest -> manifest.content() == ManifestContent.DATA),
                manifest -> Objects.equals(manifest.snapshotId(), snapshot.snapshotId())));

    Queue<DataFileChanges> fileChangesByManifest = Queues.newConcurrentLinkedQueue();
    Tasks.foreach(changedManifests)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(executorService)
        .run(manifest -> fileChangesByManifest.add(readDataFileChanges(manifest)));

    ImmutableList.Builder<DataFile> adds = ImmutableList.builder();
    ImmutableList.Builder<DataFile> deletes = ImmutableList.builder();
    for (DataFileChanges changes : fileChangesByManifest) {
      adds.addAll(changes.added);
      deletes.addAll(changes.removed);
    }

    this.addedDataFiles = adds.build();
    this.removedDataFiles = deletes.build();
  }

  private DataFileChanges readDataFileChanges(ManifestFile manifest) {
    List<DataFile> adds = Lists.newArrayList();
    List<DataFile> deletes = Lists.newArrayList();

    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, io, specsById)) {
      for (ManifestEntry<DataFile> entry : reader.entries()) {
        switch (entry.status()) {
          case ADDED:
            adds.add(entry.file().copy());
            break;
          case DELETED:
            deletes.add(entry.file().copyWithoutStats());
            break;
          default:
            // ignore existing
        }
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close manifest reader");
    }

    return new DataFileChanges(adds, deletes);
  }

  private void cacheDeleteFileChanges() {
    List<ManifestFile> changedManifests =
        Lists.newArrayList(
            Iterables.filter(
                Iterables.filter(
                    snapshot.allManifests(io),
                    manifest -> manifest.content() == ManifestContent.DELETES),
                manifest -> Objects.equals(manifest.snapshotId(), snapshot.snapshotId())));

    Queue<DeleteFileChanges> fileChangesByManifest = Queues.newConcurrentLinkedQueue();
    Tasks.foreach(changedManifests)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(executorService)
        .run(manifest -> fileChangesByManifest.add(readDeleteFileChanges(manifest)));

    ImmutableList.Builder<DeleteFile> adds = ImmutableList.builder();
    ImmutableList.Builder<DeleteFile> deletes = ImmutableList.builder();
    for (DeleteFileChanges changes : fileChangesByManifest) {
      adds.addAll(changes.added);
      deletes.addAll(changes.removed);
    }

    this.addedDeleteFiles = adds.build();
    this.removedDeleteFiles = deletes.build();
  }

  private DeleteFileChanges readDeleteFileChanges(ManifestFile manifest) {
    List<DeleteFile> adds = Lists.newArrayList();
    List<DeleteFile> deletes = Lists.newArrayList();

    try (ManifestReader<DeleteFile> reader =
        ManifestFiles.readDeleteManifest(manifest, io, specsById)) {
      for (ManifestEntry<DeleteFile> entry : reader.entries()) {
        switch (entry.status()) {
          case ADDED:
            adds.add(entry.file().copy());
            break;
          case DELETED:
            deletes.add(entry.file().copyWithoutStats());
            break;
          default:
            // ignore existing
        }
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close manifest reader");
    }

    return new DeleteFileChanges(adds, deletes);
  }

  private static class DataFileChanges {
    private final List<DataFile> added;
    private final List<DataFile> removed;

    DataFileChanges(List<DataFile> added, List<DataFile> removed) {
      this.added = added;
      this.removed = removed;
    }
  }

  private static class DeleteFileChanges {
    private final List<DeleteFile> added;
    private final List<DeleteFile> removed;

    DeleteFileChanges(List<DeleteFile> added, List<DeleteFile> removed) {
      this.added = added;
      this.removed = removed;
    }
  }

  public static class Builder {
    private final Snapshot snapshot;
    private final FileIO io;
    private final Map<Integer, PartitionSpec> specsById;
    private ExecutorService executorService = null;

    private Builder(Snapshot snapshot, FileIO io, Map<Integer, PartitionSpec> specsById) {
      this.snapshot = snapshot;
      this.io = io;
      this.specsById = specsById;
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
     * Build the SnapshotFileChanges instance.
     *
     * @return a new SnapshotFileChanges instance
     */
    public SnapshotFileChanges build() {
      return new SnapshotFileChanges(snapshot, io, specsById, executorService);
    }
  }
}
