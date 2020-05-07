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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.Pair;

public class Snapshots {
  private Snapshots() {
  }

  public static MicroBatchBuilder from(Snapshot snapshot, FileIO io) {
    return new MicroBatchBuilder(snapshot, io);
  }

  public static class MicroBatch {
    private final long snapshotId;
    private final int startFileIndex;
    private final int endFileIndex;
    private final long sizeInBytes;
    private final CloseableIterable<FileScanTask> tasks;
    private final boolean lastIndexOfSnapshot;

    private MicroBatch(long snapshotId, int startFileIndex, int endFileIndex, long sizeInBytes,
               CloseableIterable<FileScanTask> tasks, boolean lastIndexOfSnapshot) {
      this.snapshotId = snapshotId;
      this.startFileIndex = startFileIndex;
      this.endFileIndex = endFileIndex;
      this.sizeInBytes = sizeInBytes;
      this.tasks = tasks;
      this.lastIndexOfSnapshot = lastIndexOfSnapshot;
    }

    public long snapshotId() {
      return snapshotId;
    }

    public int startFileIndex() {
      return startFileIndex;
    }

    public int endFileIndex() {
      return endFileIndex;
    }

    public long sizeInBytes() {
      return sizeInBytes;
    }

    public CloseableIterable<FileScanTask> tasks() {
      return tasks;
    }

    public boolean lastIndexOfSnapshot() {
      return lastIndexOfSnapshot;
    }
  }

  public static class MicroBatchBuilder {
    private final Snapshot snapshot;
    private final FileIO io;
    private Expression rowFilter;
    private boolean caseSensitive;
    private Map<Integer, PartitionSpec> specsById;

    private MicroBatchBuilder(Snapshot snapshot, FileIO io) {
      this.snapshot = snapshot;
      this.io = io;
      this.rowFilter = Expressions.alwaysTrue();
      this.caseSensitive = true;
    }

    public MicroBatchBuilder caseSensitive(boolean sensitive) {
      this.caseSensitive = sensitive;
      return this;
    }

    public MicroBatchBuilder filter(Expression newRowFilter) {
      this.rowFilter = newRowFilter;
      return this;
    }

    public MicroBatchBuilder specsById(Map<Integer, PartitionSpec> specs) {
      this.specsById = specs;
      return this;
    }

    public MicroBatch generate(int startFileIndex, long targetSizeInBytes, boolean isStarting) {
      List<ManifestFile> manifests = isStarting ? snapshot.manifests() :
          snapshot.manifests().stream().filter(m -> m.snapshotId().equals(snapshot.snapshotId()))
              .collect(Collectors.toList());

      List<Pair<ManifestFile, Integer>> manifestIndexes = indexManifests(manifests);
      List<Pair<ManifestFile, Integer>> skippedManifestIndexes = skipManifests(manifestIndexes, startFileIndex);

      return generateMicroBatch(skippedManifestIndexes, startFileIndex, targetSizeInBytes, isStarting);
    }

    private List<Pair<ManifestFile, Integer>> indexManifests(List<ManifestFile> manifestFiles) {
      int currentFileIndex = 0;
      List<Pair<ManifestFile, Integer>> manifestIndexes = Lists.newArrayList();

      for (ManifestFile manifest : manifestFiles) {
        int filesCount = manifest.addedFilesCount() + manifest.existingFilesCount();
        manifestIndexes.add(Pair.of(manifest, currentFileIndex));
        currentFileIndex += filesCount;
      }

      return manifestIndexes;
    }

    private List<Pair<ManifestFile, Integer>> skipManifests(List<Pair<ManifestFile, Integer>> indexedManifests,
                                                            int startFileIndex) {
      if (startFileIndex == 0) {
        return indexedManifests;
      }

      int index = 0;
      for (Pair<ManifestFile, Integer> manifest : indexedManifests) {
        if (manifest.second() > startFileIndex) {
          break;
        }

        index++;
      }

      return indexedManifests.subList(index - 1, indexedManifests.size());
    }

    private MicroBatch generateMicroBatch(List<Pair<ManifestFile, Integer>> indexedManifests,
                                          int startFileIndex, long targetSizeInBytes, boolean isStarting) {
      if (indexedManifests.isEmpty()) {
        return new MicroBatch(snapshot.snapshotId(), startFileIndex, startFileIndex + 1, 0L,
            CloseableIterable.empty(), true);
      }

      long currentSizeInBytes = 0L;
      int currentFileIndex = 0;
      List<CloseableIterable<FileScanTask>> batchTasks = Lists.newArrayList();

      for (Pair<ManifestFile, Integer> pair : indexedManifests) {
        currentFileIndex = pair.second();

        CloseableIterable<FileScanTask> tasks;
        if (isStarting) {
          tasks = full(ImmutableList.of(pair.first()));
        } else {
          tasks = appends(ImmutableList.of(pair.first()));
        }

        List<FileScanTask> batch = Lists.newArrayList();

        for (FileScanTask task : tasks) {
          if (currentFileIndex < startFileIndex) {
            currentFileIndex++;
            continue;
          }

          if (currentSizeInBytes + task.length() <= targetSizeInBytes) {
            batch.add(task);
            currentSizeInBytes += task.length();
          } else {
            batchTasks.add(CloseableIterable.combine(batch, tasks));
            return new MicroBatch(snapshot.snapshotId(), startFileIndex, currentFileIndex, currentSizeInBytes,
                CloseableIterable.concat(batchTasks), false);
          }

          currentFileIndex++;
        }

        batchTasks.add(CloseableIterable.combine(batch, tasks));
      }

      return new MicroBatch(snapshot.snapshotId(), startFileIndex, currentFileIndex + 1, currentSizeInBytes,
          CloseableIterable.concat(batchTasks), true);
    }

    private CloseableIterable<FileScanTask> full(List<ManifestFile> manifests) {
      return new ManifestGroup(io, manifests)
          .specsById(specsById)
          .caseSensitive(caseSensitive)
          .filterData(rowFilter)
          .planFiles();
    }

    private CloseableIterable<FileScanTask> appends(List<ManifestFile> manifests) {
      Iterable<ManifestFile> newManifests = Iterables.filter(manifests,
          manifest -> manifest.snapshotId() == snapshot.snapshotId());

      return new ManifestGroup(io, newManifests)
          .specsById(specsById)
          .caseSensitive(caseSensitive)
          .filterManifestEntries(entry ->
              entry.snapshotId() == snapshot.snapshotId() && entry.status() == ManifestEntry.Status.ADDED)
          .filterData(rowFilter)
          .ignoreDeleted()
          .planFiles();
    }
  }
}
