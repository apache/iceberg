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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MicroBatches {
  private MicroBatches() {}

  public static class MicroBatch {
    private final long snapshotId;
    private final long startFileIndex;
    private final long endFileIndex;
    private final long sizeInBytes;
    private final List<FileScanTask> tasks;
    private final boolean lastIndexOfSnapshot;

    private MicroBatch(
        long snapshotId,
        long startFileIndex,
        long endFileIndex,
        long sizeInBytes,
        List<FileScanTask> tasks,
        boolean lastIndexOfSnapshot) {
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

    public long startFileIndex() {
      return startFileIndex;
    }

    public long endFileIndex() {
      return endFileIndex;
    }

    public long sizeInBytes() {
      return sizeInBytes;
    }

    public List<FileScanTask> tasks() {
      return tasks;
    }

    public boolean lastIndexOfSnapshot() {
      return lastIndexOfSnapshot;
    }
  }

  public static MicroBatchBuilder from(Snapshot snapshot, FileIO io) {
    return new MicroBatchBuilder(snapshot, io);
  }

  public static class MicroBatchBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(MicroBatchBuilder.class);

    private final Snapshot snapshot;
    private final FileIO io;
    private boolean caseSensitive;
    private Map<Integer, PartitionSpec> specsById;

    private MicroBatchBuilder(Snapshot snapshot, FileIO io) {
      this.snapshot = snapshot;
      this.io = io;
      this.caseSensitive = true;
    }

    public MicroBatchBuilder caseSensitive(boolean sensitive) {
      this.caseSensitive = sensitive;
      return this;
    }

    public MicroBatchBuilder specsById(Map<Integer, PartitionSpec> specs) {
      this.specsById = specs;
      return this;
    }

    public MicroBatch generate(long startFileIndex, long targetSizeInBytes, boolean scanAllFiles) {
      Preconditions.checkArgument(
          startFileIndex >= 0, "startFileIndex is unexpectedly smaller than 0");
      Preconditions.checkArgument(
          targetSizeInBytes > 0, "targetSizeInBytes should be larger than 0");

      return generateMicroBatch(
          MicroBatchesUtil.skippedManifestIndexesFromSnapshot(snapshot, startFileIndex, scanAllFiles),
          startFileIndex, endFileIndex, targetSizeInBytes, scanAllFiles);
    }

    /**
     * Method to generate MicroBatch of this snapshot based on the indexed manifests, controlled by
     * targetSizeInBytes.
     *
     * @param indexedManifests A list of indexed manifests to generate MicroBatch
     * @param startFileIndex A startFileIndex used to skip processed files.
     * @param endFileIndex An endFileIndex used to find files to include, it not inclusive.
     * @param targetSizeInBytes Used to control the size of MicroBatch, the processed file bytes must be smaller than
     *                         this size.
     * @param scanAllFiles Used to check whether all the data files should be processed, or only added files.
     * @return A MicroBatch.
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private MicroBatch generateMicroBatch(
        List<Pair<ManifestFile, Integer>> indexedManifests,
        long startFileIndex, long endFileIndex, long targetSizeInBytes, boolean scanAllFiles) {
      if (indexedManifests.isEmpty()) {
        return new MicroBatch(snapshot.snapshotId(), startFileIndex, endFileIndex, 0L,
            Collections.emptyList(), true);
      }

      long currentSizeInBytes = 0L;
      int currentFileIndex = 0;
      boolean isLastIndex = false;
      List<FileScanTask> tasks = Lists.newArrayList();

      for (int idx = 0; idx < indexedManifests.size(); idx++) {
        currentFileIndex = indexedManifests.get(idx).second();

        try (CloseableIterable<FileScanTask> taskIterable = MicroBatchesUtil.openManifestFile(io, specsById,
            caseSensitive, snapshot, indexedManifests.get(idx).first(), scanAllFiles);
            CloseableIterator<FileScanTask> taskIter = taskIterable.iterator()) {
          while (taskIter.hasNext()) {
            FileScanTask task = taskIter.next();
            // want to read [startFileIndex ... endFileIndex)
            if (currentFileIndex >= startFileIndex && currentFileIndex < endFileIndex) {
              // Make sure there's at least one task in each MicroBatch to void job to be stuck, always add task
              // firstly.
              tasks.add(task);
              currentSizeInBytes += task.length();
            }

            currentFileIndex++;
            if (currentSizeInBytes >= targetSizeInBytes || currentFileIndex >= endFileIndex) {
              break;
            }
          }

          if (idx + 1 == indexedManifests.size() && !taskIter.hasNext()) {
            // If this is the last file scan task in last manifest, set the flag to true.
            isLastIndex = true;
          }
        } catch (IOException ioe) {
          LOG.warn("Failed to close task iterable", ioe);
        }

        if (currentSizeInBytes >= targetSizeInBytes) {
          if (tasks.size() > 1 && currentSizeInBytes > targetSizeInBytes) {
            // If there's more than 1 task in this batch, and the size exceeds the limit, we should
            // revert last
            // task to make sure we don't exceed the size limit.
            FileScanTask extraTask = tasks.remove(tasks.size() - 1);
            currentSizeInBytes -= extraTask.length();
            currentFileIndex--;
            isLastIndex = false;
          }

          break;
        }
      }

      return new MicroBatch(
          snapshot.snapshotId(),
          startFileIndex,
          currentFileIndex,
          currentSizeInBytes,
          tasks,
          isLastIndex);
      // [startFileIndex ....currentFileIndex)
      return new MicroBatch(snapshot.snapshotId(), startFileIndex, currentFileIndex, currentSizeInBytes,
          tasks, isLastIndex);
    }

    private CloseableIterable<FileScanTask> open(ManifestFile manifestFile, boolean scanAllFiles) {
      ManifestGroup manifestGroup =
          new ManifestGroup(io, ImmutableList.of(manifestFile))
              .specsById(specsById)
              .caseSensitive(caseSensitive);
      if (!scanAllFiles) {
        manifestGroup =
            manifestGroup
                .filterManifestEntries(
                    entry ->
                        entry.snapshotId() == snapshot.snapshotId()
                            && entry.status() == ManifestEntry.Status.ADDED)
                .ignoreDeleted();
      }

      return manifestGroup.planFiles();
    }
  }
}
