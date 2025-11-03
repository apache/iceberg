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
import java.util.Map;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Reader for V4 root manifests that recursively expands manifest references.
 *
 * <p>Root manifests can contain direct file entries (DATA, POSITION_DELETES, EQUALITY_DELETES) as
 * well as references to leaf manifests (DATA_MANIFEST, DELETE_MANIFEST).
 *
 * <p>RootManifestReader is the orchestrator reader that handles the recursive expansion of manifest
 * references.
 */
public class RootManifestReader extends CloseableGroup {
  private final V4ManifestReader rootReader;
  private final FileIO io;
  private final Map<Integer, PartitionSpec> specsById;

  public RootManifestReader(
      String rootManifestPath,
      FileIO io,
      Map<Integer, PartitionSpec> specsById,
      long snapshotId,
      long sequenceNumber,
      Long firstRowId) {
    this.rootReader =
        V4ManifestReaders.readRoot(rootManifestPath, io, snapshotId, sequenceNumber, firstRowId);
    this.io = io;
    this.specsById = specsById;
    addCloseable(rootReader);
  }

  /**
   * Returns all TrackedFiles from the root manifest and all referenced leaf manifests.
   *
   * <p>This includes:
   *
   * <ul>
   *   <li>Direct file entries in root (DATA, POSITION_DELETES, EQUALITY_DELETES)
   *   <li>Files from expanded DATA_MANIFEST entries
   *   <li>Files from expanded DELETE_MANIFEST entries
   * </ul>
   *
   * <p>Returns TrackedFile entries (not converted to DataFile/DeleteFile yet).
   *
   * <p>TODO: Add manifest DV support.
   *
   * @return iterable of all tracked files
   */
  public CloseableIterable<TrackedFile<?>> allTrackedFiles() {
    return CloseableIterable.concat(Lists.newArrayList(directFiles(), expandManifests()));
  }

  /**
   * Returns direct file entries from the root manifest.
   *
   * <p>These are DATA, POSITION_DELETES, or EQUALITY_DELETES entries stored directly in the root
   * manifest
   */
  private CloseableIterable<TrackedFile<?>> directFiles() {
    return CloseableIterable.filter(
        rootReader.liveEntries(),
        tf ->
            tf.contentType() == FileContent.DATA
                || tf.contentType() == FileContent.POSITION_DELETES
                || tf.contentType() == FileContent.EQUALITY_DELETES);
  }

  /**
   * Expands manifest references (DATA_MANIFEST and DELETE_MANIFEST) to their contained files.
   *
   * <p>Loads all root entries to identify manifests, then reads each leaf manifest serially.
   *
   * <p>TODO: Add parallel manifest reading support via ExecutorService (like
   * ManifestGroup.planWith). This would use ParallelIterable to read leaf manifests concurrently.
   *
   * <p>TODO: Add manifest DV support - group MANIFEST_DV entries by referencedFile and apply to
   * leaf readers.
   */
  private CloseableIterable<TrackedFile<?>> expandManifests() {
    List<TrackedFile<?>> rootEntries = Lists.newArrayList(rootReader.liveEntries());

    List<CloseableIterable<TrackedFile<?>>> allLeafFiles = Lists.newArrayList();

    for (TrackedFile<?> manifestEntry : rootEntries) {
      if (manifestEntry.contentType() == FileContent.DATA_MANIFEST
          || manifestEntry.contentType() == FileContent.DELETE_MANIFEST) {
        V4ManifestReader leafReader = V4ManifestReaders.readLeaf(manifestEntry, io, specsById);
        addCloseable(leafReader);
        allLeafFiles.add(leafReader.liveEntries());
      }
    }

    return CloseableIterable.concat(allLeafFiles);
  }
}
