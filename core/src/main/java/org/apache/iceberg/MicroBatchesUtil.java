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
import java.util.stream.Collectors;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;

public class MicroBatchesUtil {

  private MicroBatchesUtil() {}

  public static List<Pair<ManifestFile, Integer>> skippedManifestIndexesFromSnapshot(
      FileIO io, Snapshot snapshot, long startFileIndex, boolean scanAllFiles) {
    //  Preconditions.checkArgument(startFileIndex >= 0, "startFileIndex is unexpectedly smaller
    // than 0");
    List<ManifestFile> manifests =
        scanAllFiles
            ? snapshot.dataManifests(io)
            : snapshot.dataManifests(io).stream()
                .filter(m -> m.snapshotId().equals(snapshot.snapshotId()))
                .collect(Collectors.toList());

    List<Pair<ManifestFile, Integer>> manifestIndexes = MicroBatchesUtil.indexManifests(manifests);

    return MicroBatchesUtil.skipManifests(manifestIndexes, startFileIndex);
  }

  public static CloseableIterable<FileScanTask> openManifestFile(
      FileIO io,
      Map<Integer, PartitionSpec> specsById,
      boolean caseSensitive,
      Snapshot snapshot,
      ManifestFile manifestFile,
      boolean scanAllFiles) {

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

  /**
   * Method to index the data files for each manifest. For example, if manifest m1 has 3 data files,
   * manifest m2 has 2 data files, manifest m3 has 1 data file, then the index will be (m1, 0), (m2,
   * 3), (m3, 5).
   *
   * @param manifestFiles List of input manifests used to index.
   * @return a list of manifest index with key as manifest file, value as file counts.
   */
  private static List<Pair<ManifestFile, Integer>> indexManifests(
      List<ManifestFile> manifestFiles) {
    int currentFileIndex = 0;
    List<Pair<ManifestFile, Integer>> manifestIndexes = Lists.newArrayList();

    for (ManifestFile manifest : manifestFiles) {
      manifestIndexes.add(Pair.of(manifest, currentFileIndex));
      currentFileIndex += manifest.addedFilesCount() + manifest.existingFilesCount();
    }

    return manifestIndexes;
  }

  /**
   * Method to skip the manifest file in which the index is smaller than startFileIndex. For
   * example, if the index list is : (m1, 0), (m2, 3), (m3, 5), and startFileIndex is 4, then the
   * returned manifest index list is: (m2, 3), (m3, 5).
   *
   * @param indexedManifests List of input manifests.
   * @param startFileIndex Index used to skip the processed manifests.
   * @return a sub-list of manifest file index which only contains the manifest indexes larger than
   *     the startFileIndex.
   */
  private static List<Pair<ManifestFile, Integer>> skipManifests(
      List<Pair<ManifestFile, Integer>> indexedManifests, long startFileIndex) {
    if (startFileIndex == 0) {
      return indexedManifests;
    }

    int manifestIndex = 0;
    for (Pair<ManifestFile, Integer> manifest : indexedManifests) {
      if (manifest.second() > startFileIndex) {
        break;
      }

      manifestIndex++;
    }

    return indexedManifests.subList(Math.max(manifestIndex - 1, 0), indexedManifests.size());
  }
}
