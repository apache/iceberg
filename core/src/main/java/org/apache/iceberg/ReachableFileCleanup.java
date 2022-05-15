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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File cleanup strategy for snapshot expiration which determines, via an in-memory reference set,
 * metadata and data files that are not reachable given the previous and current table states.
 */
class ReachableFileCleanup extends FileCleanupStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(ReachableFileCleanup.class);

  ReachableFileCleanup(
      FileIO fileIO,
      ExecutorService deleteExecutorService,
      ExecutorService planExecutorService,
      Consumer<String> deleteFunc) {
    super(fileIO, deleteExecutorService, planExecutorService, deleteFunc);
  }

  @Override
  public void cleanFiles(TableMetadata beforeExpiration, TableMetadata afterExpiration) {
    Set<String> manifestListsToDelete = Sets.newHashSet();

    Set<Snapshot> snapshotsBeforeExpiration = Sets.newHashSet(beforeExpiration.snapshots());
    Set<Snapshot> snapshotsAfterExpiration = Sets.newHashSet(afterExpiration.snapshots());
    Set<Snapshot> expiredSnapshots = Sets.newHashSet();
    for (Snapshot snapshot : snapshotsBeforeExpiration) {
      if (!snapshotsAfterExpiration.contains(snapshot)) {
        expiredSnapshots.add(snapshot);
        if (snapshot.manifestListLocation() != null) {
          manifestListsToDelete.add(snapshot.manifestListLocation());
        }
      }
    }
    Set<ManifestFile> deletionCandidates = readManifests(expiredSnapshots);

    if (!deletionCandidates.isEmpty()) {
      Set<ManifestFile> currentManifests = ConcurrentHashMap.newKeySet();
      Set<ManifestFile> manifestsToDelete =
          pruneReferencedManifests(
              snapshotsAfterExpiration, deletionCandidates, currentManifests::add);

      if (!manifestsToDelete.isEmpty()) {
        Set<String> dataFilesToDelete = findFilesToDelete(manifestsToDelete, currentManifests);
        deleteFiles(dataFilesToDelete, "data");
        Set<String> manifestPathsToDelete =
            manifestsToDelete.stream().map(ManifestFile::path).collect(Collectors.toSet());
        deleteFiles(manifestPathsToDelete, "manifest");
      }
    }

    deleteFiles(manifestListsToDelete, "manifest list");

    if (!beforeExpiration.statisticsFiles().isEmpty()) {
      deleteFiles(
          expiredStatisticsFilesLocations(beforeExpiration, afterExpiration), "statistics files");
    }
  }

  private Set<ManifestFile> pruneReferencedManifests(
      Set<Snapshot> snapshots,
      Set<ManifestFile> deletionCandidates,
      Consumer<ManifestFile> currentManifestCallback) {
    Set<ManifestFile> candidateSet = ConcurrentHashMap.newKeySet();
    candidateSet.addAll(deletionCandidates);
    Tasks.foreach(snapshots)
        .retry(3)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(planExecutorService)
        .onFailure(
            (snapshot, exc) ->
                LOG.warn(
                    "Failed to determine manifests for snapshot {}", snapshot.snapshotId(), exc))
        .run(
            snapshot -> {
              try (CloseableIterable<ManifestFile> manifestFiles = readManifests(snapshot)) {
                for (ManifestFile manifestFile : manifestFiles) {
                  candidateSet.remove(manifestFile);
                  if (candidateSet.isEmpty()) {
                    return;
                  }

                  currentManifestCallback.accept(manifestFile.copy());
                }
              } catch (IOException e) {
                throw new UncheckedIOException(
                    String.format(
                        "Failed to close manifest list: %s", snapshot.manifestListLocation()),
                    e);
              }
            });

    return candidateSet;
  }

  private Set<ManifestFile> readManifests(Set<Snapshot> snapshots) {
    Set<ManifestFile> manifestFiles = ConcurrentHashMap.newKeySet();
    Tasks.foreach(snapshots)
        .retry(3)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(planExecutorService)
        .onFailure(
            (snapshot, exc) ->
                LOG.warn(
                    "Failed to determine manifests for snapshot {}", snapshot.snapshotId(), exc))
        .run(
            snapshot -> {
              try (CloseableIterable<ManifestFile> manifests = readManifests(snapshot)) {
                for (ManifestFile manifestFile : manifests) {
                  manifestFiles.add(manifestFile.copy());
                }
              } catch (IOException e) {
                throw new UncheckedIOException(
                    String.format(
                        "Failed to close manifest list: %s", snapshot.manifestListLocation()),
                    e);
              }
            });

    return manifestFiles;
  }

  // Helper to determine data files to delete
  private Set<String> findFilesToDelete(
      Set<ManifestFile> manifestFilesToDelete, Set<ManifestFile> currentManifestFiles) {
    Set<String> filesToDelete = ConcurrentHashMap.newKeySet();

    Tasks.foreach(manifestFilesToDelete)
        .retry(3)
        .suppressFailureWhenFinished()
        .executeWith(planExecutorService)
        .onFailure(
            (item, exc) ->
                LOG.warn(
                    "Failed to determine live files in manifest {}. Retrying", item.path(), exc))
        .run(
            manifest -> {
              try (CloseableIterable<String> paths = ManifestFiles.readPaths(manifest, fileIO)) {
                paths.forEach(filesToDelete::add);
              } catch (IOException e) {
                throw new UncheckedIOException(
                    String.format("Failed to read manifest file: %s", manifest), e);
              }
            });

    if (filesToDelete.isEmpty()) {
      return filesToDelete;
    }

    try {
      Tasks.foreach(currentManifestFiles)
          .retry(3)
          .stopOnFailure()
          .throwFailureWhenFinished()
          .executeWith(planExecutorService)
          .onFailure(
              (item, exc) ->
                  LOG.warn(
                      "Failed to determine live files in manifest {}. Retrying", item.path(), exc))
          .run(
              manifest -> {
                if (filesToDelete.isEmpty()) {
                  return;
                }

                // Remove all the live files from the candidate deletion set
                try (CloseableIterable<String> paths = ManifestFiles.readPaths(manifest, fileIO)) {
                  paths.forEach(filesToDelete::remove);
                } catch (IOException e) {
                  throw new UncheckedIOException(
                      String.format("Failed to read manifest file: %s", manifest), e);
                }
              });

    } catch (Throwable e) {
      LOG.warn("Failed to list all reachable files", e);
      return Sets.newHashSet();
    }

    return filesToDelete;
  }
}
