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
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.commons.compress.utils.Sets;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManifestExpirationManager implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ManifestExpirationManager.class);

  private final Set<Long> validSnapshotIds;
  private final Map<Integer, PartitionSpec> specLookup;
  private final FileIO io;

  private ManifestExpirationManager(Set<Long> validSnapshotIds, Map<Integer, PartitionSpec> specLookup, FileIO io) {
    this.validSnapshotIds = validSnapshotIds;
    this.specLookup = specLookup;
    this.io = io;
  }

  /**
   * Creates a Manifest Scanner for analyzing manifests for files which are no longer needed
   * after expiration.
   * @param current metadata for the table being expired
   * @param io FileIO for the table, for Serializable usecases make sure this is also serializable for the framework
   */
  public ManifestExpirationManager(TableMetadata current, FileIO io) {
    this(current.snapshots().stream().map(Snapshot::snapshotId).collect(Collectors.toSet()), current.specsById(), io);
  }

  /**
   * Scans a set of manifest files for deleted data files which do not refer to a valid snapshot and can be actually
   * removed from the filesystem. This implementation will use a concurrent approach and will ignore any
   * failed manifest reads.
   *
   * @param manifests        Manifests to scan, all of these files will be scanned
   * @return The set of all files that can be safely deleted
   */
  public Set<String> scanManifestsForAbandonedDeletedFiles(Set<ManifestFile> manifests) {
    Set<String> filesToDelete = ConcurrentHashMap.newKeySet();
    Tasks.foreach(manifests)
        .retry(3).suppressFailureWhenFinished()
        .executeWith(ThreadPools.getWorkerPool())
        .onFailure((item, exc) -> LOG.warn("Failed to get deleted files: this may cause orphaned data files", exc))
        .run(manifest -> {
          try {
            filesToDelete.addAll(scanManifestForAbandonedDeletedFiles(manifest));
          } catch (IOException e) {
            throw new UncheckedIOException(String.format("Failed to read manifest file: %s", manifest), e);
          }
        });
    return filesToDelete;
  }

  /**
   * Scans a manifest files for any deleted data files which do not refer to a valid snapshot. These files can no longer
   * contribute to Table state and removed from the filesystem.
   *
   * @param manifest         Manifest to scan
   * @return The set of all files that can be safely deleted
   */
  public Set<String> scanManifestForAbandonedDeletedFiles(ManifestFile manifest) throws IOException {
    Set<String> filesToDelete = Sets.newHashSet();
    try (ManifestReader<?> reader = ManifestFiles.open(manifest, io, specLookup)) {
      for (ManifestEntry<?> entry : reader.entries()) {
        if (entry.status() == ManifestEntry.Status.DELETED &&
            !validSnapshotIds.contains(entry.snapshotId())) {
          // use toString to ensure the path will not change (Utf8 is reused)
          filesToDelete.add(entry.file().path().toString());
        }
      }
    }
    return filesToDelete;
  }

  /**
   * Uses {@link ManifestExpirationManager#scanManifestForRevertingAddedFiles(ManifestFile)}
   * on a List of manifests. This implementation will use a concurrent approach and will ignore any
   * failed manifest reads.
   *
   * @param manifests  A list of manifests which have expired
   * @return The set of all files that can be safely deleted
   */
  public Set<String> scanManifestsForRevertingAddedFiles(Set<ManifestFile> manifests) {
    Set<String> filesToDelete = ConcurrentHashMap.newKeySet();
    Tasks.foreach(manifests)
        .retry(3).suppressFailureWhenFinished()
        .executeWith(ThreadPools.getWorkerPool())
        .onFailure((item, exc) -> LOG.warn("Failed to get deleted files: this may cause orphaned data files", exc))
        .run(manifest -> {
          try {
            filesToDelete.addAll(scanManifestForRevertingAddedFiles(manifest));
          } catch (IOException e) {
            throw new UncheckedIOException(String.format("Failed to read manifest file: %s", manifest), e);
          }
        });
    return filesToDelete;
  }

  /**
   * Find any file additions which need to be reverted from a Manifest which no long belongs to the table. Only pass
   * expired manifests to this function.
   *
   * @param manifest  A manifests which has expired
   * @return The set of all files that can be safely deleted
   */
  public Set<String> scanManifestForRevertingAddedFiles(ManifestFile manifest) throws IOException {
    Set<String> filesToDelete = Sets.newHashSet();
    try (ManifestReader<?> reader = ManifestFiles.open(manifest, io, specLookup)) {
      for (ManifestEntry<?> entry : reader.entries()) {
        if (entry.status() == ManifestEntry.Status.ADDED) {
          // use toString to ensure the path will not change (Utf8 is reused)
          filesToDelete.add(entry.file().path().toString());
        }
      }
    }
    return filesToDelete;
  }
}
