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
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

/**
 * A utility class to work around the current Snapshot interface and load cahnges from V4 Manfiests
 * - needs to be discussed and changed
 */
public class SnapshotChanges {
  private final List<DataFile> addedDataFiles;
  private final List<DataFile> removedDataFiles;
  private final List<DeleteFile> addedDeleteFiles;
  private final List<DeleteFile> removedDeleteFiles;

  private SnapshotChanges(
      List<DataFile> addedDataFiles,
      List<DataFile> removedDataFiles,
      List<DeleteFile> addedDeleteFiles,
      List<DeleteFile> removedDeleteFiles) {
    this.addedDataFiles = addedDataFiles;
    this.removedDataFiles = removedDataFiles;
    this.addedDeleteFiles = addedDeleteFiles;
    this.removedDeleteFiles = removedDeleteFiles;
  }

  public List<DataFile> addedDataFiles() {
    return addedDataFiles;
  }

  public List<DataFile> removedDataFiles() {
    return removedDataFiles;
  }

  public List<DeleteFile> addedDeleteFiles() {
    return addedDeleteFiles;
  }

  public List<DeleteFile> removedDeleteFiles() {
    return removedDeleteFiles;
  }

  public static SnapshotChanges changesFrom(
      Snapshot snapshot, FileIO io, Map<Integer, PartitionSpec> specsById) {
    // Data File Changes
    ImmutableList.Builder<DataFile> addedDataFileBuilder = ImmutableList.builder();
    ImmutableList.Builder<DataFile> removedDataFileBuilder = ImmutableList.builder();

    // read only manifests that were created by this snapshot
    Iterable<ManifestFile> changedDataManifests =
        Iterables.filter(
            snapshot.dataManifests(io),
            manifest -> Objects.equal(manifest.snapshotId(), snapshot.snapshotId()));
    try (CloseableIterable<ManifestEntry<DataFile>> entries =
        new ManifestGroup(io, changedDataManifests)
            .specsById(specsById)
            .ignoreExisting()
            .entries()) {
      for (ManifestEntry<DataFile> entry : entries) {
        switch (entry.status()) {
          case ADDED:
            addedDataFileBuilder.add(entry.file().copy());
            break;
          case DELETED:
            removedDataFileBuilder.add(entry.file().copyWithoutStats());
            break;
          default:
            throw new IllegalStateException(
                "Unexpected entry status, not added or deleted: " + entry);
        }
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close entries while caching changes");
    }

    // Delete File Changes
    ImmutableList.Builder<DeleteFile> addedDeleteFilesBuilder = ImmutableList.builder();
    ImmutableList.Builder<DeleteFile> removedDeleteFilesBuilder = ImmutableList.builder();

    Iterable<ManifestFile> changedDeleteManifests =
        Iterables.filter(
            snapshot.deleteManifests(io),
            manifest -> Objects.equal(manifest.snapshotId(), snapshot.snapshotId()));

    for (ManifestFile manifest : changedDeleteManifests) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, io, specsById)) {
        for (ManifestEntry<DeleteFile> entry : reader.entries()) {
          switch (entry.status()) {
            case ADDED:
              addedDeleteFilesBuilder.add(entry.file().copy());
              break;
            case DELETED:
              removedDeleteFilesBuilder.add(entry.file().copyWithoutStats());
              break;
            default:
              // ignore existing
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close manifest reader", e);
      }
    }

    return new SnapshotChanges(
        addedDataFileBuilder.build(),
        removedDataFileBuilder.build(),
        addedDeleteFilesBuilder.build(),
        removedDeleteFilesBuilder.build());
  }
}
