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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class BaseSnapshot implements Snapshot {
  private final long snapshotId;
  private final Long parentId;
  private final long sequenceNumber;
  private final long timestampMillis;
  private final String manifestListLocation;
  private final String operation;
  private final Map<String, String> summary;
  private final Integer schemaId;
  private final String[] v1ManifestLocations;

  // lazily initialized
  private transient List<ManifestFile> allManifests = null;
  private transient List<ManifestFile> dataManifests = null;
  private transient List<ManifestFile> deleteManifests = null;
  private transient List<DataFile> addedDataFiles = null;
  private transient List<DataFile> removedDataFiles = null;
  private transient List<DeleteFile> addedDeleteFiles = null;
  private transient List<DeleteFile> removedDeleteFiles = null;

  BaseSnapshot(
      long sequenceNumber,
      long snapshotId,
      Long parentId,
      long timestampMillis,
      String operation,
      Map<String, String> summary,
      Integer schemaId,
      String manifestList) {
    this.sequenceNumber = sequenceNumber;
    this.snapshotId = snapshotId;
    this.parentId = parentId;
    this.timestampMillis = timestampMillis;
    this.operation = operation;
    this.summary = summary;
    this.schemaId = schemaId;
    this.manifestListLocation = manifestList;
    this.v1ManifestLocations = null;
  }

  BaseSnapshot(
      long sequenceNumber,
      long snapshotId,
      Long parentId,
      long timestampMillis,
      String operation,
      Map<String, String> summary,
      Integer schemaId,
      String[] v1ManifestLocations) {
    this.sequenceNumber = sequenceNumber;
    this.snapshotId = snapshotId;
    this.parentId = parentId;
    this.timestampMillis = timestampMillis;
    this.operation = operation;
    this.summary = summary;
    this.schemaId = schemaId;
    this.manifestListLocation = null;
    this.v1ManifestLocations = v1ManifestLocations;
  }

  @Override
  public long sequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public long snapshotId() {
    return snapshotId;
  }

  @Override
  public Long parentId() {
    return parentId;
  }

  @Override
  public long timestampMillis() {
    return timestampMillis;
  }

  @Override
  public String operation() {
    return operation;
  }

  @Override
  public Map<String, String> summary() {
    return summary;
  }

  @Override
  public Integer schemaId() {
    return schemaId;
  }

  private void cacheManifests(FileIO fileIO) {
    if (fileIO == null) {
      throw new IllegalArgumentException("Cannot cache changes: FileIO is null");
    }

    if (allManifests == null && v1ManifestLocations != null) {
      // if we have a collection of manifest locations, then we need to load them here
      allManifests =
          Lists.transform(
              Arrays.asList(v1ManifestLocations),
              location -> new GenericManifestFile(fileIO.newInputFile(location), 0));
    }

    if (allManifests == null) {
      // if manifests isn't set, then the snapshotFile is set and should be read to get the list
      this.allManifests = ManifestLists.read(fileIO.newInputFile(manifestListLocation));
    }

    if (dataManifests == null || deleteManifests == null) {
      this.dataManifests =
          ImmutableList.copyOf(
              Iterables.filter(
                  allManifests, manifest -> manifest.content() == ManifestContent.DATA));
      this.deleteManifests =
          ImmutableList.copyOf(
              Iterables.filter(
                  allManifests, manifest -> manifest.content() == ManifestContent.DELETES));
    }
  }

  @Override
  public List<ManifestFile> allManifests(FileIO fileIO) {
    if (allManifests == null) {
      cacheManifests(fileIO);
    }
    return allManifests;
  }

  @Override
  public List<ManifestFile> dataManifests(FileIO fileIO) {
    if (dataManifests == null) {
      cacheManifests(fileIO);
    }
    return dataManifests;
  }

  @Override
  public List<ManifestFile> deleteManifests(FileIO fileIO) {
    if (deleteManifests == null) {
      cacheManifests(fileIO);
    }
    return deleteManifests;
  }

  @Override
  public List<DataFile> addedDataFiles(FileIO fileIO) {
    if (addedDataFiles == null) {
      cacheDataFileChanges(fileIO);
    }
    return addedDataFiles;
  }

  @Override
  public List<DataFile> removedDataFiles(FileIO fileIO) {
    if (removedDataFiles == null) {
      cacheDataFileChanges(fileIO);
    }
    return removedDataFiles;
  }

  @Override
  public Iterable<DeleteFile> addedDeleteFiles(FileIO fileIO) {
    if (addedDeleteFiles == null) {
      cacheDeleteFileChanges(fileIO);
    }
    return addedDeleteFiles;
  }

  @Override
  public Iterable<DeleteFile> removedDeleteFiles(FileIO fileIO) {
    if (removedDeleteFiles == null) {
      cacheDeleteFileChanges(fileIO);
    }
    return removedDeleteFiles;
  }

  @Override
  public String manifestListLocation() {
    return manifestListLocation;
  }

  private void cacheDeleteFileChanges(FileIO fileIO) {
    Preconditions.checkArgument(fileIO != null, "Cannot cache delete file changes: FileIO is null");

    ImmutableList.Builder<DeleteFile> adds = ImmutableList.builder();
    ImmutableList.Builder<DeleteFile> deletes = ImmutableList.builder();

    Iterable<ManifestFile> changedManifests =
        Iterables.filter(
            deleteManifests(fileIO), manifest -> Objects.equal(manifest.snapshotId(), snapshotId));

    for (ManifestFile manifest : changedManifests) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, fileIO, null)) {
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
        throw new UncheckedIOException("Failed to close manifest reader", e);
      }
    }

    this.addedDeleteFiles = adds.build();
    this.removedDeleteFiles = deletes.build();
  }

  private void cacheDataFileChanges(FileIO fileIO) {
    Preconditions.checkArgument(fileIO != null, "Cannot cache data file changes: FileIO is null");

    ImmutableList.Builder<DataFile> adds = ImmutableList.builder();
    ImmutableList.Builder<DataFile> deletes = ImmutableList.builder();

    // read only manifests that were created by this snapshot
    Iterable<ManifestFile> changedManifests =
        Iterables.filter(
            dataManifests(fileIO), manifest -> Objects.equal(manifest.snapshotId(), snapshotId));
    try (CloseableIterable<ManifestEntry<DataFile>> entries =
        new ManifestGroup(fileIO, changedManifests).ignoreExisting().entries()) {
      for (ManifestEntry<DataFile> entry : entries) {
        switch (entry.status()) {
          case ADDED:
            adds.add(entry.file().copy());
            break;
          case DELETED:
            deletes.add(entry.file().copyWithoutStats());
            break;
          default:
            throw new IllegalStateException(
                "Unexpected entry status, not added or deleted: " + entry);
        }
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close entries while caching changes");
    }

    this.addedDataFiles = adds.build();
    this.removedDataFiles = deletes.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o instanceof BaseSnapshot) {
      BaseSnapshot other = (BaseSnapshot) o;
      return this.snapshotId == other.snapshotId()
          && Objects.equal(this.parentId, other.parentId())
          && this.sequenceNumber == other.sequenceNumber()
          && this.timestampMillis == other.timestampMillis()
          && Objects.equal(this.schemaId, other.schemaId());
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        this.snapshotId, this.parentId, this.sequenceNumber, this.timestampMillis, this.schemaId);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", snapshotId)
        .add("timestamp_ms", timestampMillis)
        .add("operation", operation)
        .add("summary", summary)
        .add("manifest-list", manifestListLocation)
        .add("schema-id", schemaId)
        .toString();
  }
}
