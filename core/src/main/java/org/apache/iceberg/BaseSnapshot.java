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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class BaseSnapshot implements Snapshot {
  private static final long INITIAL_SEQUENCE_NUMBER = 0;

  /**
   * @deprecated since 1.0.0 - {@link FileIO} should be passed to the methods that require them
   */
  @Deprecated
  private final FileIO io;

  private final long snapshotId;
  private final Long parentId;
  private final long sequenceNumber;
  private final long timestampMillis;
  private final String manifestListLocation;
  private final String operation;
  private final Map<String, String> summary;
  private final Integer schemaId;

  // lazily initialized
  private transient List<ManifestFile> allManifests = null;
  private transient List<ManifestFile> dataManifests = null;
  private transient List<ManifestFile> deleteManifests = null;
  private transient List<DataFile> cachedAdds = null;
  private transient List<DataFile> cachedDeletes = null;

  /**
   * For testing only.
   */
  BaseSnapshot(FileIO io,
               long snapshotId,
               Integer schemaId,
               String... manifestFiles) {
    this(io, snapshotId, null, System.currentTimeMillis(), null, null,
        schemaId, Lists.transform(Arrays.asList(manifestFiles),
            path -> new GenericManifestFile(io.newInputFile(path), 0)));
  }

  BaseSnapshot(FileIO io,
               long sequenceNumber,
               long snapshotId,
               Long parentId,
               long timestampMillis,
               String operation,
               Map<String, String> summary,
               Integer schemaId,
               String manifestList) {
    this.io = io;
    this.sequenceNumber = sequenceNumber;
    this.snapshotId = snapshotId;
    this.parentId = parentId;
    this.timestampMillis = timestampMillis;
    this.operation = operation;
    this.summary = summary;
    this.schemaId = schemaId;
    this.manifestListLocation = manifestList;
  }

  BaseSnapshot(long sequenceNumber,
               long snapshotId,
               Long parentId,
               long timestampMillis,
               String operation,
               Map<String, String> summary,
               Integer schemaId,
               String manifestList) {
    this.io = null;
    this.sequenceNumber = sequenceNumber;
    this.snapshotId = snapshotId;
    this.parentId = parentId;
    this.timestampMillis = timestampMillis;
    this.operation = operation;
    this.summary = summary;
    this.schemaId = schemaId;
    this.manifestListLocation = manifestList;
  }

  BaseSnapshot(FileIO io,
               long snapshotId,
               Long parentId,
               long timestampMillis,
               String operation,
               Map<String, String> summary,
               Integer schemaId,
               List<ManifestFile> dataManifests) {
    this(io, INITIAL_SEQUENCE_NUMBER, snapshotId, parentId, timestampMillis, operation, summary, schemaId, null);
    this.allManifests = dataManifests;
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
      throw new IllegalStateException("Cannot cache changes: file io is null");
    }

    if (allManifests == null) {
      // if manifests isn't set, then the snapshotFile is set and should be read to get the list
      this.allManifests = ManifestLists.read(fileIO.newInputFile(manifestListLocation));
    }

    if (dataManifests == null || deleteManifests == null) {
      this.dataManifests = ImmutableList.copyOf(Iterables.filter(allManifests,
          manifest -> manifest.content() == ManifestContent.DATA));
      this.deleteManifests = ImmutableList.copyOf(Iterables.filter(allManifests,
          manifest -> manifest.content() == ManifestContent.DELETES));
    }
  }

  @Override
  public List<ManifestFile> allManifests(FileIO fileIO) {
    if (allManifests == null) {
      cacheManifests(fileIO);
    }
    return allManifests;
  }

  /**
   * @deprecated since 1.0.0 - Use {@link Snapshot#allManifests(FileIO)} instead.
   */
  @Override
  @Deprecated
  public List<ManifestFile> allManifests() {
    if (allManifests == null) {
      cacheManifests(io);
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


  /**
   * @deprecated since 1.0.0 - Use {@link Snapshot#dataManifests(FileIO)} instead.
   */
  @Override
  @Deprecated
  public List<ManifestFile> dataManifests() {
    if (dataManifests == null) {
      cacheManifests(io);
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

  /**
   * @deprecated since 1.0.0 - Use {@link Snapshot#deleteManifests(FileIO)} instead.
   */
  @Override
  @Deprecated
  public List<ManifestFile> deleteManifests() {
    if (deleteManifests == null) {
      cacheManifests(io);
    }
    return deleteManifests;
  }

  @Override
  public List<DataFile> addedFiles(FileIO fileIO) {
    if (cachedAdds == null) {
      cacheChanges(fileIO);
    }
    return cachedAdds;
  }

  /**
   * @deprecated since 1.0.0 - Use {@link Snapshot#addedFiles(FileIO)} instead.
   */
  @Override
  @Deprecated
  public List<DataFile> addedFiles() {
    if (cachedAdds == null) {
      cacheChanges(io);
    }
    return cachedAdds;
  }

  @Override
  public List<DataFile> deletedFiles(FileIO fileIO) {
    if (cachedDeletes == null) {
      cacheChanges(fileIO);
    }
    return cachedDeletes;
  }

  /**
   * @deprecated since 1.0.0 - Use {@link Snapshot#deletedFiles(FileIO)} instead.
   */
  @Override
  @Deprecated
  public List<DataFile> deletedFiles() {
    if (cachedDeletes == null) {
      cacheChanges(io);
    }
    return cachedDeletes;
  }

  @Override
  public String manifestListLocation() {
    return manifestListLocation;
  }

  private void cacheChanges(FileIO fileIO) {
    if (fileIO == null) {
      throw new IllegalStateException("Cannot cache changes: file io is null");
    }

    ImmutableList.Builder<DataFile> adds = ImmutableList.builder();
    ImmutableList.Builder<DataFile> deletes = ImmutableList.builder();

    // read only manifests that were created by this snapshot
    Iterable<ManifestFile> changedManifests = Iterables.filter(dataManifests(fileIO),
        manifest -> Objects.equal(manifest.snapshotId(), snapshotId));
    try (CloseableIterable<ManifestEntry<DataFile>> entries = new ManifestGroup(fileIO, changedManifests)
        .ignoreExisting()
        .entries()) {
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

    this.cachedAdds = adds.build();
    this.cachedDeletes = deletes.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o instanceof BaseSnapshot) {
      BaseSnapshot other = (BaseSnapshot) o;
      return this.snapshotId == other.snapshotId() &&
          Objects.equal(this.parentId, other.parentId()) &&
          this.sequenceNumber == other.sequenceNumber() &&
          this.timestampMillis == other.timestampMillis() &&
          Objects.equal(this.schemaId, other.schemaId());
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
      this.snapshotId,
      this.parentId,
      this.sequenceNumber,
      this.timestampMillis,
      this.schemaId
    );
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
