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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;

class BaseSnapshot implements Snapshot {
  private static final long INITIAL_SEQUENCE_NUMBER = 0;

  private final FileIO io;
  private final long snapshotId;
  private final Long parentId;
  private final long sequenceNumber;
  private final long timestampMillis;
  private final InputFile manifestList;
  private final String operation;
  private final Map<String, String> summary;

  // lazily initialized
  private List<ManifestFile> manifests = null;
  private List<DataFile> cachedAdds = null;
  private List<DataFile> cachedDeletes = null;

  /**
   * For testing only.
   */
  BaseSnapshot(FileIO io,
               long snapshotId,
               String... manifestFiles) {
    this(io, snapshotId, null, System.currentTimeMillis(), null, null,
        Lists.transform(Arrays.asList(manifestFiles),
            path -> new GenericManifestFile(io.newInputFile(path), 0)));
  }

  BaseSnapshot(FileIO io,
               long sequenceNumber,
               long snapshotId,
               Long parentId,
               long timestampMillis,
               String operation,
               Map<String, String> summary,
               InputFile manifestList) {
    this.io = io;
    this.sequenceNumber = sequenceNumber;
    this.snapshotId = snapshotId;
    this.parentId = parentId;
    this.timestampMillis = timestampMillis;
    this.operation = operation;
    this.summary = summary;
    this.manifestList = manifestList;
  }

  BaseSnapshot(FileIO io,
               long snapshotId,
               Long parentId,
               long timestampMillis,
               String operation,
               Map<String, String> summary,
               List<ManifestFile> manifests) {
    this(io, INITIAL_SEQUENCE_NUMBER, snapshotId, parentId, timestampMillis, operation, summary, (InputFile) null);
    this.manifests = manifests;
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
  public List<ManifestFile> manifests() {
    if (manifests == null) {
      // if manifests isn't set, then the snapshotFile is set and should be read to get the list
      this.manifests = ManifestLists.read(manifestList);
    }

    return manifests;
  }

  @Override
  public List<DataFile> addedFiles() {
    if (cachedAdds == null) {
      cacheChanges();
    }
    return cachedAdds;
  }

  @Override
  public List<DataFile> deletedFiles() {
    if (cachedDeletes == null) {
      cacheChanges();
    }
    return cachedDeletes;
  }

  @Override
  public String manifestListLocation() {
    return manifestList != null ? manifestList.location() : null;
  }

  private void cacheChanges() {
    ImmutableList.Builder<DataFile> adds = ImmutableList.builder();
    ImmutableList.Builder<DataFile> deletes = ImmutableList.builder();

    // read only manifests that were created by this snapshot
    Iterable<ManifestFile> changedManifests = Iterables.filter(manifests(),
        manifest -> Objects.equal(manifest.snapshotId(), snapshotId));
    try (CloseableIterable<ManifestEntry> entries = new ManifestGroup(io, changedManifests)
        .ignoreExisting()
        .entries()) {
      for (ManifestEntry entry : entries) {
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
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", snapshotId)
        .add("timestamp_ms", timestampMillis)
        .add("operation", operation)
        .add("summary", summary)
        .add("manifests", manifests())
        .toString();
  }
}
