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
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;

class BaseSnapshot implements Snapshot {
  private final TableOperations ops;
  private final long snapshotId;
  private final Long parentId;
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
  BaseSnapshot(TableOperations ops,
               long snapshotId,
               String... manifestFiles) {
    this(ops, snapshotId, null, System.currentTimeMillis(), null, null,
        Lists.transform(Arrays.asList(manifestFiles),
            path -> new GenericManifestFile(ops.io().newInputFile(path), 0)));
  }

  BaseSnapshot(TableOperations ops,
               long snapshotId,
               Long parentId,
               long timestampMillis,
               String operation,
               Map<String, String> summary,
               InputFile manifestList) {
    this.ops = ops;
    this.snapshotId = snapshotId;
    this.parentId = parentId;
    this.timestampMillis = timestampMillis;
    this.operation = operation;
    this.summary = summary;
    this.manifestList = manifestList;
  }

  BaseSnapshot(TableOperations ops,
               long snapshotId,
               Long parentId,
               long timestampMillis,
               String operation,
               Map<String, String> summary,
               List<ManifestFile> manifests) {
    this(ops, snapshotId, parentId, timestampMillis, operation, summary, (InputFile) null);
    this.manifests = manifests;
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
      try (CloseableIterable<ManifestFile> files = Avro.read(manifestList)
          .rename("manifest_file", GenericManifestFile.class.getName())
          .rename("partitions", GenericPartitionFieldSummary.class.getName())
          .rename("r508", GenericPartitionFieldSummary.class.getName())
          .project(ManifestFile.schema())
          .reuseContainers(false)
          .build()) {

        this.manifests = Lists.newLinkedList(files);

      } catch (IOException e) {
        throw new RuntimeIOException(e, "Cannot read snapshot file: %s", manifestList.location());
      }
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
    try (CloseableIterable<ManifestEntry> entries = new ManifestGroup(ops, changedManifests)
        .ignoreExisting()
        .select(ManifestReader.CHANGE_WITH_STATS_COLUMNS)
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
