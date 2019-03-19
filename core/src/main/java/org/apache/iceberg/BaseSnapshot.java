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

package com.netflix.iceberg;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.netflix.iceberg.avro.Avro;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.io.CloseableIterable;
import com.netflix.iceberg.io.InputFile;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
  private List<DataFile> adds = null;
  private List<DataFile> deletes = null;

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
    if (adds == null) {
      cacheChanges();
    }
    return adds;
  }

  @Override
  public List<DataFile> deletedFiles() {
    if (deletes == null) {
      cacheChanges();
    }
    return deletes;
  }

  @Override
  public String manifestListLocation() {
    return manifestList != null ? manifestList.location() : null;
  }

  private void cacheChanges() {
    List<DataFile> adds = Lists.newArrayList();
    List<DataFile> deletes = Lists.newArrayList();

    // accumulate adds and deletes from all manifests.
    // because manifests can be reused in newer snapshots, filter the changes by snapshot id.
    for (String manifest : Iterables.transform(manifests(), ManifestFile::path)) {
      try (ManifestReader reader = ManifestReader.read(ops.io().newInputFile(manifest))) {
        for (ManifestEntry add : reader.addedFiles()) {
          if (add.snapshotId() == snapshotId) {
            adds.add(add.file().copy());
          }
        }
        for (ManifestEntry delete : reader.deletedFiles()) {
          if (delete.snapshotId() == snapshotId) {
            deletes.add(delete.file().copy());
          }
        }
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to close reader while caching changes");
      }
    }

    this.adds = adds;
    this.deletes = deletes;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("id", snapshotId)
        .add("timestamp_ms", timestampMillis)
        .add("operation", operation)
        .add("summary", summary)
        .add("manifests", manifests())
        .toString();
  }
}
