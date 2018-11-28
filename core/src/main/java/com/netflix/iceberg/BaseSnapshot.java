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
import com.google.common.collect.Lists;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.io.CloseableGroup;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

class BaseSnapshot extends CloseableGroup implements Snapshot {
  private final TableOperations ops;
  private final long snapshotId;
  private final Long parentId;
  private final long timestampMillis;
  private final List<String> manifestFiles;

  // lazily initialized
  private List<DataFile> adds = null;
  private List<DataFile> deletes = null;

  /**
   * For testing only.
   */
  BaseSnapshot(TableOperations ops,
               long snapshotId,
               String... manifestFiles) {
    this(ops, snapshotId, null, System.currentTimeMillis(), Arrays.asList(manifestFiles));
  }

  BaseSnapshot(TableOperations ops,
               long snapshotId,
               Long parentId,
               long timestampMillis,
               List<String> manifestFiles) {
    this.ops = ops;
    this.snapshotId = snapshotId;
    this.parentId = parentId;
    this.timestampMillis = timestampMillis;
    this.manifestFiles = manifestFiles;
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
  public List<String> manifests() {
    return manifestFiles;
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

  private void cacheChanges() {
    List<DataFile> adds = Lists.newArrayList();
    List<DataFile> deletes = Lists.newArrayList();

    // accumulate adds and deletes from all manifests.
    // because manifests can be reused in newer snapshots, filter the changes by snapshot id.
    for (String manifest : manifestFiles) {
      try (ManifestReader reader = ManifestReader.read(ops.newInputFile(manifest))) {
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
        .add("manifests", manifestFiles)
        .toString();
  }
}
