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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.OutputFile;

/**
 * {@link AppendFiles Append} implementation that adds a new manifest file for the write.
 * <p>
 * This implementation will attempt to commit 5 times before throwing {@link CommitFailedException}.
 */
class FastAppend extends SnapshotProducer<AppendFiles> implements AppendFiles {
  private final TableOperations ops;
  private final PartitionSpec spec;
  private final SnapshotSummary.Builder summaryBuilder = SnapshotSummary.builder();
  private final List<DataFile> newFiles = Lists.newArrayList();
  private final List<ManifestFile> appendManifests = Lists.newArrayList();
  private ManifestFile newManifest = null;
  private final AtomicInteger manifestCount = new AtomicInteger(0);
  private boolean hasNewFiles = false;

  FastAppend(TableOperations ops) {
    super(ops);
    this.ops = ops;
    this.spec = ops.current().spec();
  }

  @Override
  protected AppendFiles self() {
    return this;
  }

  @Override
  public AppendFiles set(String property, String value) {
    summaryBuilder.set(property, value);
    return this;
  }

  @Override
  protected String operation() {
    return DataOperations.APPEND;
  }

  @Override
  protected Map<String, String> summary() {
    return summaryBuilder.build();
  }

  @Override
  public FastAppend appendFile(DataFile file) {
    this.hasNewFiles = true;
    newFiles.add(file);
    summaryBuilder.addedFile(spec, file);
    return this;
  }

  @Override
  public FastAppend appendManifest(ManifestFile manifest) {
    // the manifest must be rewritten with this update's snapshot ID
    try (ManifestReader reader = ManifestReader.read(
        ops.io().newInputFile(manifest.path()), ops.current()::spec)) {
      OutputFile newManifestPath = manifestPath(manifestCount.getAndIncrement());
      appendManifests.add(ManifestWriter.copyAppendManifest(reader, newManifestPath, snapshotId(), summaryBuilder));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close manifest: %s", manifest);
    }

    return this;
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base) {
    List<ManifestFile> newManifests = Lists.newArrayList();

    try {
      ManifestFile manifest = writeManifest();
      if (manifest != null) {
        newManifests.add(manifest);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write manifest");
    }

    newManifests.addAll(appendManifests);

    if (base.currentSnapshot() != null) {
      newManifests.addAll(base.currentSnapshot().manifests());
    }

    return newManifests;
  }

  @Override
  protected void cleanUncommitted(Set<ManifestFile> committed) {
    if (newManifest != null && !committed.contains(newManifest)) {
      deleteFile(newManifest.path());
    }

    for (ManifestFile manifest : appendManifests) {
      if (!committed.contains(manifest)) {
        deleteFile(manifest.path());
      }
    }
  }

  private ManifestFile writeManifest() throws IOException {
    if (hasNewFiles && newManifest != null) {
      deleteFile(newManifest.path());
      newManifest = null;
    }

    if (newManifest == null && newFiles.size() > 0) {
      OutputFile out = manifestPath(manifestCount.getAndIncrement());

      ManifestWriter writer = new ManifestWriter(spec, out, snapshotId());
      try {
        writer.addAll(newFiles);
      } finally {
        writer.close();
      }

      this.newManifest = writer.toManifestFile();
      hasNewFiles = false;
    }

    return newManifest;
  }
}
