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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.DataFileSet;

/** {@link AppendFiles Append} implementation that adds new manifest files for writes. */
class FastAppend extends SnapshotProducer<AppendFiles> implements AppendFiles {
  private final String tableName;
  private final SnapshotSummary.Builder summaryBuilder = SnapshotSummary.builder();
  private final Map<Integer, DataFileSet> newDataFilesBySpec = Maps.newHashMap();
  private final List<ManifestFile> appendManifests = Lists.newArrayList();
  private final List<ManifestFile> rewrittenAppendManifests = Lists.newArrayList();
  private final List<ManifestFile> newManifests = Lists.newArrayList();
  private boolean hasNewFiles = false;

  FastAppend(String tableName, TableOperations ops) {
    super(ops);
    this.tableName = tableName;
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
    summaryBuilder.setPartitionSummaryLimit(
        ops()
            .current()
            .propertyAsInt(
                TableProperties.WRITE_PARTITION_SUMMARY_LIMIT,
                TableProperties.WRITE_PARTITION_SUMMARY_LIMIT_DEFAULT));
    return summaryBuilder.build();
  }

  @Override
  public FastAppend appendFile(DataFile file) {
    Preconditions.checkNotNull(file, "Invalid data file: null");
    PartitionSpec spec = spec(file.specId());
    Preconditions.checkArgument(
        spec != null,
        "Cannot find partition spec %s for data file: %s",
        file.specId(),
        file.location());

    DataFileSet dataFiles =
        newDataFilesBySpec.computeIfAbsent(spec.specId(), ignored -> DataFileSet.create());

    if (dataFiles.add(file)) {
      this.hasNewFiles = true;
      summaryBuilder.addedFile(spec, file);
    }

    return this;
  }

  private PartitionSpec spec(int specId) {
    return ops().current().spec(specId);
  }

  @Override
  public FastAppend toBranch(String branch) {
    targetBranch(branch);
    return this;
  }

  @Override
  public FastAppend appendManifest(ManifestFile manifest) {
    Preconditions.checkArgument(
        !manifest.hasExistingFiles(), "Cannot append manifest with existing files");
    Preconditions.checkArgument(
        !manifest.hasDeletedFiles(), "Cannot append manifest with deleted files");
    Preconditions.checkArgument(
        manifest.snapshotId() == null || manifest.snapshotId() == -1,
        "Snapshot id must be assigned during commit");
    Preconditions.checkArgument(
        manifest.sequenceNumber() == -1, "Sequence number must be assigned during commit");

    if (canInheritSnapshotId() && manifest.snapshotId() == null) {
      summaryBuilder.addedManifest(manifest);
      appendManifests.add(manifest);
    } else {
      // the manifest must be rewritten with this update's snapshot ID
      ManifestFile copiedManifest = copyManifest(manifest);
      rewrittenAppendManifests.add(copiedManifest);
    }

    return this;
  }

  private ManifestFile copyManifest(ManifestFile manifest) {
    TableMetadata current = ops().current();
    InputFile toCopy = ops().io().newInputFile(manifest);
    EncryptedOutputFile newManifestFile = newManifestOutputFile();
    return ManifestFiles.copyAppendManifest(
        current.formatVersion(),
        manifest.partitionSpecId(),
        toCopy,
        current.specsById(),
        newManifestFile,
        snapshotId(),
        summaryBuilder,
        current.properties());
  }

  @Override
  public List<ManifestFile> apply(TableMetadata base, Snapshot snapshot) {
    List<ManifestFile> manifests = Lists.newArrayList();

    try {
      List<ManifestFile> newWrittenManifests = writeNewManifests();
      if (newWrittenManifests != null) {
        manifests.addAll(newWrittenManifests);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write manifest");
    }

    Iterable<ManifestFile> appendManifestsWithMetadata =
        Iterables.transform(
            Iterables.concat(appendManifests, rewrittenAppendManifests),
            manifest -> GenericManifestFile.copyOf(manifest).withSnapshotId(snapshotId()).build());
    Iterables.addAll(manifests, appendManifestsWithMetadata);

    if (snapshot != null) {
      manifests.addAll(snapshot.allManifests(ops().io()));
    }

    return manifests;
  }

  @Override
  public Object updateEvent() {
    long snapshotId = snapshotId();
    Snapshot snapshot = ops().current().snapshot(snapshotId);
    long sequenceNumber = snapshot.sequenceNumber();
    return new CreateSnapshotEvent(
        tableName, operation(), snapshotId, sequenceNumber, snapshot.summary());
  }

  @Override
  protected void cleanUncommitted(Set<ManifestFile> committed) {
    if (newManifests != null) {
      boolean hasDeletes = false;
      for (ManifestFile manifest : newManifests) {
        if (!committed.contains(manifest)) {
          deleteFile(manifest.path());
          hasDeletes = true;
        }
      }
      if (hasDeletes) {
        this.newManifests.clear();
      }
    }

    // clean up only rewrittenAppendManifests as they are always owned by the table
    // don't clean up appendManifests as they are added to the manifest list and are not compacted
    for (ManifestFile manifest : rewrittenAppendManifests) {
      if (!committed.contains(manifest)) {
        deleteFile(manifest.path());
      }
    }
  }

  /**
   * Cleanup after committing is disabled for FastAppend unless there are rewrittenAppendManifests
   * because: 1.) Appended manifests are never rewritten 2.) Manifests which are written out as part
   * of appendFile are already cleaned up between commit attempts in writeNewManifests
   */
  @Override
  protected boolean cleanupAfterCommit() {
    return !rewrittenAppendManifests.isEmpty();
  }

  private List<ManifestFile> writeNewManifests() throws IOException {
    if (hasNewFiles && !newManifests.isEmpty()) {
      newManifests.forEach(file -> deleteFile(file.path()));
      newManifests.clear();
    }

    if (newManifests.isEmpty() && !newDataFilesBySpec.isEmpty()) {
      newDataFilesBySpec.forEach(
          (specId, dataFiles) -> newManifests.addAll(writeDataManifests(dataFiles, spec(specId))));
      hasNewFiles = false;
    }

    return newManifests;
  }
}
