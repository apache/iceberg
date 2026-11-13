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
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

class V4SnapshotChanges implements SnapshotFileChanges {
  private final CloseableIterable<TrackedFile> entries;
  private final Map<Integer, PartitionSpec> specsById;
  private List<DataFile> addedDataFiles = null;
  private List<DataFile> removedDataFiles = null;
  private List<DataFile> modifiedDataFiles = null;
  private List<DataFile> replacedDataFiles = null;
  private RuntimeException loadFailure = null;

  /** Takes ownership of {@code entries} and closes it after the first load attempt. */
  V4SnapshotChanges(CloseableIterable<TrackedFile> entries, Map<Integer, PartitionSpec> specsById) {
    this.entries = entries;
    this.specsById = specsById;
  }

  @Override
  public Iterable<DataFile> addedDataFiles() {
    loadChanges();
    return addedDataFiles;
  }

  @Override
  public Iterable<DataFile> removedDataFiles() {
    loadChanges();
    return removedDataFiles;
  }

  @Override
  public Iterable<DataFile> modifiedDataFiles() {
    loadChanges();
    return modifiedDataFiles;
  }

  @Override
  public Iterable<DataFile> replacedDataFiles() {
    loadChanges();
    return replacedDataFiles;
  }

  @Override
  public Iterable<DeleteFile> addedDeleteFiles() {
    loadChanges();
    return ImmutableList.of();
  }

  @Override
  public Iterable<DeleteFile> removedDeleteFiles() {
    loadChanges();
    return ImmutableList.of();
  }

  private void loadChanges() {
    if (loadFailure != null) {
      throw loadFailure;
    }

    if (addedDataFiles != null) {
      return;
    }

    ImmutableList.Builder<DataFile> addedFiles = ImmutableList.builder();
    ImmutableList.Builder<DataFile> removedFiles = ImmutableList.builder();
    ImmutableList.Builder<DataFile> modifiedFiles = ImmutableList.builder();
    ImmutableList.Builder<DataFile> replacedFiles = ImmutableList.builder();
    try (CloseableIterable<TrackedFile> changes = entries) {
      for (TrackedFile file : changes) {
        ImmutableList.Builder<DataFile> filesForStatus =
            switch (file.tracking().status()) {
              case ADDED -> addedFiles;
              case DELETED -> removedFiles;
              case MODIFIED -> modifiedFiles;
              case REPLACED -> replacedFiles;
              case EXISTING ->
                  throw new IllegalArgumentException(
                      "Unexpected unchanged file in snapshot changes: " + file.location());
            };
        filesForStatus.add(TrackedFileAdapters.asDataFile(file, specsById));
      }
    } catch (IOException e) {
      this.loadFailure = new UncheckedIOException("Failed to close snapshot changes", e);
      throw loadFailure;
    } catch (RuntimeException e) {
      this.loadFailure = e;
      throw e;
    }

    this.addedDataFiles = addedFiles.build();
    this.removedDataFiles = removedFiles.build();
    this.modifiedDataFiles = modifiedFiles.build();
    this.replacedDataFiles = replacedFiles.build();
  }
}
