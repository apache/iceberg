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
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.ParallelIterable;
import org.apache.iceberg.util.ThreadPools;

class V4ChangelogPlanner {
  private final FileIO io;
  private final ManifestFile root;
  private final Schema tableSchema;
  private final Map<Integer, PartitionSpec> specsById;

  private String tableLocation = null;
  private Expression dataFilter = Expressions.alwaysTrue();
  private int[] statsFieldIds = null;
  private boolean caseSensitive = true;
  private ExecutorService executorService = ThreadPools.getWorkerPool();

  V4ChangelogPlanner(
      FileIO io, ManifestFile root, Schema tableSchema, Map<Integer, PartitionSpec> specsById) {
    Preconditions.checkArgument(io != null, "Invalid file IO: null");
    Preconditions.checkArgument(root != null, "Invalid root manifest: null");
    Preconditions.checkArgument(tableSchema != null, "Invalid table schema: null");
    Preconditions.checkArgument(specsById != null, "Invalid specs by ID: null");
    this.io = io;
    this.root = root;
    this.tableSchema = tableSchema;
    this.specsById = ImmutableMap.copyOf(specsById);
  }

  V4ChangelogPlanner tableLocation(String newTableLocation) {
    Preconditions.checkArgument(newTableLocation != null, "Invalid table location: null");
    this.tableLocation = newTableLocation;
    return this;
  }

  V4ChangelogPlanner filterData(Expression expr) {
    Preconditions.checkArgument(expr != null, "Invalid filter: null");
    this.dataFilter = expr;
    return this;
  }

  V4ChangelogPlanner projectStats(int... fieldIds) {
    Preconditions.checkArgument(fieldIds != null, "Invalid field IDs: null");
    this.statsFieldIds = Arrays.copyOf(fieldIds, fieldIds.length);
    return this;
  }

  V4ChangelogPlanner caseSensitive(boolean newCaseSensitive) {
    this.caseSensitive = newCaseSensitive;
    return this;
  }

  V4ChangelogPlanner planWith(ExecutorService newExecutorService) {
    this.executorService = newExecutorService;
    return this;
  }

  SnapshotFileChanges planChanges() {
    return new V4SnapshotChanges(planEntries(), specsById);
  }

  private CloseableIterable<TrackedFile> planEntries() {
    List<TrackedFile> rootDataFiles = Lists.newArrayList();
    List<ManifestFile> leafManifests = Lists.newArrayList();

    try (CloseableIterable<TrackedFile> rootChanges = changesReader(root)) {
      for (TrackedFile entry : rootChanges) {
        switch (entry.contentType()) {
          case DATA -> rootDataFiles.add(entry);
          case DATA_MANIFEST -> {
            EntryStatus status = entry.tracking().status();
            if (status == EntryStatus.ADDED || status == EntryStatus.MODIFIED) {
              leafManifests.add(TrackedFileAdapters.asManifestFile(entry));
            }
          }
          case DELETE_MANIFEST ->
              throw new UnsupportedOperationException(
                  "Delete manifests are not supported: " + entry.location());
          default ->
              throw new UnsupportedOperationException(
                  "Unsupported file type in root manifest: "
                      + entry.contentType()
                      + ": "
                      + entry.location());
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close root manifest: " + root.path(), e);
    }

    List<CloseableIterable<TrackedFile>> leafReaders = Lists.newArrayList();
    for (ManifestFile leaf : leafManifests) {
      leafReaders.add(changesReader(leaf));
    }

    CloseableIterable<TrackedFile> leafChanges =
        executorService != null && leafReaders.size() > 1
            ? new ParallelIterable<>(leafReaders, executorService)
            : CloseableIterable.concat(leafReaders);

    return CloseableIterable.concat(
        ImmutableList.of(CloseableIterable.of(rootDataFiles), leafChanges));
  }

  private CloseableIterable<TrackedFile> changesReader(ManifestFile manifest) {
    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, io, tableSchema, specsById)
            .forChanges()
            .filter(dataFilter)
            .caseSensitive(caseSensitive);
    if (tableLocation != null) {
      builder.tableLocation(tableLocation);
    }

    if (statsFieldIds != null) {
      builder.projectStats(statsFieldIds);
    }

    return builder.build();
  }
}
