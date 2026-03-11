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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.DataFileSet;

/** Accumulates data files and flushes them to manifests when a count threshold is reached. */
class FlushingDataFileAccumulator {

  private final long flushThreshold;
  private final BiFunction<Collection<DataFile>, Integer, List<ManifestFile>> writeManifests;
  private final Function<ManifestFile, CloseableIterable<DataFile>> readManifest;

  private final Map<Integer, DataFileSet> pendingBySpec = Maps.newHashMap();
  private final List<ManifestFile> flushedManifests = Lists.newArrayList();
  private final Set<Integer> specIds = Sets.newHashSet();
  private int pendingCount = 0;

  FlushingDataFileAccumulator(
      long flushThreshold,
      BiFunction<Collection<DataFile>, Integer, List<ManifestFile>> writeManifests,
      Function<ManifestFile, CloseableIterable<DataFile>> readManifest) {
    Preconditions.checkArgument(
        flushThreshold > 0, "Flush threshold must be positive: %s", flushThreshold);
    this.flushThreshold = flushThreshold;
    this.writeManifests = writeManifests;
    this.readManifest = readManifest;
  }

  /**
   * Accumulate a data file to the pending map.
   *
   * <p>If count threshold is reached, flush all pending map to manifest to reduce memory pressure
   *
   * @return true if the file was not already in the current pending map. Files already flushed to
   *     manifests are not tracked.
   */
  boolean offer(DataFile file, int specId) {
    DataFileSet files = pendingBySpec.computeIfAbsent(specId, ignored -> DataFileSet.create());
    if (files.add(file)) {
      specIds.add(specId);
      pendingCount++;
      if (pendingCount >= flushThreshold) {
        flush();
      }
      return true;
    }
    return false;
  }

  boolean hasFiles() {
    return !pendingBySpec.isEmpty() || !flushedManifests.isEmpty();
  }

  boolean hasPendingFiles() {
    return !pendingBySpec.isEmpty();
  }

  /** Partition spec IDs seen across all adds, including flushed files. */
  Set<Integer> specIds() {
    return Collections.unmodifiableSet(specIds);
  }

  Map<Integer, DataFileSet> pendingBySpec() {
    return Collections.unmodifiableMap(pendingBySpec);
  }

  List<ManifestFile> flushedManifests() {
    return Collections.unmodifiableList(flushedManifests);
  }

  /**
   * All added data files (flushed + pending). Flushed files are lazily read back from their
   * manifests to avoid holding them in memory during the add phase.
   */
  CloseableIterable<DataFile> allAddedFiles() {
    CloseableIterable<DataFile> flushedFiles =
        CloseableIterable.concat(Iterables.transform(flushedManifests, readManifest::apply));

    CloseableIterable<DataFile> pendingFiles =
        CloseableIterable.withNoopClose(Iterables.concat(pendingBySpec.values()));

    return CloseableIterable.concat(Lists.newArrayList(flushedFiles, pendingFiles));
  }

  void deleteUncommitted(Set<ManifestFile> committed, Consumer<String> deleteFunc) {
    for (ManifestFile manifest : flushedManifests) {
      if (!committed.contains(manifest)) {
        deleteFunc.accept(manifest.path());
      }
    }
    flushedManifests.clear();
  }

  private void flush() {
    if (pendingBySpec.isEmpty()) {
      return;
    }

    pendingBySpec.forEach(
        (specId, files) -> flushedManifests.addAll(writeManifests.apply(files, specId)));

    pendingBySpec.clear();
    pendingCount = 0;
  }
}
