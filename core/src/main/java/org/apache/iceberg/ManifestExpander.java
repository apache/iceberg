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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.ParallelIterable;
import org.roaringbitmap.RoaringBitmap;

/**
 * V4 replacement for {@link ManifestGroup}.
 *
 * <p>Reads v4 manifests via {@link V4ManifestReader}, separates entries by content type, expands
 * leaf manifests (DATA_MANIFEST entries), and converts DATA entries to {@link FileScanTask}
 * instances using {@link TrackedFileAdapters}.
 *
 * <p>DV support is deferred to a future phase. Delete files are not matched to data files.
 */
class ManifestExpander extends CloseableGroup {
  private final FileIO io;
  private final Iterable<ManifestFile> manifests;
  private final Map<Integer, PartitionSpec> specsById;

  private Expression dataFilter = Expressions.alwaysTrue();
  private boolean ignoreResiduals = false;
  private boolean caseSensitive = true;

  @SuppressWarnings("UnusedVariable")
  private ScanMetrics scanMetrics = ScanMetrics.noop();

  private ExecutorService executorService = null;
  private String tableLocation;

  ManifestExpander(
      FileIO io, Iterable<ManifestFile> manifests, Map<Integer, PartitionSpec> specsById) {
    this.io = io;
    this.manifests = manifests;
    this.specsById = specsById;
  }

  ManifestExpander tableLocation(String newTableLocation) {
    this.tableLocation = newTableLocation;
    return this;
  }

  ManifestExpander filterData(Expression newDataFilter) {
    this.dataFilter = Expressions.and(dataFilter, newDataFilter);
    return this;
  }

  ManifestExpander ignoreResiduals() {
    this.ignoreResiduals = true;
    return this;
  }

  ManifestExpander caseSensitive(boolean newCaseSensitive) {
    this.caseSensitive = newCaseSensitive;
    return this;
  }

  ManifestExpander scanMetrics(ScanMetrics newScanMetrics) {
    this.scanMetrics = newScanMetrics;
    return this;
  }

  ManifestExpander planWith(ExecutorService newExecutorService) {
    this.executorService = newExecutorService;
    return this;
  }

  CloseableIterable<FileScanTask> planFiles() {
    List<CloseableIterable<FileScanTask>> taskGroups = Lists.newArrayList();

    for (ManifestFile manifest : manifests) {
      taskGroups.addAll(expandManifest(manifest));
    }

    if (executorService != null) {
      return new ParallelIterable<>(taskGroups, executorService);
    }

    return CloseableIterable.concat(taskGroups);
  }

  private List<CloseableIterable<FileScanTask>> expandManifest(ManifestFile manifest) {
    InputFile manifestFile = io.newInputFile(manifest);
    V4ManifestReader reader = new V4ManifestReader(manifestFile, specsById);
    addCloseable(reader);

    // read all live entries once and partition by content type (entries are copied)
    List<TrackedFile> dataFiles = Lists.newArrayList();
    List<TrackedFile> leafManifests = Lists.newArrayList();

    try (CloseableIterable<TrackedFile> liveEntries = reader.liveEntries()) {
      for (TrackedFile entry : liveEntries) {
        switch (entry.contentType()) {
          case DATA:
            dataFiles.add(entry.copy());
            break;
          case DATA_MANIFEST:
            leafManifests.add(entry.copy());
            break;
          default:
            // EQUALITY_DELETES, DELETE_MANIFEST: skip for now (future phase)
            break;
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    List<CloseableIterable<FileScanTask>> result = Lists.newArrayList();

    // direct DATA entries from root
    if (!dataFiles.isEmpty()) {
      result.add(
          CloseableIterable.transform(
              CloseableIterable.withNoopClose(dataFiles), this::createTask));
    }

    // expand leaf manifests
    for (TrackedFile leafEntry : leafManifests) {
      result.add(expandLeafManifest(leafEntry));
    }

    return result;
  }

  private CloseableIterable<FileScanTask> expandLeafManifest(TrackedFile manifestEntry) {
    String leafLocation = LocationUtil.resolve(manifestEntry.location(), tableLocation);
    InputFile leafFile = io.newInputFile(leafLocation);
    V4ManifestReader leafReader = new V4ManifestReader(leafFile, specsById);
    addCloseable(leafReader);

    RoaringBitmap deletedPositions = deletedPositions(manifestEntry);
    CloseableIterable<TrackedFile> entries;
    if (deletedPositions != null) {
      // use all entries (not just live) so position counter matches manifest ordinals
      AtomicInteger position = new AtomicInteger(0);
      entries =
          CloseableIterable.filter(
              leafReader.entries(),
              tf -> !deletedPositions.contains(position.getAndIncrement()) && isLiveData(tf));
    } else {
      entries =
          CloseableIterable.filter(
              leafReader.liveEntries(), tf -> tf.contentType() == FileContent.DATA);
    }

    return CloseableIterable.transform(entries, tf -> createTask(tf.copy()));
  }

  private static boolean isLiveData(TrackedFile tf) {
    if (tf == null || tf.contentType() != FileContent.DATA) {
      return false;
    }

    Tracking tracking = tf.tracking();
    return tracking != null && tracking.isLive();
  }

  private static RoaringBitmap deletedPositions(TrackedFile manifestEntry) {
    Tracking tracking = manifestEntry.tracking();
    if (tracking == null) {
      return null;
    }

    ByteBuffer deleted = tracking.deletedPositions();
    if (deleted == null) {
      return null;
    }

    return deserializeBitmap(deleted);
  }

  private static RoaringBitmap deserializeBitmap(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.asReadOnlyBuffer().get(bytes);

    RoaringBitmap bitmap = new RoaringBitmap();
    try {
      bitmap.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to deserialize metadata deletion vector");
    }

    return bitmap;
  }

  private FileScanTask createTask(TrackedFile trackedFile) {
    int specId = trackedFile.specId() != null ? trackedFile.specId() : 0;
    PartitionSpec spec = specsById.get(specId);
    DataFile dataFile = TrackedFileAdapters.asDataFile(trackedFile, spec, tableLocation);

    Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : dataFilter;
    ResidualEvaluator residuals = ResidualEvaluator.of(spec, filter, caseSensitive);

    return new BaseFileScanTask(
        dataFile,
        new DeleteFile[0],
        SchemaParser.toJson(spec.schema()),
        PartitionSpecParser.toJson(spec),
        residuals);
  }
}
