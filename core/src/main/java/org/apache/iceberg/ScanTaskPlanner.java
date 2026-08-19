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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.metrics.ScanMetricsUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ParallelIterable;

/**
 * Plans {@link FileScanTask}s from a V4 root manifest.
 *
 * <p>Emits a task for each live {@code DATA} entry and expands {@code DATA_MANIFEST} entries into
 * their leaf manifests. A data entry's colocated deletion vector is attached to its task as a
 * {@link DeleteFile}.
 */
class ScanTaskPlanner {
  private static final DeleteFile[] NO_DELETES = new DeleteFile[0];

  private final FileIO io;
  private final String rootManifestLocation;
  private final Map<Integer, PartitionSpec> specsById;
  private final String tableLocation;
  private final Expression dataFilter;
  private final boolean ignoreResiduals;
  private final boolean caseSensitive;
  private final ScanMetrics scanMetrics;
  private final ExecutorService executorService;
  private final Map<Integer, TaskContext> taskContextsBySpec = Maps.newConcurrentMap();

  private ScanTaskPlanner(
      FileIO io,
      String rootManifestLocation,
      Map<Integer, PartitionSpec> specsById,
      String tableLocation,
      Expression dataFilter,
      boolean ignoreResiduals,
      boolean caseSensitive,
      ScanMetrics scanMetrics,
      ExecutorService executorService) {
    this.io = io;
    this.rootManifestLocation = rootManifestLocation;
    this.specsById = specsById;
    this.tableLocation = tableLocation;
    this.dataFilter = dataFilter;
    this.ignoreResiduals = ignoreResiduals;
    this.caseSensitive = caseSensitive;
    this.scanMetrics = scanMetrics;
    this.executorService = executorService;
  }

  static Builder builder(
      FileIO io,
      String rootManifestLocation,
      Map<Integer, PartitionSpec> specsById,
      String tableLocation) {
    return new Builder(io, rootManifestLocation, specsById, tableLocation);
  }

  CloseableIterable<FileScanTask> planFiles() {
    List<TrackedFile> rootDataFiles = Lists.newArrayList();
    List<TrackedFile> leafManifests = Lists.newArrayList();

    // root is drained into these lists. Leaf references must be buffered so they can be fanned
    // out; direct DATA entries are buffered too, bounded by how many data files a tree keeps
    // directly in the root. Leaf tasks stay lazy: planLeaf opens no reader until iterated.
    scanMetrics.scannedDataManifests().increment();
    try (CloseableIterable<TrackedFile> rootEntries =
        open(io.newInputFile(rootManifestLocation), /* manifestDv= */ null)) {
      for (TrackedFile entry : rootEntries) {
        switch (entry.contentType()) {
          case DATA:
            rootDataFiles.add(entry);
            break;
          case DATA_MANIFEST:
            leafManifests.add(entry);
            break;
          default:
            // delete manifests appear only on upgraded v2/v3 tables; that 2-phase path is not
            // supported yet, so a natively-written v4 tree is the only supported input for now
            throw new UnsupportedOperationException(
                "Cannot plan content type in root manifest: " + entry.contentType());
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close root manifest: " + rootManifestLocation, e);
    }

    // read each leaf's data entries lazily; only leaf reads are worth the parallel backend, so the
    // already-in-hand root data entries are concatenated in directly
    List<CloseableIterable<TrackedFile>> leafDataEntries = Lists.newArrayList();
    for (TrackedFile leaf : leafManifests) {
      leafDataEntries.add(planLeaf(leaf));
    }

    CloseableIterable<TrackedFile> expandedLeafEntries =
        executorService != null
            ? new ParallelIterable<>(leafDataEntries, executorService)
            : CloseableIterable.concat(leafDataEntries);

    CloseableIterable<TrackedFile> dataEntries =
        CloseableIterable.concat(
            ImmutableList.of(CloseableIterable.withNoopClose(rootDataFiles), expandedLeafEntries));

    return CloseableIterable.transform(dataEntries, this::createTask);
  }

  private CloseableIterable<TrackedFile> planLeaf(TrackedFile leaf) {
    // an upgraded tree can reference a legacy-format leaf
    if (leaf.formatVersion() != TableMetadata.MIN_FORMAT_VERSION_ADAPTIVE_MANIFEST_TREE) {
      throw new UnsupportedOperationException(
          "Cannot expand leaf manifest with format version "
              + leaf.formatVersion()
              + ": "
              + leaf.location());
    }

    if (leaf.keyMetadata() != null) {
      throw new UnsupportedOperationException(
          "Cannot read encrypted leaf manifest: " + leaf.location());
    }

    return new LeafDataEntries(leaf);
  }

  /** Lazily reads one leaf manifest's data entries; each iteration owns and counts its reader. */
  private class LeafDataEntries extends CloseableGroup implements CloseableIterable<TrackedFile> {
    private final TrackedFile leaf;

    private LeafDataEntries(TrackedFile leaf) {
      this.leaf = leaf;
    }

    @Override
    public CloseableIterator<TrackedFile> iterator() {
      // pass the known leaf size so the reader sizes the read instead of stat-ing the file
      ByteBuffer manifestDv = leaf.manifestInfo() != null ? leaf.manifestInfo().dv() : null;
      CloseableIterable<TrackedFile> entries =
          open(io.newInputFile(leaf.location(), leaf.fileSizeInBytes()), manifestDv);
      addCloseable(entries);
      scanMetrics.scannedDataManifests().increment();
      return CloseableIterable.transform(entries, ScanTaskPlanner.this::requireDataEntry)
          .iterator();
    }
  }

  private CloseableIterable<TrackedFile> open(InputFile manifest, ByteBuffer manifestDv) {
    return V4ManifestReader.builder(manifest, specsById, tableLocation)
        .forScanPlanning()
        .filter(dataFilter)
        .caseSensitive(caseSensitive)
        .scanMetrics(scanMetrics)
        .manifestDv(manifestDv)
        .build();
  }

  private TrackedFile requireDataEntry(TrackedFile entry) {
    // the tree is at most two levels, so a leaf holds only DATA entries
    Preconditions.checkArgument(
        entry.contentType() != FileContent.DATA_MANIFEST,
        "Cannot expand a nested manifest in a leaf manifest: %s",
        entry.location());
    if (entry.contentType() != FileContent.DATA) {
      throw new UnsupportedOperationException(
          "Cannot plan content type in leaf manifest: " + entry.contentType());
    }

    return entry;
  }

  private FileScanTask createTask(TrackedFile trackedFile) {
    DataFile dataFile = TrackedFileAdapters.asDataFile(trackedFile, specsById);
    TaskContext context =
        taskContextsBySpec.computeIfAbsent(dataFile.specId(), this::newTaskContext);

    DeleteFile[] deletes;
    if (trackedFile.deletionVector() != null) {
      deletes = new DeleteFile[] {TrackedFileAdapters.asDVDeleteFile(trackedFile, specsById)};
    } else {
      deletes = NO_DELETES;
    }

    ScanMetricsUtil.fileTask(scanMetrics, dataFile, deletes);

    return new BaseFileScanTask(
        dataFile, deletes, context.schemaAsString, context.specAsString, context.residuals);
  }

  private TaskContext newTaskContext(int specId) {
    PartitionSpec spec = specsById.get(specId);
    Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : dataFilter;
    return new TaskContext(
        SchemaParser.toJson(spec.schema()),
        PartitionSpecParser.toJson(spec),
        ResidualEvaluator.of(spec, filter, caseSensitive));
  }

  /** Per-spec task inputs computed once and shared across all files of a spec. */
  private static class TaskContext {
    private final String schemaAsString;
    private final String specAsString;
    private final ResidualEvaluator residuals;

    private TaskContext(String schemaAsString, String specAsString, ResidualEvaluator residuals) {
      this.schemaAsString = schemaAsString;
      this.specAsString = specAsString;
      this.residuals = residuals;
    }
  }

  static class Builder {
    private final FileIO io;
    private final String rootManifestLocation;
    private final Map<Integer, PartitionSpec> specsById;
    private final String tableLocation;
    private Expression dataFilter = Expressions.alwaysTrue();
    private boolean ignoreResiduals = false;
    private boolean caseSensitive = true;
    private ScanMetrics scanMetrics = ScanMetrics.noop();
    private ExecutorService executorService = null;

    private Builder(
        FileIO io,
        String rootManifestLocation,
        Map<Integer, PartitionSpec> specsById,
        String tableLocation) {
      Preconditions.checkArgument(io != null, "Invalid file IO: null");
      Preconditions.checkArgument(
          rootManifestLocation != null, "Invalid root manifest location: null");
      Preconditions.checkArgument(specsById != null, "Invalid specs by ID: null");
      Preconditions.checkArgument(tableLocation != null, "Invalid table location: null");
      this.io = io;
      this.rootManifestLocation = rootManifestLocation;
      this.specsById = ImmutableMap.copyOf(specsById);
      this.tableLocation = tableLocation;
    }

    /** Sets the filter used for partition pruning and residual evaluation. */
    Builder filterData(Expression expr) {
      Preconditions.checkArgument(expr != null, "Invalid filter: null");
      this.dataFilter = expr;
      return this;
    }

    Builder ignoreResiduals() {
      this.ignoreResiduals = true;
      return this;
    }

    Builder caseSensitive(boolean newCaseSensitive) {
      this.caseSensitive = newCaseSensitive;
      return this;
    }

    Builder scanMetrics(ScanMetrics newScanMetrics) {
      Preconditions.checkArgument(newScanMetrics != null, "Invalid scan metrics: null");
      this.scanMetrics = newScanMetrics;
      return this;
    }

    Builder planWith(ExecutorService newExecutorService) {
      Preconditions.checkArgument(newExecutorService != null, "Invalid executor service: null");
      this.executorService = newExecutorService;
      return this;
    }

    ScanTaskPlanner build() {
      return new ScanTaskPlanner(
          io,
          rootManifestLocation,
          specsById,
          tableLocation,
          dataFilter,
          ignoreResiduals,
          caseSensitive,
          scanMetrics,
          executorService);
    }
  }
}
