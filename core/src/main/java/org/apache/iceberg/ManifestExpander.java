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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.io.IOException;
import java.io.UncheckedIOException;
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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.ParallelIterable;

/**
 * Planner that expands the adaptive metadata tree into {@link FileScanTask}s.
 *
 * <p>Emits a task for each live {@code DATA} entry and expands {@code DATA_MANIFEST} entries into
 * their leaf manifests. A data entry's colocated deletion vector is attached to its task as a
 * {@link DeleteFile}.
 *
 * <p>Emitted tasks do not yet carry inherited tracking: leaf and root {@code DATA} entries are not
 * assigned the data/file sequence numbers, snapshot id, and first-row-id they inherit from their
 * parent, so {@code data_sequence_number}, {@code file_sequence_number}, and row-lineage columns
 * ({@code _row_id}, {@code _last_updated_sequence_number}) read null for added files. Inheritance
 * must be applied before this planner is wired into a scan or the delete-manifest matching path,
 * since delete scoping compares a data file's sequence number against the delete's.
 */
class ManifestExpander {
  private static final int FORMAT_VERSION = 4;
  private static final DeleteFile[] NO_DELETES = new DeleteFile[0];

  private final FileIO io;
  private final InputFile rootManifest;
  private final Map<Integer, PartitionSpec> specsById;
  private final String tableLocation;
  private final Expression dataFilter;
  private final boolean ignoreResiduals;
  private final boolean caseSensitive;
  private final ScanMetrics scanMetrics;
  private final ExecutorService executorService;
  private final LoadingCache<Integer, TaskContext> taskContextCache;

  private ManifestExpander(
      FileIO io,
      InputFile rootManifest,
      Map<Integer, PartitionSpec> specsById,
      String tableLocation,
      Expression dataFilter,
      boolean ignoreResiduals,
      boolean caseSensitive,
      ScanMetrics scanMetrics,
      ExecutorService executorService) {
    this.io = io;
    this.rootManifest = rootManifest;
    this.specsById = specsById;
    this.tableLocation = tableLocation;
    this.dataFilter = dataFilter;
    this.ignoreResiduals = ignoreResiduals;
    this.caseSensitive = caseSensitive;
    this.scanMetrics = scanMetrics;
    this.executorService = executorService;
    this.taskContextCache = Caffeine.newBuilder().build(this::newTaskContext);
  }

  static Builder builder(
      FileIO io,
      InputFile rootManifest,
      Map<Integer, PartitionSpec> specsById,
      String tableLocation) {
    return new Builder(io, rootManifest, specsById, tableLocation);
  }

  CloseableIterable<FileScanTask> planFiles() {
    List<CloseableIterable<FileScanTask>> taskGroups = Lists.newArrayList();
    List<TrackedFile> dataFiles = Lists.newArrayList();
    List<TrackedFile> leafManifests = Lists.newArrayList();

    // root is drained into these lists. Leaf references must be buffered so they can be fanned
    // out; direct DATA entries are buffered too, bounded by how many data files a tree keeps
    // directly in the root. Leaf tasks stay lazy: createLeafTasks opens no reader until iterated.
    try (CloseableIterable<TrackedFile> rootEntries = open(rootManifest)) {
      for (TrackedFile entry : rootEntries) {
        switch (entry.contentType()) {
          case DATA:
            dataFiles.add(entry);
            break;
          case DATA_MANIFEST:
            leafManifests.add(entry);
            break;
          default:
            // delete content is only produced by upgraded trees; the 2-phase path is not built
            // yet.
            throw new UnsupportedOperationException(
                "Cannot plan content type in root manifest: " + entry.contentType());
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to close root manifest: " + rootManifest.location(), e);
    }

    if (!dataFiles.isEmpty()) {
      taskGroups.add(
          CloseableIterable.transform(
              CloseableIterable.withNoopClose(dataFiles), this::createTask));
    }

    for (TrackedFile leaf : leafManifests) {
      taskGroups.add(createLeafTasks(leaf));
    }

    if (executorService != null) {
      return new ParallelIterable<>(taskGroups, executorService);
    }

    return CloseableIterable.concat(taskGroups);
  }

  private CloseableIterable<FileScanTask> createLeafTasks(TrackedFile leaf) {
    // an upgraded tree can reference legacy format leaf that must be read with the legacy
    // manifest reader; per-leaf dispatch is not built yet, so reject a non-v4 leaf.
    if (leaf.formatVersion() != FORMAT_VERSION) {
      throw new UnsupportedOperationException(
          "Cannot expand leaf manifest with format version "
              + leaf.formatVersion()
              + ": "
              + leaf.location());
    }

    // applying manifest DV during expansion is not supported yet, so reject leaf with manifest DVs.
    if (leaf.manifestInfo() != null && leaf.manifestInfo().dv() != null) {
      throw new UnsupportedOperationException(
          "Cannot apply manifest deletion vector for leaf manifest: " + leaf.location());
    }

    // an encrypted leaf must be opened through a decrypting input path; not supported yet.
    if (leaf.keyMetadata() != null) {
      throw new UnsupportedOperationException(
          "Cannot read encrypted leaf manifest: " + leaf.location());
    }

    // the reader is opened and the leaf counted as scanned lazily, only when the result is
    // iterated: a plan that is built and closed without iterating reads nothing, and each leaf
    // increments scannedDataManifests exactly once.
    return new LeafTasks(leaf);
  }

  /** A leaf's tasks, with each iteration owning and counting its own reader. */
  private class LeafTasks extends CloseableGroup implements CloseableIterable<FileScanTask> {
    private final TrackedFile leaf;

    private LeafTasks(TrackedFile leaf) {
      this.leaf = leaf;
    }

    @Override
    public CloseableIterator<FileScanTask> iterator() {
      scanMetrics.scannedDataManifests().increment();
      CloseableIterable<TrackedFile> entries = open(io.newInputFile(leaf.location()));
      CloseableIterable<FileScanTask> tasks =
          CloseableIterable.transform(entries, ManifestExpander.this::createTaskFromLeafEntry);
      addCloseable(tasks);
      return tasks.iterator();
    }
  }

  private FileScanTask createTaskFromLeafEntry(TrackedFile entry) {
    // the tree is at most two levels, so a leaf holds only DATA entries
    if (entry.contentType() == FileContent.DATA_MANIFEST) {
      throw new IllegalArgumentException(
          "Cannot expand a nested manifest in a leaf manifest: " + entry.location());
    } else if (entry.contentType() != FileContent.DATA) {
      // delete content is only produced by upgraded trees; the 2-phase path is not built yet
      throw new UnsupportedOperationException(
          "Cannot plan content type in leaf manifest: " + entry.contentType());
    }

    return createTask(entry);
  }

  private CloseableIterable<TrackedFile> open(InputFile manifest) {
    return V4ManifestReader.builder(manifest, specsById, tableLocation)
        .forScanPlanning()
        .filter(dataFilter)
        .caseSensitive(caseSensitive)
        .scanMetrics(scanMetrics)
        .build();
  }

  private FileScanTask createTask(TrackedFile trackedFile) {
    DataFile dataFile = TrackedFileAdapters.asDataFile(trackedFile, specsById);
    TaskContext context = taskContextCache.get(dataFile.specId());

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
    private final InputFile rootManifest;
    private final Map<Integer, PartitionSpec> specsById;
    private final String tableLocation;
    private Expression dataFilter = Expressions.alwaysTrue();
    private boolean ignoreResiduals = false;
    private boolean caseSensitive = true;
    private ScanMetrics scanMetrics = ScanMetrics.noop();
    private ExecutorService executorService = null;

    private Builder(
        FileIO io,
        InputFile rootManifest,
        Map<Integer, PartitionSpec> specsById,
        String tableLocation) {
      Preconditions.checkArgument(io != null, "Invalid file IO: null");
      Preconditions.checkArgument(rootManifest != null, "Invalid root manifest: null");
      Preconditions.checkArgument(specsById != null, "Invalid specs by ID: null");
      Preconditions.checkArgument(tableLocation != null, "Invalid table location: null");
      this.io = io;
      this.rootManifest = rootManifest;
      this.specsById = ImmutableMap.copyOf(specsById);
      this.tableLocation = tableLocation;
    }

    /** Narrows the filter used for partition pruning and residual evaluation. */
    Builder filterData(Expression expr) {
      Preconditions.checkArgument(expr != null, "Invalid filter: null");
      this.dataFilter = Expressions.and(dataFilter, expr);
      return this;
    }

    Builder ignoreResiduals() {
      this.ignoreResiduals = true;
      return this;
    }

    Builder caseSensitive(boolean caseSensitive) {
      this.caseSensitive = caseSensitive;
      return this;
    }

    Builder scanMetrics(ScanMetrics scanMetrics) {
      Preconditions.checkArgument(scanMetrics != null, "Invalid scan metrics: null");
      this.scanMetrics = scanMetrics;
      return this;
    }

    Builder planWith(ExecutorService executorService) {
      this.executorService = executorService;
      return this;
    }

    ManifestExpander build() {
      return new ManifestExpander(
          io,
          rootManifest,
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
