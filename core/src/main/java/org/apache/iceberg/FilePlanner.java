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
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
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
 * their leaf manifests. A data file's colocated deletion vector is attached to its task as a {@link
 * DeleteFile}.
 */
class FilePlanner {
  private static final DeleteFile[] NO_DELETES = new DeleteFile[0];

  private final FileIO io;
  private final ManifestFile root;
  private final Map<Integer, PartitionSpec> specsById;
  private final String tableLocation;
  private final Expression dataFilter;
  private final boolean ignoreResiduals;
  private final boolean caseSensitive;
  private final ScanMetrics scanMetrics;
  private final ExecutorService executorService;
  private final Map<Integer, TaskContext> taskContextsBySpec = Maps.newConcurrentMap();

  private FilePlanner(
      FileIO io,
      ManifestFile root,
      Map<Integer, PartitionSpec> specsById,
      String tableLocation,
      Expression dataFilter,
      boolean ignoreResiduals,
      boolean caseSensitive,
      ScanMetrics scanMetrics,
      ExecutorService executorService) {
    this.io = io;
    this.root = root;
    this.specsById = specsById;
    this.tableLocation = tableLocation;
    this.dataFilter = dataFilter;
    this.ignoreResiduals = ignoreResiduals;
    this.caseSensitive = caseSensitive;
    this.scanMetrics = scanMetrics;
    this.executorService = executorService;
  }

  static Builder builder(
      FileIO io, ManifestFile root, Map<Integer, PartitionSpec> specsById, String tableLocation) {
    return new Builder(io, root, specsById, tableLocation);
  }

  CloseableIterable<FileScanTask> planFiles() {
    List<TrackedFile> rootDataFiles = Lists.newArrayList();
    List<ManifestFile> leafManifests = Lists.newArrayList();

    try (CloseableIterable<TrackedFile> rootEntries = reader(root)) {
      for (TrackedFile entry : rootEntries) {
        switch (entry.contentType()) {
          case DATA:
            rootDataFiles.add(entry);
            break;
          case DATA_MANIFEST:
            leafManifests.add(TrackedFileAdapters.asManifestFile(entry));
            break;
          case DELETE_MANIFEST:
            throw new UnsupportedOperationException("v3 and earlier deletes are not yet supported");
          default:
            throw new UnsupportedOperationException(
                "Unsupported file type in root manifest: " + entry.contentType());
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close root manifest: " + root.path(), e);
    }

    List<CloseableIterable<TrackedFile>> leafFiles = Lists.newArrayList();
    for (ManifestFile leaf : leafManifests) {
      leafFiles.add(expandLeaf(leaf));
    }

    CloseableIterable<TrackedFile> expandedLeafFiles =
        executorService != null && leafManifests.size() > 1
            ? new ParallelIterable<>(leafFiles, executorService)
            : CloseableIterable.concat(leafFiles);

    CloseableIterable<TrackedFile> files =
        CloseableIterable.concat(
            ImmutableList.of(CloseableIterable.withNoopClose(rootDataFiles), expandedLeafFiles));

    CloseableIterable<DataFile> dataFiles =
        CloseableIterable.transform(files, file -> TrackedFileAdapters.asDataFile(file, specsById));

    return CloseableIterable.transform(dataFiles, this::createTask);
  }

  private CloseableIterable<TrackedFile> expandLeaf(ManifestFile leaf) {
    return CloseableIterable.transform(
        reader(leaf),
        entry -> {
          Preconditions.checkArgument(
              entry.contentType() == FileContent.DATA,
              "Unsupported file type in leaf manifest: %s",
              entry.contentType());
          return entry;
        });
  }

  private CloseableIterable<TrackedFile> reader(ManifestFile manifest) {
    return V4ManifestReader.builder(manifest, io, specsById, tableLocation)
        .forScanPlanning()
        .filter(dataFilter)
        .caseSensitive(caseSensitive)
        .scanMetrics(scanMetrics)
        .build();
  }

  private FileScanTask createTask(DataFile dataFile) {
    TaskContext context =
        taskContextsBySpec.computeIfAbsent(dataFile.specId(), this::newTaskContext);

    DeleteFile[] deletes =
        dataFile.deletionVector() != null
            ? new DeleteFile[] {TrackedFileAdapters.asDVDeleteFile(dataFile)}
            : NO_DELETES;

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
    private final ManifestFile root;
    private final Map<Integer, PartitionSpec> specsById;
    private final String tableLocation;
    private Expression dataFilter = Expressions.alwaysTrue();
    private boolean ignoreResiduals = false;
    private boolean caseSensitive = true;
    private ScanMetrics scanMetrics = ScanMetrics.noop();
    private ExecutorService executorService = null;

    private Builder(
        FileIO io, ManifestFile root, Map<Integer, PartitionSpec> specsById, String tableLocation) {
      Preconditions.checkArgument(io != null, "Invalid file IO: null");
      Preconditions.checkArgument(root != null, "Invalid root manifest: null");
      Preconditions.checkArgument(specsById != null, "Invalid specs by ID: null");
      Preconditions.checkArgument(tableLocation != null, "Invalid table location: null");
      this.io = io;
      this.root = root;
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

    FilePlanner build() {
      return new FilePlanner(
          io,
          root,
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
