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
import org.apache.iceberg.util.ThreadPools;

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
  private final Schema tableSchema;
  private final Map<Integer, PartitionSpec> specsById;
  private final Map<Integer, TaskContext> taskContextsBySpec = Maps.newConcurrentMap();

  private String tableLocation = null;
  private Expression dataFilter = Expressions.alwaysTrue();
  private boolean ignoreResiduals = false;
  private boolean caseSensitive = true;
  private ScanMetrics scanMetrics = ScanMetrics.noop();
  private ExecutorService executorService = ThreadPools.getWorkerPool();

  FilePlanner(
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

  FilePlanner tableLocation(String newTableLocation) {
    Preconditions.checkArgument(newTableLocation != null, "Invalid table location: null");
    this.tableLocation = newTableLocation;
    return this;
  }

  FilePlanner filterData(Expression expr) {
    Preconditions.checkArgument(expr != null, "Invalid filter: null");
    this.dataFilter = expr;
    return this;
  }

  FilePlanner ignoreResiduals() {
    this.ignoreResiduals = true;
    return this;
  }

  FilePlanner caseSensitive(boolean newCaseSensitive) {
    this.caseSensitive = newCaseSensitive;
    return this;
  }

  FilePlanner scanMetrics(ScanMetrics newScanMetrics) {
    Preconditions.checkArgument(newScanMetrics != null, "Invalid scan metrics: null");
    this.scanMetrics = newScanMetrics;
    return this;
  }

  FilePlanner planWith(ExecutorService newExecutorService) {
    Preconditions.checkArgument(newExecutorService != null, "Invalid executor service: null");
    this.executorService = newExecutorService;
    return this;
  }

  CloseableIterable<FileScanTask> planFiles() {
    List<DataFile> rootDataFiles = Lists.newArrayList();
    List<ManifestFile> leafManifests = Lists.newArrayList();

    try (CloseableIterable<TrackedFile> rootEntries = reader(root)) {
      for (TrackedFile entry : rootEntries) {
        switch (entry.contentType()) {
          case DATA:
            rootDataFiles.add(TrackedFileAdapters.asDataFile(entry, specsById));
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

    List<CloseableIterable<TrackedFile>> leafPlanTasks = Lists.newArrayList();
    for (ManifestFile leaf : leafManifests) {
      leafPlanTasks.add(reader(leaf));
    }

    CloseableIterable<TrackedFile> leafFiles =
        leafManifests.size() > 1
            ? new ParallelIterable<>(leafPlanTasks, executorService)
            : CloseableIterable.concat(leafPlanTasks);

    CloseableIterable<DataFile> leafDataFiles =
        CloseableIterable.transform(
            leafFiles, file -> TrackedFileAdapters.asDataFile(file, specsById));

    CloseableIterable<DataFile> dataFiles =
        CloseableIterable.concat(
            ImmutableList.of(CloseableIterable.withNoopClose(rootDataFiles), leafDataFiles));

    return CloseableIterable.transform(dataFiles, this::createTask);
  }

  private CloseableIterable<TrackedFile> reader(ManifestFile manifest) {
    V4ManifestReader.Builder builder =
        V4ManifestReader.builder(manifest, io, tableSchema, specsById)
            .forScanPlanning()
            .filter(dataFilter)
            .caseSensitive(caseSensitive)
            .scanMetrics(scanMetrics);
    if (tableLocation != null) {
      builder.tableLocation(tableLocation);
    }

    return builder.build();
  }

  private FileScanTask createTask(DataFile dataFile) {
    TaskContext context =
        taskContextsBySpec.computeIfAbsent(dataFile.specId(), this::newTaskContext);

    DeleteFile[] deletes = NO_DELETES;
    if (dataFile.deletionVector() != null) {
      TrackedFile tracked = ((TrackedFileAdapters.TrackedDataFile) dataFile).file();
      DeleteFile dv = TrackedFileAdapters.asDVDeleteFile(tracked, specsById);
      scanMetrics.dvs().increment();
      deletes = new DeleteFile[] {dv};
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
}
