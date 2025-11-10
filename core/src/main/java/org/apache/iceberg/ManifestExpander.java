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

import static org.apache.iceberg.expressions.Expressions.alwaysTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;

/**
 * Expands V4 root manifests and plans file scan tasks.
 *
 * <p>Root manifests can contain direct file entries (DATA, POSITION_DELETES, EQUALITY_DELETES) as
 * well as references to leaf manifests (DATA_MANIFEST, DELETE_MANIFEST). Handles recursive
 * expansion of manifest references and creates scan tasks, similar to ManifestGroup in V3.
 *
 * <p>Currently returns DataFileScanInfo (TrackedFile data + deletes). Full FileScanTask creation
 * blocked on ContentStats implementation.
 *
 * <p><b>TODO: (after ContentStats is ready):</b>
 *
 * <ol>
 *   <li>Implement TrackedFile.asDataFile(spec)
 *   <li>Implement TrackedFile.asDeleteFile(spec)
 *   <li>Add planFiles() method that returns CloseableIterable&lt;FileScanTask&gt;
 *   <li>Handle equality deletes
 *   <li>Add manifest-level filtering using ManifestStats
 * </ol>
 */
public class ManifestExpander extends CloseableGroup {
  private final V4ManifestReader rootReader;
  private final FileIO io;
  private final Map<Integer, PartitionSpec> specsById;

  private static final Types.StructType FILE_METADATA_TYPE =
      Types.StructType.of(
          TrackedFile.CONTENT_TYPE,
          TrackedFile.RECORD_COUNT,
          TrackedFile.FILE_SIZE_IN_BYTES,
          TrackedFile.PARTITION_SPEC_ID,
          TrackedFile.SORT_ORDER_ID);

  private Expression dataFilter;
  private Expression fileFilter;
  private Evaluator fileFilterEvaluator;
  private Map<String, List<TrackedFile<?>>> deletesByPath;

  private boolean ignoreDeleted;

  private boolean ignoreExisting;

  private List<String> columns;

  private boolean caseSensitive;

  @SuppressWarnings("UnusedVariable")
  private ScanMetrics scanMetrics;

  public ManifestExpander(
      V4ManifestReader rootReader, FileIO io, Map<Integer, PartitionSpec> specsById) {
    this.rootReader = rootReader;
    this.io = io;
    this.specsById = specsById;
    this.dataFilter = alwaysTrue();
    this.fileFilter = alwaysTrue();
    this.ignoreDeleted = false;
    this.ignoreExisting = false;
    this.columns = V4ManifestReader.ALL_COLUMNS;
    this.caseSensitive = true;
    this.scanMetrics = ScanMetrics.noop();
    addCloseable(rootReader);
  }

  private Map<String, List<TrackedFile<?>>> buildDeleteIndex() {
    if (deletesByPath != null) {
      return deletesByPath;
    }

    Map<String, List<TrackedFile<?>>> index = Maps.newHashMap();

    try (CloseableIterable<TrackedFile<?>> allFiles = allTrackedFiles()) {
      for (TrackedFile<?> entry : allFiles) {
        if (entry.contentType() == FileContent.POSITION_DELETES && entry.referencedFile() != null) {
          index.computeIfAbsent(entry.referencedFile(), k -> Lists.newArrayList()).add(entry);
        }
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to build delete index");
    }

    this.deletesByPath = index;
    return index;
  }

  private List<TrackedFile<?>> deleteFilesForDataFile(TrackedFile<?> dataFile) {
    Map<String, List<TrackedFile<?>>> index = buildDeleteIndex();
    List<TrackedFile<?>> deletes = index.get(dataFile.location());

    if (deletes == null || deletes.isEmpty()) {
      return Collections.emptyList();
    }

    TrackingInfo dataTracking = dataFile.trackingInfo();
    Long dataSeq = dataTracking != null ? dataTracking.sequenceNumber() : null;

    if (dataSeq == null) {
      return deletes;
    }

    List<TrackedFile<?>> filtered = Lists.newArrayList();
    for (TrackedFile<?> delete : deletes) {
      TrackingInfo deleteTracking = delete.trackingInfo();
      Long deleteSeq = deleteTracking != null ? deleteTracking.sequenceNumber() : null;

      if (deleteSeq == null || deleteSeq <= dataSeq) {
        filtered.add(delete);
      }
    }

    return filtered;
  }

  public ManifestExpander filterData(Expression expression) {
    this.dataFilter = Expressions.and(dataFilter, expression);
    return this;
  }

  public ManifestExpander filterFiles(Expression expression) {
    this.fileFilter = Expressions.and(fileFilter, expression);
    this.fileFilterEvaluator = null;
    return this;
  }

  public ManifestExpander ignoreDeleted() {
    this.ignoreDeleted = true;
    return this;
  }

  public ManifestExpander ignoreExisting() {
    this.ignoreExisting = true;
    return this;
  }

  public ManifestExpander select(List<String> selectedColumns) {
    this.columns = Lists.newArrayList(selectedColumns);
    return this;
  }

  public ManifestExpander caseSensitive(boolean isCaseSensitive) {
    this.caseSensitive = isCaseSensitive;
    return this;
  }

  public ManifestExpander scanMetrics(ScanMetrics metrics) {
    this.scanMetrics = metrics;
    return this;
  }

  /**
   * Plans file scan tasks for table scans with delete matching.
   *
   * <p>This is the main entry point for query planning, similar to ManifestGroup.planFiles() in V3.
   *
   * <p>Filters and matching:
   *
   * <ul>
   *   <li>Entry-level: Filters by status (ignoreDeleted/ignoreExisting)
   *   <li>Content-type: Returns only DATA files
   *   <li>File-level: Filters by file metadata (record_count, file_size, etc.)
   *   <li>Delete matching: Path-based matching for POSITION_DELETES with referencedFile
   * </ul>
   *
   * <p>Returns DataFileScanInfo which pairs each data file with matched delete files.
   *
   * <p>TODO: When ContentStats is implemented, add planFiles() method that returns
   * CloseableIterable&lt;FileScanTask&gt;:
   *
   * <pre>{@code
   * public CloseableIterable<FileScanTask> planFiles() {
   *     return CloseableIterable.transform(planDataFiles(), this::createFileScanTask);
   * }
   *
   * private FileScanTask createFileScanTask(DataFileScanInfo scanInfo) {
   *     TrackedFile<?> tf = scanInfo.dataFile();
   *     PartitionSpec spec = specsById.get(tf.partitionSpecId());
   *
   *     // TrackedFile.asDataFile(spec) extracts partition from contentStats internally
   *     DataFile dataFile = tf.asDataFile(spec);
   *
   *     // Convert TrackedFile deletes → DeleteFile array
   *     DeleteFile[] deleteFiles = convertDeleteFiles(scanInfo.deleteFiles(), spec);
   *
   *     // Build residual evaluator
   *     ResidualEvaluator residuals = ResidualEvaluator.of(spec, dataFilter, caseSensitive);
   *
   *     // Create task
   *     return new BaseFileScanTask(
   *         dataFile,
   *         deleteFiles,
   *         SchemaParser.toJson(spec.schema()),
   *         PartitionSpecParser.toJson(spec),
   *         residuals);
   * }
   *
   * private DeleteFile[] convertDeleteFiles(
   *     List<TrackedFile<?>> deleteTrackedFiles,
   *     PartitionSpec spec) {
   *     DeleteFile[] deleteFiles = new DeleteFile[deleteTrackedFiles.size()];
   *     for (int i = 0; i < deleteTrackedFiles.size(); i++) {
   *         // TrackedFile.asDeleteFile(spec) extracts partition from contentStats internally
   *         deleteFiles[i] = deleteTrackedFiles.get(i).asDeleteFile(spec);
   *     }
   *     return deleteFiles;
   * }
   * }</pre>
   *
   * <p>TODO: When ContentStats is implemented:
   *
   * <ul>
   *   <li>Equality deletes (match by partition + equality fields)
   * </ul>
   *
   * @return iterable of DataFileScanInfo (data file + matched deletes)
   */
  public CloseableIterable<DataFileScanInfo> planDataFiles() {
    CloseableIterable<TrackedFile<?>> allFiles =
        CloseableIterable.concat(Lists.newArrayList(directFiles(), expandManifests()));

    allFiles = CloseableIterable.filter(allFiles, tf -> tf.contentType() == FileContent.DATA);

    allFiles = applyStatusFilters(allFiles);

    allFiles = applyFileFilter(allFiles);

    buildDeleteIndex();

    return CloseableIterable.transform(
        allFiles,
        dataFile -> {
          List<TrackedFile<?>> deletes = deleteFilesForDataFile(dataFile);
          return new DataFileScanInfo(dataFile, deletes);
        });
  }

  /**
   * Information needed to scan a data file with associated delete files.
   *
   * <p>This is the V4 equivalent of the information used to create FileScanTask in V3. When
   * ContentStats is implemented, this will be converted to BaseFileScanTask.
   *
   * <p>Contains:
   *
   * <ul>
   *   <li>Data file (as TrackedFile)
   *   <li>Matched delete files (as TrackedFile list)
   * </ul>
   *
   * <p>TODO: When ContentStats available, convert this to FileScanTask by:
   *
   * <ol>
   *   <li>Call TrackedFile.asDataFile(spec) - extracts partition from contentStats internally
   *   <li>Call TrackedFile.asDeleteFile(spec) for deletes - extracts partition internally
   *   <li>Create BaseFileScanTask with DataFile, DeleteFile array, and residuals
   * </ol>
   */
  public static class DataFileScanInfo {
    private final TrackedFile<?> dataFile;
    private final List<TrackedFile<?>> deleteFiles;

    DataFileScanInfo(TrackedFile<?> dataFile, List<TrackedFile<?>> deleteFiles) {
      this.dataFile = dataFile;
      this.deleteFiles = deleteFiles;
    }

    public TrackedFile<?> dataFile() {
      return dataFile;
    }

    public List<TrackedFile<?>> deleteFiles() {
      return deleteFiles;
    }
  }

  /**
   * Returns all TrackedFiles from the root manifest and all referenced leaf manifests.
   *
   * <p>This includes:
   *
   * <ul>
   *   <li>Direct file entries in root (DATA, POSITION_DELETES, EQUALITY_DELETES)
   *   <li>Files from expanded DATA_MANIFEST entries
   *   <li>Files from expanded DELETE_MANIFEST entries
   * </ul>
   *
   * <p>Returns TrackedFile entries (not converted to DataFile/DeleteFile yet).
   *
   * <p>TODO: Add manifest DV support.
   *
   * @return iterable of all tracked files
   */
  public CloseableIterable<TrackedFile<?>> allTrackedFiles() {
    return CloseableIterable.concat(Lists.newArrayList(directFiles(), expandManifests()));
  }

  private CloseableIterable<TrackedFile<?>> applyStatusFilters(
      CloseableIterable<TrackedFile<?>> entries) {
    CloseableIterable<TrackedFile<?>> filtered = entries;

    if (ignoreDeleted) {
      filtered =
          CloseableIterable.filter(
              filtered,
              tf -> {
                TrackingInfo tracking = tf.trackingInfo();
                return tracking == null || tracking.status() != TrackingInfo.Status.DELETED;
              });
    }

    if (ignoreExisting) {
      filtered =
          CloseableIterable.filter(
              filtered,
              tf -> {
                TrackingInfo tracking = tf.trackingInfo();
                return tracking != null && tracking.status() != TrackingInfo.Status.EXISTING;
              });
    }

    return filtered;
  }

  private CloseableIterable<TrackedFile<?>> applyFileFilter(
      CloseableIterable<TrackedFile<?>> entries) {
    if (fileFilter == null || fileFilter == alwaysTrue()) {
      return entries;
    }

    return CloseableIterable.filter(entries, this::evaluateFileFilter);
  }

  private boolean evaluateFileFilter(TrackedFile<?> file) {
    if (fileFilterEvaluator == null) {
      fileFilterEvaluator = new Evaluator(FILE_METADATA_TYPE, fileFilter, caseSensitive);
    }
    return fileFilterEvaluator.eval((StructLike) file);
  }

  /**
   * Returns direct file entries from the root manifest.
   *
   * <p>These are DATA, POSITION_DELETES, or EQUALITY_DELETES entries stored directly in the root
   * manifest
   */
  private CloseableIterable<TrackedFile<?>> directFiles() {
    return CloseableIterable.filter(
        rootReader.liveEntries(),
        tf ->
            tf.contentType() == FileContent.DATA
                || tf.contentType() == FileContent.POSITION_DELETES
                || tf.contentType() == FileContent.EQUALITY_DELETES);
  }

  /**
   * Expands manifest references (DATA_MANIFEST and DELETE_MANIFEST) to their contained files.
   *
   * <p>Loads all root entries to identify manifests, then reads each leaf manifest serially.
   *
   * <p>Applies manifest DVs (MANIFEST_DV entries) to skip deleted positions in leaf manifests.
   *
   * <p>TODO: Add parallel manifest reading support via ExecutorService (like
   * ManifestGroup.planWith). This would use ParallelIterable to read leaf manifests concurrently.
   */
  private CloseableIterable<TrackedFile<?>> expandManifests() {
    List<TrackedFile<?>> rootEntries = Lists.newArrayList(rootReader.liveEntries());

    Map<String, TrackedFile<?>> dvByTarget = indexManifestDVs(rootEntries);

    List<CloseableIterable<TrackedFile<?>>> allLeafFiles = Lists.newArrayList();

    for (TrackedFile<?> manifestEntry : rootEntries) {
      if (manifestEntry.contentType() == FileContent.DATA_MANIFEST
          || manifestEntry.contentType() == FileContent.DELETE_MANIFEST) {
        TrackedFile<?> dv = dvByTarget.get(manifestEntry.location());

        V4ManifestReader leafReader =
            V4ManifestReaders.readLeaf(manifestEntry, io, specsById)
                .select(columns)
                .withDeletionVector(dv);
        addCloseable(leafReader);
        allLeafFiles.add(leafReader.liveEntries());
      }
    }

    return CloseableIterable.concat(allLeafFiles);
  }

  private Map<String, TrackedFile<?>> indexManifestDVs(List<TrackedFile<?>> rootEntries) {
    Map<String, TrackedFile<?>> index = Maps.newHashMap();

    for (TrackedFile<?> entry : rootEntries) {
      if (entry.contentType() == FileContent.MANIFEST_DV) {
        String target = entry.referencedFile();
        TrackedFile<?> existing = index.put(target, entry);

        Preconditions.checkState(
            existing == null, "Multiple MANIFEST_DVs found for manifest: %s", target);
      }
    }

    return index;
  }
}
