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
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;

/** Reader that reads a v4+ manifest file as {@link TrackedFile}s. */
class V4ManifestReader extends CloseableGroup implements CloseableIterable<TrackedFile> {
  private final InputFile file;
  private final Schema readSchema;
  private final boolean includeTombstones;
  private final ScanMetrics scanMetrics;

  // partition pruning state, keyed by spec ID
  private final Map<Integer, Evaluator> partitionEvaluators;
  private final Map<Integer, StructProjection> partitionProjections;

  private V4ManifestReader(
      InputFile file,
      Schema readSchema,
      Map<Integer, Evaluator> partitionEvaluators,
      Map<Integer, StructProjection> partitionProjections,
      boolean includeTombstones,
      ScanMetrics scanMetrics) {
    this.file = file;
    this.readSchema = readSchema;
    this.partitionEvaluators = partitionEvaluators;
    this.partitionProjections = partitionProjections;
    this.includeTombstones = includeTombstones;
    this.scanMetrics = scanMetrics;
  }

  static Builder builder(InputFile file, Map<Integer, PartitionSpec> specsById) {
    return new Builder(file, specsById);
  }

  /** Returns copies of the tracked files that match this reader's configured filters. */
  @Override
  public CloseableIterator<TrackedFile> iterator() {
    CloseableIterable<TrackedFile> entries = CloseableIterable.transform(open(), this::prepare);
    if (!partitionEvaluators.isEmpty()) {
      // manifest references are expanded later and are not pruned by the partition filter
      entries =
          CloseableIterable.filter(entries, entry -> isManifest(entry) || matchesPartition(entry));
    }

    if (!includeTombstones) {
      entries = CloseableIterable.filter(entries, entry -> entry.tracking().isLive());
    }

    return CloseableIterable.transform(entries, TrackedFile::copy).iterator();
  }

  private boolean matchesPartition(TrackedFile trackedFile) {
    Integer specId = trackedFile.specId();
    if (specId == null) {
      // a file without a spec is not partitioned and may match the filter
      return true;
    }

    Evaluator evaluator = partitionEvaluators.get(specId);
    if (evaluator == null) {
      // the row filter does not project to a partition filter for this spec
      return true;
    }

    StructProjection projection = partitionProjections.get(specId);
    Preconditions.checkState(
        projection != null,
        "Cannot produce partition tuple for spec ID %s in manifest %s",
        specId,
        file.location());

    boolean matches = evaluator.eval(projection.wrap(trackedFile.partition()));
    if (!matches) {
      incrementSkipCount(trackedFile.contentType());
    }

    return matches;
  }

  private void incrementSkipCount(FileContent content) {
    switch (content) {
      case DATA:
        scanMetrics.skippedDataFiles().increment();
        break;
      case EQUALITY_DELETES:
        scanMetrics.skippedDeleteFiles().increment();
        break;
      case DATA_MANIFEST:
        scanMetrics.skippedDataManifests().increment();
        break;
      case DELETE_MANIFEST:
        scanMetrics.skippedDeleteManifests().increment();
        break;
      default:
        throw new UnsupportedOperationException("Unsupported content type: " + content);
    }
  }

  private CloseableIterable<TrackedFile> open() {
    FileFormat format = FileFormat.fromFileName(file.location());
    Preconditions.checkArgument(
        format != null, "Cannot determine format of manifest: %s", file.location());

    CloseableIterable<TrackedFile> reader =
        InternalData.read(format, file)
            .project(readSchema)
            .setRootType(TrackedFileStruct.class)
            .setCustomType(TrackedFile.TRACKING.fieldId(), TrackingStruct.class)
            .setCustomType(TrackedFile.DELETION_VECTOR.fieldId(), DeletionVectorStruct.class)
            .setCustomType(TrackedFile.MANIFEST_INFO.fieldId(), ManifestInfoStruct.class)
            .setCustomType(TrackedFile.PARTITION_ID, PartitionData.class)
            .reuseContainers()
            .build();
    addCloseable(reader);
    return reader;
  }

  private TrackedFile prepare(TrackedFile trackedFile) {
    Tracking tracking = trackedFile.tracking();
    // manifestLocation is not stored in the manifest; the reader fills it in
    if (tracking instanceof TrackingStruct) {
      ((TrackingStruct) tracking).setManifestLocation(file.location());
    }

    return trackedFile;
  }

  private static boolean isManifest(TrackedFile trackedFile) {
    FileContent content = trackedFile.contentType();
    return content == FileContent.DATA_MANIFEST || content == FileContent.DELETE_MANIFEST;
  }

  static class Builder {
    private final InputFile file;
    private final Types.StructType unionPartitionType;
    private final Map<Integer, PartitionSpec> specsById;
    private Expression rowFilter = Expressions.alwaysTrue();
    private boolean caseSensitive = true;
    private boolean includeTombstones = false;
    private boolean scanPlanning = false;
    private Collection<String> columns = null;
    private Schema fileProjection = null;
    private ScanMetrics scanMetrics = ScanMetrics.noop();

    private Builder(InputFile file, Map<Integer, PartitionSpec> specsById) {
      this.file = file;
      this.unionPartitionType = Partitioning.unionPartitionTypes(specsById.values());
      this.specsById = specsById;
    }

    /** Sets a row filter; files that cannot match the expression are skipped. */
    Builder filterRows(Expression expr) {
      Preconditions.checkArgument(expr != null, "Invalid row filter: null");
      this.rowFilter = expr;
      return this;
    }

    Builder caseSensitive(boolean isCaseSensitive) {
      this.caseSensitive = isCaseSensitive;
      return this;
    }

    /** Returns deleted and replaced files in addition to {@link Tracking#isLive() live} files. */
    Builder includeTombstones() {
      this.includeTombstones = true;
      return this;
    }

    /** Configures the reader to select the minimal fields needed for scan planning. */
    Builder forScanPlanning() {
      Preconditions.checkState(
          columns == null && fileProjection == null,
          "Cannot use forScanPlanning() with select(Collection<String>) or project(Schema)");
      this.scanPlanning = true;
      return this;
    }

    /** Selects columns to read by name; fields needed by the reader are always read. */
    Builder select(Collection<String> newColumns) {
      Preconditions.checkArgument(newColumns != null, "Invalid columns: null");
      Preconditions.checkState(
          !scanPlanning, "Cannot use select(Collection<String>) with forScanPlanning()");
      Preconditions.checkState(
          fileProjection == null,
          "Cannot select columns using both select(Collection<String>) and project(Schema)");
      this.columns = newColumns;
      return this;
    }

    /** Sets the exact schema to read; used in place of {@link #select(Collection)}. */
    Builder project(Schema newFileProjection) {
      Preconditions.checkState(!scanPlanning, "Cannot use project(Schema) with forScanPlanning()");
      Preconditions.checkState(
          columns == null,
          "Cannot select columns using both select(Collection<String>) and project(Schema)");
      this.fileProjection = newFileProjection;
      return this;
    }

    Builder scanMetrics(ScanMetrics newScanMetrics) {
      Preconditions.checkArgument(newScanMetrics != null, "Invalid scan metrics: null");
      this.scanMetrics = newScanMetrics;
      return this;
    }

    V4ManifestReader build() {
      Map<Integer, Evaluator> partitionEvaluators = Maps.newHashMap();
      Map<Integer, StructProjection> partitionProjections = Maps.newHashMap();
      if (hasPartitionFilter()) {
        for (PartitionSpec spec : specsById.values()) {
          Expression partFilter = Projections.inclusive(spec, caseSensitive).project(rowFilter);
          if (partFilter != Expressions.alwaysTrue()) {
            partitionEvaluators.put(
                spec.specId(), new Evaluator(spec.partitionType(), partFilter, caseSensitive));
            partitionProjections.put(
                spec.specId(), StructProjection.create(unionPartitionType, spec.partitionType()));
          }
        }
      }

      return new V4ManifestReader(
          file,
          readSchema(),
          partitionEvaluators,
          partitionProjections,
          includeTombstones,
          scanMetrics);
    }

    private boolean hasPartitionFilter() {
      return rowFilter != Expressions.alwaysTrue() && !unionPartitionType.fields().isEmpty();
    }

    private Schema readSchema() {
      Types.StructType trackingType =
          scanPlanning ? TrackingStruct.SCAN_TYPE : TrackingStruct.BASE_TYPE;
      Schema fullSchema =
          new Schema(
              TrackedFile.schema(trackingType, unionPartitionType, Types.StructType.of()).fields());
      Schema projection = projection(fullSchema);
      if (projection == null) {
        return fullSchema;
      }

      Set<Integer> projectedIds = Sets.newHashSet(TypeUtil.getProjectedIds(projection));

      // fields the reader itself needs: status for live filtering, row_position for manifestPos,
      // record count, and content type to distinguish entry kinds
      projectedIds.add(Tracking.STATUS.fieldId());
      projectedIds.add(MetadataColumns.ROW_POSITION.fieldId());
      projectedIds.add(TrackedFile.RECORD_COUNT.fieldId());
      projectedIds.add(TrackedFile.CONTENT_TYPE.fieldId());
      if (hasPartitionFilter()) {
        projectedIds.add(TrackedFile.SPEC_ID.fieldId());
        projectedIds.add(TrackedFile.PARTITION_ID);
      }

      return TypeUtil.select(fullSchema, projectedIds);
    }

    private Schema projection(Schema fullSchema) {
      if (columns != null) {
        return caseSensitive
            ? fullSchema.select(columns)
            : fullSchema.caseInsensitiveSelect(columns);
      }

      return fileProjection;
    }
  }
}
