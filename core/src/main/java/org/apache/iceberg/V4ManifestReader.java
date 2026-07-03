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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;

/** Reader that reads a v4+ manifest file as {@link TrackedFile}s. */
class V4ManifestReader extends CloseableGroup implements CloseableIterable<TrackedFile> {
  // Tracking fields read on the scan path. Omits the change-tracking fields (dv_snapshot_id,
  // deleted_positions, replaced_positions) that a scan does not need. row_position backs
  // Tracking.manifestPos.
  private static final Types.StructType SCAN_TRACKING =
      Types.StructType.of(
          Tracking.STATUS,
          Tracking.SNAPSHOT_ID,
          Tracking.SEQUENCE_NUMBER,
          Tracking.FILE_SEQUENCE_NUMBER,
          Tracking.FIRST_ROW_ID,
          MetadataColumns.ROW_POSITION);

  private final InputFile file;
  private final Types.StructType partitionType;
  private final Schema fileProjection;
  private final ScanMetrics scanMetrics;

  // partition pruning state, keyed by spec ID; empty when no filtering is required
  private final Map<Integer, Evaluator> partitionEvaluators;
  private final Map<Integer, StructProjection> partitionProjections;

  private V4ManifestReader(
      InputFile file,
      Types.StructType partitionType,
      Map<Integer, Evaluator> partitionEvaluators,
      Map<Integer, StructProjection> partitionProjections,
      Schema fileProjection,
      ScanMetrics scanMetrics) {
    this.file = file;
    this.partitionType = partitionType;
    this.partitionEvaluators = partitionEvaluators;
    this.partitionProjections = partitionProjections;
    this.fileProjection = fileProjection;
    this.scanMetrics = scanMetrics;
  }

  static Builder builder(
      InputFile file, Schema tableSchema, Map<Integer, PartitionSpec> specsById) {
    return new Builder(file, tableSchema, specsById);
  }

  /** Returns all tracked files in this manifest, regardless of status. */
  CloseableIterable<TrackedFile> allFiles() {
    return files(false /* all files */);
  }

  /** Returns tracked files whose tracking {@link Tracking#isLive() is live}. */
  CloseableIterable<TrackedFile> liveFiles() {
    return files(true /* only live files */);
  }

  /** Returns live tracked files. Makes defensive copies before returning. */
  @Override
  public CloseableIterator<TrackedFile> iterator() {
    return CloseableIterable.transform(liveFiles(), TrackedFile::copy).iterator();
  }

  private CloseableIterable<TrackedFile> files(boolean onlyLive) {
    CloseableIterable<TrackedFile> entries = CloseableIterable.transform(open(), this::prepare);
    if (!partitionEvaluators.isEmpty()) {
      entries = CloseableIterable.filter(entries, this::matchesPartition);
    }

    if (onlyLive) {
      entries = CloseableIterable.filter(entries, entry -> entry.tracking().isLive());
    }

    return entries;
  }

  private boolean matchesPartition(TrackedFile trackedFile) {
    FileContent content = trackedFile.contentType();
    if (content == FileContent.DATA_MANIFEST || content == FileContent.DELETE_MANIFEST) {
      // manifest references are expanded later and are not pruned by the partition filter
      return true;
    }

    Integer specId = trackedFile.specId();
    Evaluator evaluator = specId != null ? partitionEvaluators.get(specId) : null;
    StructProjection projection = specId != null ? partitionProjections.get(specId) : null;
    Preconditions.checkState(
        evaluator != null && projection != null,
        "Cannot apply partition filter: spec ID %s is not one of the known specs %s in manifest %s",
        specId,
        partitionEvaluators.keySet(),
        file.location());

    boolean matches = evaluator.eval(projection.wrap(trackedFile.partition()));
    if (!matches) {
      if (content == FileContent.DATA) {
        scanMetrics.skippedDataFiles().increment();
      } else {
        scanMetrics.skippedDeleteFiles().increment();
      }
    }

    return matches;
  }

  private CloseableIterable<TrackedFile> open() {
    FileFormat format = FileFormat.fromFileName(file.location());
    Preconditions.checkArgument(
        format != null, "Cannot determine format of manifest: %s", file.location());

    CloseableIterable<TrackedFile> reader =
        InternalData.read(format, file)
            .project(readSchema())
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
    // manifestLocation is not stored in the manifest; the reader fills it from the file location.
    // manifestPos is filled from ROW_POSITION while reading the tracking struct.
    if (tracking instanceof TrackingStruct) {
      ((TrackingStruct) tracking).setManifestLocation(file.location());
    }

    return trackedFile;
  }

  private Schema readSchema() {
    Types.StructType fullType =
        TrackedFile.schemaWithContentStats(partitionType, Types.StructType.of());
    boolean unpartitioned = partitionType.fields().isEmpty();

    Set<Integer> projectedIds = null;
    if (fileProjection != null) {
      projectedIds =
          fileProjection.asStruct().fields().stream()
              .map(Types.NestedField::fieldId)
              .collect(Collectors.toCollection(Sets::newHashSet));

      // Always project tracking and content type. status drives live-file filtering, and content
      // type distinguishes data, delete, and manifest-reference entries.
      projectedIds.add(TrackedFile.TRACKING.fieldId());
      projectedIds.add(TrackedFile.CONTENT_TYPE.fieldId());

      // Force-project the remaining fields the partition filter reads, regardless of caller
      // projection.
      if (!partitionEvaluators.isEmpty()) {
        projectedIds.add(TrackedFile.SPEC_ID.fieldId());
        projectedIds.add(TrackedFile.PARTITION_ID);
      }
    }

    List<Types.NestedField> fields = Lists.newArrayList();
    for (Types.NestedField field : fullType.fields()) {
      if (projectedIds != null && !projectedIds.contains(field.fieldId())) {
        continue;
      }

      if (field.fieldId() == TrackedFile.TRACKING.fieldId()) {
        fields.add(
            Types.NestedField.required(
                TrackedFile.TRACKING.fieldId(),
                TrackedFile.TRACKING.name(),
                SCAN_TRACKING,
                TrackedFile.TRACKING.doc()));
      } else if (field.fieldId() == TrackedFile.CONTENT_STATS_ID) {
        // content_stats are not projected yet
      } else if (field.fieldId() == TrackedFile.PARTITION_ID && unpartitioned) {
        // unpartitioned manifests omit the partition field
      } else {
        fields.add(field);
      }
    }

    return new Schema(fields);
  }

  static class Builder {
    private final InputFile file;
    private final Types.StructType partitionType;
    private final Map<Integer, PartitionSpec> specsById;
    private Expression rowFilter = Expressions.alwaysTrue();
    private boolean caseSensitive = true;
    private Schema fileProjection = null;
    private ScanMetrics scanMetrics = ScanMetrics.noop();

    private Builder(InputFile file, Schema tableSchema, Map<Integer, PartitionSpec> specsById) {
      this.file = file;
      this.partitionType = Partitioning.partitionType(tableSchema, specsById.values());
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

    Builder project(Schema newFileProjection) {
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
      if (rowFilter != Expressions.alwaysTrue() && !partitionType.fields().isEmpty()) {
        for (PartitionSpec spec : specsById.values()) {
          Expression partFilter = Projections.inclusive(spec, caseSensitive).project(rowFilter);
          partitionEvaluators.put(
              spec.specId(), new Evaluator(spec.partitionType(), partFilter, caseSensitive));
          partitionProjections.put(
              spec.specId(), StructProjection.create(partitionType, spec.partitionType()));
        }
      }

      return new V4ManifestReader(
          file,
          partitionType,
          partitionEvaluators,
          partitionProjections,
          fileProjection,
          scanMetrics);
    }
  }
}
