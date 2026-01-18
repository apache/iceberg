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
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.roaringbitmap.RoaringBitmap;

/**
 * Reader for V4 manifest files containing TrackedFile entries.
 *
 * <p>Supports reading both root manifests and leaf manifests. Returns TrackedFile entries which can
 * represent data files, delete files, or manifest references.
 *
 * <p>Projection capabilities:
 *
 * <ul>
 *   <li>By default, all columns are projected
 *   <li>Use {@link #filterData(Expression)} for automatic filter-based stats projection
 *   <li>Use {@link #projectStats(Set)} for custom stats field projection (unioned with
 *       filter-based)
 *   <li>Use {@link #project(Schema)} for fine-grained projection in use cases like metadata table
 *       scans
 * </ul>
 */
class V4ManifestReader extends CloseableGroup implements CloseableIterable<TrackedFile> {

  private final InputFile file;
  private final FileFormat format;
  private final InheritableTrackedMetadata inheritableMetadata;
  private final Long manifestFirstRowId;

  private ByteBuffer manifestDV = null;

  @SuppressWarnings("UnusedVariable")
  private Expression filter = Expressions.alwaysTrue();

  @SuppressWarnings("UnusedVariable")
  private Set<Integer> statsFieldIds = null;

  private Schema customProjection = null;

  protected V4ManifestReader(
      InputFile file,
      FileFormat format,
      InheritableTrackedMetadata inheritableMetadata,
      Long manifestFirstRowId) {
    this.file = file;
    this.format = format;
    this.inheritableMetadata = inheritableMetadata;
    this.manifestFirstRowId = manifestFirstRowId;
  }

  /**
   * Sets the manifest deletion vector to filter entries.
   *
   * @param dv serialized deletion vector of deleted entry positions, or null for no filtering
   */
  public V4ManifestReader withManifestDV(ByteBuffer dv) {
    this.manifestDV = dv;
    return this;
  }

  /**
   * Sets a filter expression for automatic stats projection.
   *
   * <p>Stats for columns referenced in the filter will be projected automatically. This is more
   * efficient than projecting all stats when only a subset is needed for filtering.
   *
   * @param expr filter expression to push down
   * @return this reader for method chaining
   */
  public V4ManifestReader filterData(Expression expr) {
    this.filter = expr;
    return this;
  }

  /**
   * Sets custom stats field IDs to project.
   *
   * <p>These field IDs are unioned with any fields extracted from {@link #filterData(Expression)}.
   * Use this when you need stats for specific columns beyond what the filter requires.
   *
   * @param fieldIds table schema field IDs for which to project stats
   * @return this reader for method chaining
   */
  public V4ManifestReader projectStats(Set<Integer> fieldIds) {
    this.statsFieldIds = fieldIds;
    return this;
  }

  /**
   * Sets a custom projection schema for fine-grained control.
   *
   * <p>Use this for metadata table scans and other custom situations (like {@code
   * ManifestFiles.readPaths}) that need specific column projection. When set, this takes precedence
   * over filter-based and custom stats projection.
   *
   * <p>Note: The tracking_info struct is always projected regardless of the custom schema to ensure
   * correctness.
   *
   * @param projection custom schema to project
   * @return this reader for method chaining
   */
  public V4ManifestReader project(Schema projection) {
    this.customProjection = projection;
    return this;
  }

  public CloseableIterable<TrackedFile> allFiles() {
    return open();
  }

  public CloseableIterable<TrackedFile> liveFiles() {
    return filterLiveFiles(open());
  }

  /**
   * Builds the projection schema based on configured projection options.
   *
   * <p>If a custom projection is set, it is used with tracking_info always included to ensure
   * correctness. Otherwise, projects all columns by default. Filter-based stats projection will be
   * implemented when content stats structure is finalized.
   *
   * <p>Always includes:
   *
   * <ul>
   *   <li>{@link TrackedFile#TRACKING_INFO} - required for correct first_row_id handling
   *   <li>{@link MetadataColumns#ROW_POSITION} - for safe position tracking (handles row group
   *       skipping)
   * </ul>
   */
  private Schema buildProjection() {
    List<Types.NestedField> fields = Lists.newArrayList();

    if (customProjection != null) {
      fields.addAll(customProjection.asStruct().fields());

      // Always project tracking_info to ensure first_row_id and status are available.
      // This prevents bugs when reading via metadata tables with partial projection.
      if (customProjection.findField(TrackedFile.TRACKING_INFO.fieldId()) == null) {
        fields.add(TrackedFile.TRACKING_INFO);
      }

      // Always add ROW_POSITION for safe position tracking (handles row group skipping)
      if (customProjection.findField(MetadataColumns.ROW_POSITION.fieldId()) == null) {
        fields.add(MetadataColumns.ROW_POSITION);
      }
    } else {
      // Default: project all columns including ROW_POSITION
      // TODO: When content stats structure is implemented, union filter-based field IDs
      // with statsFieldIds to project only required stats columns
      fields.addAll(TrackedFileStruct.BASE_TYPE.fields());
    }

    return new Schema(fields);
  }

  private CloseableIterable<TrackedFile> open() {
    Schema projection = buildProjection();

    CloseableIterable<TrackedFileStruct> entries =
        InternalData.read(format, file)
            .project(projection)
            .setRootType(TrackedFileStruct.class)
            .build();

    addCloseable(entries);

    CloseableIterable<TrackedFileStruct> transformed =
        CloseableIterable.transform(entries, inheritableMetadata::apply);

    transformed = CloseableIterable.transform(transformed, rowIdAssigner(manifestFirstRowId));

    if (manifestDV != null) {
      RoaringBitmap deletedPositions = deserializeManifestDV(manifestDV);
      transformed =
          CloseableIterable.filter(
              transformed,
              entry -> {
                Long pos = entry.pos();
                Preconditions.checkNotNull(
                    pos, "Position should not be null when applying manifest deletion vector");
                return !deletedPositions.contains(pos.intValue());
              });
    }

    return CloseableIterable.transform(transformed, e -> e);
  }

  private static RoaringBitmap deserializeManifestDV(ByteBuffer dv) {
    byte[] bytes = ByteBuffers.toByteArray(dv);

    RoaringBitmap deletedPositions = new RoaringBitmap();
    try {
      deletedPositions.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to deserialize manifest DV", e);
    }

    return deletedPositions;
  }

  private CloseableIterable<TrackedFile> filterLiveFiles(CloseableIterable<TrackedFile> files) {
    return CloseableIterable.filter(
        files,
        entry -> {
          TrackingInfo tracking = entry.trackingInfo();
          Preconditions.checkNotNull(
              tracking, "Tracking info should not be null for committed data files");
          return tracking.status() != TrackingInfo.Status.DELETED;
        });
  }

  private static Function<TrackedFileStruct, TrackedFileStruct> rowIdAssigner(Long firstRowId) {
    if (firstRowId == null) {
      // Explicitly clear first_row_id for v2->v3/v4 migration.
      // Passing null signals that all row IDs should be cleared, not preserved.
      return entry -> {
        if (entry.contentType() == FileContent.DATA) {
          // tracking_info is always projected and should never be null for manifest entries.
          // If null, fail fast with NPE rather than silently ignoring.
          TrackingInfo tracking = entry.trackingInfo();
          if (tracking.firstRowId() != null) {
            entry.ensureTrackingInfo().setFirstRowId(null);
          }
        }

        return entry;
      };
    }

    return new Function<>() {
      private long nextRowId = firstRowId;

      @Override
      public TrackedFileStruct apply(TrackedFileStruct entry) {
        if (entry.contentType() == FileContent.DATA) {
          // tracking_info is always projected and should never be null for manifest entries.
          // If null, fail fast with NPE rather than silently ignoring.
          TrackingInfo tracking = entry.trackingInfo();
          if (tracking.status() != TrackingInfo.Status.DELETED && tracking.firstRowId() == null) {
            entry.ensureTrackingInfo().setFirstRowId(nextRowId);
            nextRowId = nextRowId + entry.recordCount();
          }
        }

        return entry;
      }
    };
  }

  @Override
  public CloseableIterator<TrackedFile> iterator() {
    return CloseableIterable.transform(liveFiles(), TrackedFile::copy).iterator();
  }
}
