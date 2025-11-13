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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.roaringbitmap.RoaringBitmap;

/**
 * Reader for V4 manifest files containing TrackedFile entries.
 *
 * <p>Supports reading both root manifests and leaf manifests. Returns TrackedFile entries which can
 * represent data files, delete files, or manifest references. TODO: implement caching.
 */
class V4ManifestReader extends CloseableGroup implements CloseableIterable<TrackedFile<?>> {
  static final ImmutableList<String> ALL_COLUMNS = ImmutableList.of("*");

  private final InputFile file;
  private final InheritableTrackedMetadata inheritableMetadata;
  private final Long manifestFirstRowId;

  private Collection<String> columns = null;
  private TrackedFile<?> manifestDV = null;

  protected V4ManifestReader(
      InputFile file, InheritableTrackedMetadata inheritableMetadata, Long manifestFirstRowId) {
    this.file = file;
    this.inheritableMetadata = inheritableMetadata;
    this.manifestFirstRowId = manifestFirstRowId;
  }

  public V4ManifestReader select(Collection<String> newColumns) {
    this.columns = newColumns;
    return this;
  }

  public V4ManifestReader withDeletionVector(TrackedFile<?> dv) {
    this.manifestDV = dv;
    return this;
  }

  public CloseableIterable<TrackedFile<?>> entries() {
    return entries(false);
  }

  public CloseableIterable<TrackedFile<?>> liveEntries() {
    return entries(true);
  }

  private CloseableIterable<TrackedFile<?>> entries(boolean onlyLive) {
    CloseableIterable<TrackedFile<?>> entries = open(columns);
    return onlyLive ? filterLiveEntries(entries) : entries;
  }

  private CloseableIterable<TrackedFile<?>> open(Collection<String> cols) {
    Schema projection = buildProjection(cols);

    FileFormat format = FileFormat.fromFileName(file.location());

    CloseableIterable<GenericTrackedFile> entries =
        InternalData.read(format, file)
            .project(projection)
            .setRootType(GenericTrackedFile.class)
            .build();

    addCloseable(entries);

    CloseableIterable<TrackedFile<?>> transformed =
        CloseableIterable.transform(entries, inheritableMetadata::apply);

    transformed = CloseableIterable.transform(transformed, rowIdAssigner(manifestFirstRowId));

    transformed = assignPositions(transformed);

    if (manifestDV != null) {
      RoaringBitmap deletedPositions = deserializeManifestDV(manifestDV);
      transformed =
          CloseableIterable.filter(
              transformed,
              entry -> {
                Long pos = entry.pos();
                // positions are 0-based and should not exceed Integer.MAX_VALUE
                return pos == null || !deletedPositions.contains(pos.intValue());
              });
    }

    return transformed;
  }

  private static RoaringBitmap deserializeManifestDV(TrackedFile<?> manifestDV) {
    Preconditions.checkArgument(
        manifestDV.contentType() == FileContent.MANIFEST_DV,
        "Expected MANIFEST_DV, got: %s",
        manifestDV.contentType());

    DeletionVector dvInfo = manifestDV.deletionVector();
    Preconditions.checkNotNull(dvInfo, "MANIFEST_DV must have deletion_vector");

    Preconditions.checkNotNull(
        dvInfo.inlineContent(),
        "Manifest DV must have inline content (External not supported): %s",
        manifestDV.referencedFile());

    ByteBuffer buffer = dvInfo.inlineContent();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.asReadOnlyBuffer().get(bytes);

    RoaringBitmap bitmap = new RoaringBitmap();
    try {
      bitmap.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to deserialize Roaring bitmap from manifest DV");
    }
    return bitmap;
  }

  private Schema buildProjection(Collection<String> cols) {
    if (cols == null || cols.containsAll(ALL_COLUMNS)) {
      return new Schema(GenericTrackedFile.BASE_TYPE.fields());
    }

    List<Types.NestedField> fields = Lists.newArrayList();
    for (String column : cols) {
      Types.NestedField field = GenericTrackedFile.BASE_TYPE.field(column);
      if (field != null) {
        fields.add(field);
      }
    }

    return new Schema(fields);
  }

  private CloseableIterable<TrackedFile<?>> filterLiveEntries(
      CloseableIterable<TrackedFile<?>> entries) {
    return CloseableIterable.filter(
        entries,
        entry -> {
          TrackingInfo tracking = entry.trackingInfo();
          return tracking == null || tracking.status() != TrackingInfo.Status.DELETED;
        });
  }

  private static Function<TrackedFile<?>, TrackedFile<?>> rowIdAssigner(Long firstRowId) {
    if (firstRowId == null) {
      return entry -> entry;
    }

    return new Function<>() {
      private long nextRowId = firstRowId;

      @Override
      public TrackedFile<?> apply(TrackedFile<?> entry) {
        if (entry.contentType() == FileContent.DATA) {
          TrackingInfo tracking = entry.trackingInfo();
          if (tracking != null
              && tracking.status() != TrackingInfo.Status.DELETED
              && tracking.firstRowId() == null) {
            entry.setFirstRowId(nextRowId);
            nextRowId += entry.recordCount();
          }
        }
        return entry;
      }
    };
  }

  private CloseableIterable<TrackedFile<?>> assignPositions(
      CloseableIterable<TrackedFile<?>> entries) {
    return CloseableIterable.transform(
        entries,
        new Function<>() {
          private long position = 0;

          @Override
          public TrackedFile<?> apply(TrackedFile<?> entry) {
            entry.setPos(position++);
            return entry;
          }
        });
  }

  @Override
  @SuppressWarnings("unchecked")
  public CloseableIterator<TrackedFile<?>> iterator() {
    return (CloseableIterator<TrackedFile<?>>)
        (CloseableIterator<?>)
            CloseableIterable.transform(liveEntries(), TrackedFile::copy).iterator();
  }
}
