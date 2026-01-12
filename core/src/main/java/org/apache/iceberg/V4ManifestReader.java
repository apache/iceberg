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
import java.util.function.Function;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.ByteBuffers;
import org.roaringbitmap.RoaringBitmap;

/**
 * Reader for V4 manifest files containing TrackedFile entries.
 *
 * <p>Supports reading both root manifests and leaf manifests. Returns TrackedFile entries which can
 * represent data files, delete files, or manifest references.
 */
class V4ManifestReader extends CloseableGroup implements CloseableIterable<TrackedFile> {
  static final ImmutableList<String> ALL_COLUMNS = ImmutableList.of("*");

  private final InputFile file;
  private final FileFormat format;
  private final InheritableTrackedMetadata inheritableMetadata;
  private final Long manifestFirstRowId;

  private ByteBuffer manifestDV = null;

  @SuppressWarnings("UnusedVariable")
  private List<String> columns = ALL_COLUMNS;

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
   * Selects columns to read from the manifest. Currently unused - reads all columns.
   *
   * @param selectedColumns columns to read
   * @return this reader for method chaining
   */
  public V4ManifestReader select(List<String> selectedColumns) {
    this.columns = selectedColumns;
    return this;
  }

  public CloseableIterable<TrackedFile> allFiles() {
    return open();
  }

  public CloseableIterable<TrackedFile> liveFiles() {
    return filterLiveFiles(open());
  }

  private CloseableIterable<TrackedFile> open() {
    Schema projection = new Schema(TrackedFileStruct.BASE_TYPE.fields());

    CloseableIterable<TrackedFileStruct> entries =
        InternalData.read(format, file)
            .project(projection)
            .setRootType(TrackedFileStruct.class)
            .build();

    addCloseable(entries);

    CloseableIterable<TrackedFileStruct> transformed =
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
          return tracking == null || tracking.status() != TrackingInfo.Status.DELETED;
        });
  }

  private static Function<TrackedFileStruct, TrackedFileStruct> rowIdAssigner(Long firstRowId) {
    if (firstRowId == null) {
      return entry -> entry;
    }

    return new Function<>() {
      private long nextRowId = firstRowId;

      @Override
      public TrackedFileStruct apply(TrackedFileStruct entry) {
        if (entry.contentType() == FileContent.DATA) {
          TrackingInfo tracking = entry.trackingInfo();
          Preconditions.checkNotNull(
              tracking, "Tracking info should not be null for committed data files");

          if (tracking.status() != TrackingInfo.Status.DELETED && tracking.firstRowId() == null) {
            entry.setFirstRowId(nextRowId);
            nextRowId += entry.recordCount();
          }
        }
        return entry;
      }
    };
  }

  private CloseableIterable<TrackedFileStruct> assignPositions(
      CloseableIterable<TrackedFileStruct> entries) {
    return CloseableIterable.transform(
        entries,
        new Function<>() {
          private long position = 0;

          @Override
          public TrackedFileStruct apply(TrackedFileStruct entry) {
            entry.setPos(position);
            position++;
            return entry;
          }
        });
  }

  @Override
  public CloseableIterator<TrackedFile> iterator() {
    return CloseableIterable.transform(liveFiles(), TrackedFile::copy).iterator();
  }
}
