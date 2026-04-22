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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;

/**
 * Adapts {@link TrackedFile} entries to the {@link DataFile} and {@link DeleteFile} APIs.
 *
 * <p>Note: V4 colocates deletion vectors with data file entries in {@link TrackedFile}. This
 * adapter does not carry over {@link TrackedFile#deletionVector()} because {@link DataFile} has no
 * way to represent it. Once {@link DataFile} is extended with deletion vector support, this adapter
 * should be updated to include it.
 */
class TrackedFileAdapters {

  private TrackedFileAdapters() {}

  static DataFile asDataFile(TrackedFile file, PartitionSpec spec) {
    Preconditions.checkState(
        file.contentType() == FileContent.DATA,
        "Cannot convert tracked file to DataFile: content type is %s, not DATA",
        file.contentType());
    return new TrackedDataFile(file, spec);
  }

  static DeleteFile asDeleteFile(TrackedFile file, PartitionSpec spec) {
    Preconditions.checkState(
        file.contentType() == FileContent.EQUALITY_DELETES,
        "Cannot convert tracked file to DeleteFile: content type is %s, not EQUALITY_DELETES",
        file.contentType());
    return new TrackedDeleteFile(file, spec);
  }

  // TODO: TrackedFile will likely get an explicit partition tuple field (using a union partition
  //  schema), replacing this transform-based derivation. Once that lands, this method should be
  //  removed and the adapter should read the tuple directly.
  @SuppressWarnings({"unchecked", "rawtypes"})
  static StructLike extractPartition(TrackedFile file, PartitionSpec spec) {
    if (spec == null || spec.isUnpartitioned()) {
      return null;
    }

    ContentStats stats = file.contentStats();
    if (stats == null) {
      return null;
    }

    PartitionData partition = new PartitionData(spec.partitionType());

    for (int i = 0; i < spec.fields().size(); i += 1) {
      PartitionField field = spec.fields().get(i);

      if (field.transform().isVoid()) {
        partition.set(i, null);
        continue;
      }

      FieldStats<?> fieldStats = stats.statsFor(field.sourceId());
      if (fieldStats == null || fieldStats.lowerBound() == null) {
        partition.set(i, null);
        continue;
      }

      Type sourceType = spec.schema().findType(field.sourceId());
      Function boundTransform = field.transform().bind(sourceType);
      partition.set(i, boundTransform.apply(fieldStats.lowerBound()));
    }

    return partition;
  }

  static Map<Integer, Long> valueCounts(ContentStats stats) {
    if (stats == null) {
      return null;
    }

    Map<Integer, Long> result = Maps.newHashMap();
    for (FieldStats<?> fs : stats.fieldStats()) {
      if (fs != null && fs.valueCount() != null) {
        result.put(fs.fieldId(), fs.valueCount());
      }
    }

    return result.isEmpty() ? null : result;
  }

  static Map<Integer, Long> nullValueCounts(ContentStats stats) {
    if (stats == null) {
      return null;
    }

    Map<Integer, Long> result = Maps.newHashMap();
    for (FieldStats<?> fs : stats.fieldStats()) {
      if (fs != null && fs.nullValueCount() != null) {
        result.put(fs.fieldId(), fs.nullValueCount());
      }
    }

    return result.isEmpty() ? null : result;
  }

  static Map<Integer, Long> nanValueCounts(ContentStats stats) {
    if (stats == null) {
      return null;
    }

    Map<Integer, Long> result = Maps.newHashMap();
    for (FieldStats<?> fs : stats.fieldStats()) {
      if (fs != null && fs.nanValueCount() != null) {
        result.put(fs.fieldId(), fs.nanValueCount());
      }
    }

    return result.isEmpty() ? null : result;
  }

  static Map<Integer, ByteBuffer> lowerBounds(ContentStats stats) {
    if (stats == null) {
      return null;
    }

    Map<Integer, ByteBuffer> result = Maps.newHashMap();
    for (FieldStats<?> fs : stats.fieldStats()) {
      if (fs != null && fs.lowerBound() != null && fs.type() != null) {
        result.put(fs.fieldId(), Conversions.toByteBuffer(fs.type(), fs.lowerBound()));
      }
    }

    return result.isEmpty() ? null : result;
  }

  static Map<Integer, ByteBuffer> upperBounds(ContentStats stats) {
    if (stats == null) {
      return null;
    }

    Map<Integer, ByteBuffer> result = Maps.newHashMap();
    for (FieldStats<?> fs : stats.fieldStats()) {
      if (fs != null && fs.upperBound() != null && fs.type() != null) {
        result.put(fs.fieldId(), Conversions.toByteBuffer(fs.type(), fs.upperBound()));
      }
    }

    return result.isEmpty() ? null : result;
  }

  /** Adapts a TrackedFile DATA entry to the {@link DataFile} interface. */
  private static class TrackedDataFile implements DataFile {
    private final TrackedFile file;
    private final Tracking tracking;
    private final PartitionSpec spec;

    private TrackedDataFile(TrackedFile file, PartitionSpec spec) {
      this.file = file;
      this.tracking = file.tracking();
      this.spec = spec;
    }

    @Override
    public Long pos() {
      return tracking != null ? tracking.manifestPos() : null;
    }

    @Override
    public int specId() {
      // null specId in v4 means unpartitioned; default to 0 to match PartitionSpec.unpartitioned()
      return file.specId() != null ? file.specId() : 0;
    }

    @Override
    public FileContent content() {
      return FileContent.DATA;
    }

    @SuppressWarnings("deprecation")
    @Override
    public CharSequence path() {
      return file.location();
    }

    @Override
    public FileFormat format() {
      return file.fileFormat();
    }

    @Override
    public StructLike partition() {
      return extractPartition(file, spec);
    }

    @Override
    public long recordCount() {
      return file.recordCount();
    }

    @Override
    public long fileSizeInBytes() {
      return file.fileSizeInBytes();
    }

    @Override
    public Integer sortOrderId() {
      return file.sortOrderId();
    }

    @Override
    public Long dataSequenceNumber() {
      return tracking != null ? tracking.dataSequenceNumber() : null;
    }

    @Override
    public Long fileSequenceNumber() {
      return tracking != null ? tracking.fileSequenceNumber() : null;
    }

    @Override
    public Long firstRowId() {
      return tracking != null ? tracking.firstRowId() : null;
    }

    @Override
    public ByteBuffer keyMetadata() {
      return file.keyMetadata();
    }

    @Override
    public List<Long> splitOffsets() {
      return file.splitOffsets();
    }

    @Override
    public List<Integer> equalityFieldIds() {
      return null;
    }

    @Override
    public String manifestLocation() {
      return tracking != null ? tracking.manifestLocation() : null;
    }

    @Override
    public Map<Integer, Long> columnSizes() {
      return null;
    }

    @Override
    public Map<Integer, Long> valueCounts() {
      return TrackedFileAdapters.valueCounts(file.contentStats());
    }

    @Override
    public Map<Integer, Long> nullValueCounts() {
      return TrackedFileAdapters.nullValueCounts(file.contentStats());
    }

    @Override
    public Map<Integer, Long> nanValueCounts() {
      return TrackedFileAdapters.nanValueCounts(file.contentStats());
    }

    @Override
    public Map<Integer, ByteBuffer> lowerBounds() {
      return TrackedFileAdapters.lowerBounds(file.contentStats());
    }

    @Override
    public Map<Integer, ByteBuffer> upperBounds() {
      return TrackedFileAdapters.upperBounds(file.contentStats());
    }

    @Override
    public DataFile copy() {
      return this;
    }

    @Override
    public DataFile copy(boolean withStats) {
      return this;
    }

    @Override
    public DataFile copyWithoutStats() {
      return this;
    }

    @Override
    public DataFile copyWithStats(Set<Integer> requestedColumnIds) {
      return this;
    }
  }

  /** Adapts a TrackedFile EQUALITY_DELETES entry to the {@link DeleteFile} interface. */
  private static class TrackedDeleteFile implements DeleteFile {
    private final TrackedFile file;
    private final Tracking tracking;
    private final PartitionSpec spec;

    private TrackedDeleteFile(TrackedFile file, PartitionSpec spec) {
      this.file = file;
      this.tracking = file.tracking();
      this.spec = spec;
    }

    @Override
    public Long pos() {
      return tracking != null ? tracking.manifestPos() : null;
    }

    @Override
    public int specId() {
      // null specId in v4 means unpartitioned; default to 0 to match PartitionSpec.unpartitioned()
      return file.specId() != null ? file.specId() : 0;
    }

    @Override
    public FileContent content() {
      return FileContent.EQUALITY_DELETES;
    }

    @SuppressWarnings("deprecation")
    @Override
    public CharSequence path() {
      return file.location();
    }

    @Override
    public FileFormat format() {
      return file.fileFormat();
    }

    @Override
    public StructLike partition() {
      return extractPartition(file, spec);
    }

    @Override
    public long recordCount() {
      return file.recordCount();
    }

    @Override
    public long fileSizeInBytes() {
      return file.fileSizeInBytes();
    }

    @Override
    public Integer sortOrderId() {
      return file.sortOrderId();
    }

    @Override
    public Long dataSequenceNumber() {
      return tracking != null ? tracking.dataSequenceNumber() : null;
    }

    @Override
    public Long fileSequenceNumber() {
      return tracking != null ? tracking.fileSequenceNumber() : null;
    }

    @Override
    public Long firstRowId() {
      return tracking != null ? tracking.firstRowId() : null;
    }

    @Override
    public ByteBuffer keyMetadata() {
      return file.keyMetadata();
    }

    @Override
    public List<Long> splitOffsets() {
      return file.splitOffsets();
    }

    @Override
    public List<Integer> equalityFieldIds() {
      return file.equalityIds();
    }

    @Override
    public String manifestLocation() {
      return tracking != null ? tracking.manifestLocation() : null;
    }

    @Override
    public Map<Integer, Long> columnSizes() {
      return null;
    }

    @Override
    public Map<Integer, Long> valueCounts() {
      return TrackedFileAdapters.valueCounts(file.contentStats());
    }

    @Override
    public Map<Integer, Long> nullValueCounts() {
      return TrackedFileAdapters.nullValueCounts(file.contentStats());
    }

    @Override
    public Map<Integer, Long> nanValueCounts() {
      return TrackedFileAdapters.nanValueCounts(file.contentStats());
    }

    @Override
    public Map<Integer, ByteBuffer> lowerBounds() {
      return TrackedFileAdapters.lowerBounds(file.contentStats());
    }

    @Override
    public Map<Integer, ByteBuffer> upperBounds() {
      return TrackedFileAdapters.upperBounds(file.contentStats());
    }

    @Override
    public DeleteFile copy() {
      return this;
    }

    @Override
    public DeleteFile copy(boolean withStats) {
      return this;
    }

    @Override
    public DeleteFile copyWithoutStats() {
      return this;
    }

    @Override
    public DeleteFile copyWithStats(Set<Integer> requestedColumnIds) {
      return this;
    }
  }
}
