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
import java.util.Collections;
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
 * <p>V4 colocates deletion vectors with data file entries in {@link TrackedFile}. Rather than
 * extending {@link DataFile} with deletion vector fields, DVs are extracted as separate {@link
 * DeleteFile} objects via {@link #asDVDeleteFile(TrackedFile, Map)}. This matches the v3 convention
 * where DVs are tracked as {@link DeleteFile} entries in delete manifests and keeps the existing
 * {@link FileScanTask} contract ({@code file()} + {@code deletes()}) unchanged.
 */
class TrackedFileAdapters {

  private TrackedFileAdapters() {}

  static DataFile asDataFile(TrackedFile file, Map<Integer, PartitionSpec> specsById) {
    Preconditions.checkState(
        file.contentType() == FileContent.DATA,
        "Cannot convert tracked file to DataFile: content type is %s, not DATA",
        file.contentType());
    return new TrackedDataFile(file, resolveSpec(file, specsById));
  }

  static DeleteFile asDVDeleteFile(TrackedFile file, Map<Integer, PartitionSpec> specsById) {
    Preconditions.checkState(
        file.contentType() == FileContent.DATA,
        "Cannot extract DV from tracked file: content type is %s, not DATA",
        file.contentType());
    Preconditions.checkState(
        file.deletionVector() != null, "Cannot extract DV from tracked file: no deletion vector");
    return new TrackedDVDeleteFile(file, resolveSpec(file, specsById));
  }

  static DeleteFile asEqualityDeleteFile(TrackedFile file, Map<Integer, PartitionSpec> specsById) {
    Preconditions.checkState(
        file.contentType() == FileContent.EQUALITY_DELETES,
        "Cannot convert tracked file to DeleteFile: content type is %s, not EQUALITY_DELETES",
        file.contentType());
    return new TrackedDeleteFile(file, resolveSpec(file, specsById));
  }

  private static PartitionSpec resolveSpec(
      TrackedFile file, Map<Integer, PartitionSpec> specsById) {
    Integer specId = file.specId();
    if (specId != null) {
      PartitionSpec spec = specsById.get(specId);
      Preconditions.checkArgument(
          spec != null, "Cannot find partition spec for spec ID: %s", specId);
      return spec;
    }

    return PartitionSpec.unpartitioned();
  }

  // TODO: TrackedFile will likely get an explicit partition tuple field (using a union partition
  //  schema), replacing this transform-based derivation. Once that lands, this method should be
  //  removed and the adapter should read the tuple directly.
  //
  // This derives partition values by applying the partition transform to the lower bound of the
  // source column stats. This is correct because each data file belongs to exactly one partition,
  // so lower == upper for partition source columns. For non-identity transforms (bucket, truncate),
  // the transform of the lower bound produces the correct partition value under this invariant.
  @SuppressWarnings({"unchecked", "rawtypes"})
  static StructLike extractPartition(TrackedFile file, PartitionSpec spec) {
    if (spec.isUnpartitioned()) {
      return BaseFile.EMPTY_PARTITION_DATA;
    }

    ContentStats stats = file.contentStats();
    if (stats == null) {
      return new PartitionData(spec.partitionType());
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

    return result.isEmpty() ? null : Collections.unmodifiableMap(result);
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

    return result.isEmpty() ? null : Collections.unmodifiableMap(result);
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

    return result.isEmpty() ? null : Collections.unmodifiableMap(result);
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

    return result.isEmpty() ? null : Collections.unmodifiableMap(result);
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

    return result.isEmpty() ? null : Collections.unmodifiableMap(result);
  }

  /**
   * Shared base for adapters that delegate to a {@link TrackedFile} for content file fields.
   *
   * <p>Subclasses provide {@code content()}, {@code firstRowId()}, {@code equalityFieldIds()}, and
   * the copy methods.
   */
  private abstract static class AbstractTrackedContentFile<F extends ContentFile<F>>
      implements ContentFile<F> {
    protected final TrackedFile file;
    protected final Tracking tracking;
    protected final PartitionSpec spec;

    private AbstractTrackedContentFile(TrackedFile file, PartitionSpec spec) {
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
      return spec.specId();
    }

    @SuppressWarnings("deprecation")
    @Override
    public CharSequence path() {
      return file.location();
    }

    @Override
    public String location() {
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
    public ByteBuffer keyMetadata() {
      return file.keyMetadata();
    }

    @Override
    public List<Long> splitOffsets() {
      return file.splitOffsets();
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
  }

  /** Adapts a TrackedFile DATA entry to the {@link DataFile} interface. */
  private static class TrackedDataFile extends AbstractTrackedContentFile<DataFile>
      implements DataFile {
    private TrackedDataFile(TrackedFile file, PartitionSpec spec) {
      super(file, spec);
    }

    @Override
    public FileContent content() {
      return FileContent.DATA;
    }

    @Override
    public Long firstRowId() {
      return tracking != null ? tracking.firstRowId() : null;
    }

    @Override
    public List<Integer> equalityFieldIds() {
      return null;
    }

    @Override
    public DataFile copy() {
      return new TrackedDataFile(file.copy(), spec);
    }

    @Override
    public DataFile copy(boolean withStats) {
      return withStats ? copy() : copyWithoutStats();
    }

    @Override
    public DataFile copyWithoutStats() {
      return new TrackedDataFile(file.copyWithoutStats(), spec);
    }

    @Override
    public DataFile copyWithStats(Set<Integer> requestedColumnIds) {
      return new TrackedDataFile(file.copyWithStats(requestedColumnIds), spec);
    }
  }

  /** Adapts a TrackedFile EQUALITY_DELETES entry to the {@link DeleteFile} interface. */
  private static class TrackedDeleteFile extends AbstractTrackedContentFile<DeleteFile>
      implements DeleteFile {
    private TrackedDeleteFile(TrackedFile file, PartitionSpec spec) {
      super(file, spec);
    }

    @Override
    public FileContent content() {
      return FileContent.EQUALITY_DELETES;
    }

    @Override
    public Long firstRowId() {
      return null;
    }

    @Override
    public List<Integer> equalityFieldIds() {
      return file.equalityIds();
    }

    @Override
    public DeleteFile copy() {
      return new TrackedDeleteFile(file.copy(), spec);
    }

    @Override
    public DeleteFile copy(boolean withStats) {
      return withStats ? copy() : copyWithoutStats();
    }

    @Override
    public DeleteFile copyWithoutStats() {
      return new TrackedDeleteFile(file.copyWithoutStats(), spec);
    }

    @Override
    public DeleteFile copyWithStats(Set<Integer> requestedColumnIds) {
      return new TrackedDeleteFile(file.copyWithStats(requestedColumnIds), spec);
    }
  }

  /**
   * Adapts the deletion vector from a TrackedFile DATA entry to the {@link DeleteFile} interface.
   *
   * <p>The DV blob metadata is mapped to the DeleteFile DV fields: {@link
   * DeleteFile#referencedDataFile()} is the data file location, and {@link
   * DeleteFile#contentOffset()} / {@link DeleteFile#contentSizeInBytes()} point to the blob within
   * the Puffin file.
   */
  private static class TrackedDVDeleteFile implements DeleteFile {
    private final TrackedFile file;
    private final DeletionVector dv;
    private final Tracking tracking;
    private final PartitionSpec spec;

    private TrackedDVDeleteFile(TrackedFile file, PartitionSpec spec) {
      Preconditions.checkArgument(
          file.deletionVector() != null, "Cannot create DV delete file: no deletion vector");
      this.file = file;
      this.dv = file.deletionVector();
      this.tracking = file.tracking();
      this.spec = spec;
    }

    @Override
    public Long pos() {
      return tracking != null ? tracking.manifestPos() : null;
    }

    @Override
    public int specId() {
      return spec.specId();
    }

    @Override
    public FileContent content() {
      return FileContent.POSITION_DELETES;
    }

    @SuppressWarnings("deprecation")
    @Override
    public CharSequence path() {
      return dv.location();
    }

    @Override
    public String location() {
      return dv.location();
    }

    @Override
    public FileFormat format() {
      return FileFormat.PUFFIN;
    }

    @Override
    public StructLike partition() {
      return extractPartition(file, spec);
    }

    @Override
    public long recordCount() {
      return dv.cardinality();
    }

    // Returns the DV blob size, not the full Puffin file size. The DeletionVector metadata does not
    // include the Puffin file size, so this is the best approximation available. Space accounting
    // that sums fileSizeInBytes() was already imprecise in v3 (multiple DVs sharing a Puffin file
    // each reported the full file size).
    @Override
    public long fileSizeInBytes() {
      return dv.sizeInBytes();
    }

    // Position deletes are required to be sorted by file and position, not a table order, and
    // should set sort order id to null
    @Override
    public Integer sortOrderId() {
      return null;
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
      return null;
    }

    @Override
    public ByteBuffer keyMetadata() {
      return null;
    }

    @Override
    public List<Long> splitOffsets() {
      return null;
    }

    @Override
    public List<Integer> equalityFieldIds() {
      return null;
    }

    @Override
    public String referencedDataFile() {
      return file.location();
    }

    @Override
    public Long contentOffset() {
      return dv.offset();
    }

    @Override
    public Long contentSizeInBytes() {
      return dv.sizeInBytes();
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
      return null;
    }

    @Override
    public Map<Integer, Long> nullValueCounts() {
      return null;
    }

    @Override
    public Map<Integer, Long> nanValueCounts() {
      return null;
    }

    @Override
    public Map<Integer, ByteBuffer> lowerBounds() {
      return null;
    }

    @Override
    public Map<Integer, ByteBuffer> upperBounds() {
      return null;
    }

    @Override
    public DeleteFile copy() {
      return new TrackedDVDeleteFile(file.copy(), spec);
    }

    @Override
    public DeleteFile copy(boolean withStats) {
      return copy();
    }

    @Override
    public DeleteFile copyWithoutStats() {
      return copy();
    }

    @Override
    public DeleteFile copyWithStats(Set<Integer> requestedColumnIds) {
      return copy();
    }
  }
}
