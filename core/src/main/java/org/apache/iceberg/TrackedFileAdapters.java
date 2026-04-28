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
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.LocationUtil;

/**
 * Adapts {@link TrackedFile} entries to the {@link DataFile} and {@link DeleteFile} APIs.
 *
 * <p>V4 colocates deletion vectors with data file entries in {@link TrackedFile}. Rather than
 * extending {@link DataFile} with deletion vector fields, DVs are extracted as separate {@link
 * DeleteFile} objects via {@link #asDVDeleteFile(TrackedFile, PartitionSpec)}. This matches the v3
 * convention where DVs are tracked as {@link DeleteFile} entries in delete manifests and keeps the
 * existing {@link FileScanTask} contract ({@code file()} + {@code deletes()}) unchanged.
 */
class TrackedFileAdapters {

  private TrackedFileAdapters() {}

  /**
   * Creates a {@link GenericDataFile} from a TrackedFile using the reader constructor so that
   * SupportsIndexProjection is correctly initialized for metadata table reads.
   */
  static GenericDataFile asGenericDataFile(TrackedFile file, PartitionSpec spec) {
    Preconditions.checkState(
        file.contentType() == FileContent.DATA,
        "Cannot convert tracked file to DataFile: content type is %s, not DATA",
        file.contentType());

    Types.StructType partitionType = spec != null ? spec.rawPartitionType() : Types.StructType.of();
    Types.StructType projection = DataFile.getType(partitionType);

    // use the reader constructor for correct SupportsIndexProjection mapping
    GenericDataFile dataFile = new GenericDataFile(projection);

    // populate using DataFile.getType() positions (same as BaseFile internal positions)
    // 0=content, 1=file_path, 2=file_format, 3=spec_id, 4=partition, 5=record_count,
    // 6=file_size, 7=column_sizes, 8=value_counts, 9=null_value_counts, 10=nan_value_counts,
    // 11=lower_bounds, 12=upper_bounds, 13=key_metadata, 14=split_offsets, 15=equality_ids,
    // 16=sort_order_id, 17=first_row_id
    Tracking tracking = file.tracking();
    dataFile.set(0, file.contentType().id());
    dataFile.set(1, file.location());
    dataFile.set(2, file.fileFormat() != null ? file.fileFormat().toString() : null);
    dataFile.set(3, file.specId() != null ? file.specId() : 0);
    if (!partitionType.fields().isEmpty()) {
      dataFile.set(4, extractPartition(file, spec));
    }

    dataFile.set(5, file.recordCount());
    dataFile.set(6, file.fileSizeInBytes());
    // 7: column_sizes - null default
    dataFile.set(8, valueCounts(file.contentStats()));
    dataFile.set(9, nullValueCounts(file.contentStats()));
    dataFile.set(10, nanValueCounts(file.contentStats()));
    dataFile.set(11, lowerBounds(file.contentStats()));
    dataFile.set(12, upperBounds(file.contentStats()));
    dataFile.set(13, file.keyMetadata());
    dataFile.set(14, file.splitOffsets());
    // 15: equality_ids - null default
    dataFile.set(16, file.sortOrderId());
    dataFile.set(17, tracking != null ? tracking.firstRowId() : null);

    return dataFile;
  }

  static DataFile asDataFile(TrackedFile file, PartitionSpec spec) {
    return asDataFile(file, spec, null);
  }

  static DataFile asDataFile(TrackedFile file, PartitionSpec spec, String tableLocation) {
    Preconditions.checkState(
        file.contentType() == FileContent.DATA,
        "Cannot convert tracked file to DataFile: content type is %s, not DATA",
        file.contentType());
    return new TrackedDataFile(file, spec, tableLocation);
  }

  static DeleteFile asDVDeleteFile(TrackedFile file, PartitionSpec spec) {
    Preconditions.checkState(
        file.contentType() == FileContent.DATA,
        "Cannot extract DV from tracked file: content type is %s, not DATA",
        file.contentType());
    Preconditions.checkState(
        file.deletionVector() != null, "Cannot extract DV from tracked file: no deletion vector");
    return new TrackedDVDeleteFile(file, spec);
  }

  static DeleteFile asEqualityDeleteFile(TrackedFile file, PartitionSpec spec) {
    return asEqualityDeleteFile(file, spec, null);
  }

  static DeleteFile asEqualityDeleteFile(
      TrackedFile file, PartitionSpec spec, String tableLocation) {
    Preconditions.checkState(
        file.contentType() == FileContent.EQUALITY_DELETES
            || file.contentType() == FileContent.POSITION_DELETES,
        "Cannot convert tracked file to DeleteFile: content type is %s",
        file.contentType());
    return new TrackedDeleteFile(file, spec, tableLocation);
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
    if (spec == null || spec.isUnpartitioned()) {
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
  private static class TrackedDataFile implements DataFile, StructLike, java.io.Serializable {
    // BaseFile StructLike field count (content through fileOrdinal)
    private static final int STRUCT_SIZE = 22;

    private final TrackedFile file;
    private final Tracking tracking;
    private final PartitionSpec spec;
    private final String tableLocation;

    private TrackedDataFile(TrackedFile file, PartitionSpec spec, String tableLocation) {
      this.file = file;
      this.tracking = file.tracking();
      this.spec = spec;
      this.tableLocation = tableLocation;
    }

    @Override
    public int size() {
      return STRUCT_SIZE;
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("TrackedDataFile is read-only");
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(getByPos(pos));
    }

    // positions match BaseFile / DataFile.getType() field order
    private Object getByPos(int pos) {
      switch (pos) {
        case 0:
          return content().id();
        case 1:
          return location();
        case 2:
          return format() != null ? format().toString() : null;
        case 3:
          return specId();
        case 4:
          return partition();
        case 5:
          return recordCount();
        case 6:
          return fileSizeInBytes();
        case 7:
          return columnSizes();
        case 8:
          return valueCounts();
        case 9:
          return nullValueCounts();
        case 10:
          return nanValueCounts();
        case 11:
          return lowerBounds();
        case 12:
          return upperBounds();
        case 13:
          return keyMetadata();
        case 14:
          return splitOffsets();
        case 15:
          return equalityFieldIds();
        case 16:
          return sortOrderId();
        case 17:
          return firstRowId();
        case 18:
          return null; // referencedDataFile
        case 19:
          return null; // contentOffset
        case 20:
          return null; // contentSizeInBytes
        case 21:
          return pos();
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
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
      return LocationUtil.resolve(file.location(), tableLocation);
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
      return new TrackedDataFile(file.copy(), spec, tableLocation);
    }

    @Override
    public DataFile copy(boolean withStats) {
      return withStats ? copy() : copyWithoutStats();
    }

    @Override
    public DataFile copyWithoutStats() {
      return new TrackedDataFile(file.copyWithoutStats(), spec, tableLocation);
    }

    @Override
    public DataFile copyWithStats(Set<Integer> requestedColumnIds) {
      return new TrackedDataFile(file.copyWithStats(requestedColumnIds), spec, tableLocation);
    }
  }

  /** Adapts a TrackedFile EQUALITY_DELETES entry to the {@link DeleteFile} interface. */
  private static class TrackedDeleteFile implements DeleteFile, java.io.Serializable {
    private final TrackedFile file;
    private final Tracking tracking;
    private final PartitionSpec spec;
    private final String tableLocation;

    private TrackedDeleteFile(TrackedFile file, PartitionSpec spec, String tableLocation) {
      this.file = file;
      this.tracking = file.tracking();
      this.spec = spec;
      this.tableLocation = tableLocation;
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
      return file.contentType();
    }

    @SuppressWarnings("deprecation")
    @Override
    public CharSequence path() {
      return LocationUtil.resolve(file.location(), tableLocation);
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
      return new TrackedDeleteFile(file.copy(), spec, tableLocation);
    }

    @Override
    public DeleteFile copy(boolean withStats) {
      return withStats ? copy() : copyWithoutStats();
    }

    @Override
    public DeleteFile copyWithoutStats() {
      return new TrackedDeleteFile(file.copyWithoutStats(), spec, tableLocation);
    }

    @Override
    public DeleteFile copyWithStats(Set<Integer> requestedColumnIds) {
      return new TrackedDeleteFile(file.copyWithStats(requestedColumnIds), spec, tableLocation);
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
      return file.specId() != null ? file.specId() : 0;
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
