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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Adapts {@link TrackedFile} entries to the {@link DataFile} and {@link DeleteFile} APIs. */
class TrackedFileAdapters {

  private TrackedFileAdapters() {}

  static DataFile asDataFile(TrackedFile file, Map<Integer, PartitionSpec> specsById) {
    Preconditions.checkArgument(
        file.contentType() == FileContent.DATA,
        "Invalid content type for DataFile: %s",
        file.contentType());
    return new TrackedDataFile(file, resolveSpec(file, specsById));
  }

  static DeleteFile asDVDeleteFile(TrackedFile file, Map<Integer, PartitionSpec> specsById) {
    Preconditions.checkArgument(
        file.contentType() == FileContent.DATA,
        "Invalid content type for DV delete file: %s",
        file.contentType());
    return new TrackedDVDeleteFile(file, resolveSpec(file, specsById));
  }

  static DeleteFile asEqualityDeleteFile(TrackedFile file, Map<Integer, PartitionSpec> specsById) {
    Preconditions.checkArgument(
        file.contentType() == FileContent.EQUALITY_DELETES,
        "Invalid content type for equality delete file: %s",
        file.contentType());
    return new TrackedEqualityDeleteFile(file, resolveSpec(file, specsById));
  }

  /** Shared base for all tracked file adapters. */
  private abstract static class TrackedFileAdapter<F extends ContentFile<F>>
      implements ContentFile<F> {
    private final TrackedFile file;
    private final PartitionSpec spec;

    private TrackedFileAdapter(TrackedFile file, PartitionSpec spec) {
      Preconditions.checkArgument(
          file.specId() == null ? spec.isUnpartitioned() : file.specId() == spec.specId(),
          "File spec ID %s does not match partition spec %s",
          file.specId(),
          spec.specId());
      this.file = file;
      this.spec = spec;
    }

    protected TrackedFile file() {
      return file;
    }

    protected PartitionSpec spec() {
      return spec;
    }

    protected Tracking tracking() {
      return file.tracking();
    }

    @Override
    public Long pos() {
      Tracking tracking = tracking();
      return tracking != null ? tracking.manifestPos() : null;
    }

    @Override
    public String manifestLocation() {
      Tracking tracking = tracking();
      return tracking != null ? tracking.manifestLocation() : null;
    }

    @Override
    public int specId() {
      return spec.specId();
    }

    @Override
    public StructLike partition() {
      return file().partition() != null ? file().partition() : PartitionData.EMPTY;
    }

    @Override
    public Long dataSequenceNumber() {
      Tracking tracking = tracking();
      return tracking != null ? tracking.dataSequenceNumber() : null;
    }

    @Override
    public Long fileSequenceNumber() {
      Tracking tracking = tracking();
      return tracking != null ? tracking.fileSequenceNumber() : null;
    }
  }

  /**
   * Shared base for adapters where the {@link ContentFile} is the {@link TrackedFile} itself, as
   * opposed to {@link TrackedDVDeleteFile} which represents the tracked file's deletion vector.
   */
  private abstract static class TrackedContentFile<F extends ContentFile<F>>
      extends TrackedFileAdapter<F> {
    private TrackedContentFile(TrackedFile file, PartitionSpec spec) {
      super(file, spec);
    }

    @SuppressWarnings("deprecation")
    @Override
    public CharSequence path() {
      return file().location();
    }

    @Override
    public String location() {
      return file().location();
    }

    @Override
    public FileFormat format() {
      return file().fileFormat();
    }

    @Override
    public long recordCount() {
      return file().recordCount();
    }

    @Override
    public long fileSizeInBytes() {
      return file().fileSizeInBytes();
    }

    @Override
    public Integer sortOrderId() {
      return file().sortOrderId();
    }

    @Override
    public ByteBuffer keyMetadata() {
      return file().keyMetadata();
    }

    @Override
    public List<Long> splitOffsets() {
      return file().splitOffsets();
    }

    @Override
    public Map<Integer, Long> columnSizes() {
      return null;
    }

    @Override
    public Map<Integer, Long> valueCounts() {
      return MetricsUtil.valueCounts(file().contentStats());
    }

    @Override
    public Map<Integer, Long> nullValueCounts() {
      return MetricsUtil.nullValueCounts(file().contentStats());
    }

    @Override
    public Map<Integer, Long> nanValueCounts() {
      return MetricsUtil.nanValueCounts(file().contentStats());
    }

    @Override
    public Map<Integer, ByteBuffer> lowerBounds() {
      return MetricsUtil.lowerBounds(file().contentStats());
    }

    @Override
    public Map<Integer, ByteBuffer> upperBounds() {
      return MetricsUtil.upperBounds(file().contentStats());
    }
  }

  /** Adapts a TrackedFile DATA entry to the {@link DataFile} interface. */
  private static class TrackedDataFile extends TrackedContentFile<DataFile> implements DataFile {
    private TrackedDataFile(TrackedFile file, PartitionSpec spec) {
      super(file, spec);
    }

    @Override
    public FileContent content() {
      return FileContent.DATA;
    }

    @Override
    public Long firstRowId() {
      return tracking() != null ? tracking().firstRowId() : null;
    }

    @Override
    public DataFile copy() {
      return new TrackedDataFile(file().copy(), spec());
    }

    @Override
    public DataFile copy(boolean withStats) {
      return withStats ? copy() : copyWithoutStats();
    }

    @Override
    public DataFile copyWithoutStats() {
      return new TrackedDataFile(file().copyWithoutStats(), spec());
    }

    @Override
    public DataFile copyWithStats(Set<Integer> requestedColumnIds) {
      return new TrackedDataFile(file().copyWithStats(requestedColumnIds), spec());
    }
  }

  /** Adapts a TrackedFile EQUALITY_DELETES entry to the {@link DeleteFile} interface. */
  private static class TrackedEqualityDeleteFile extends TrackedContentFile<DeleteFile>
      implements DeleteFile {
    private TrackedEqualityDeleteFile(TrackedFile file, PartitionSpec spec) {
      super(file, spec);
    }

    @Override
    public FileContent content() {
      return FileContent.EQUALITY_DELETES;
    }

    @Override
    public List<Integer> equalityFieldIds() {
      return file().equalityIds();
    }

    @Override
    public DeleteFile copy() {
      return new TrackedEqualityDeleteFile(file().copy(), spec());
    }

    @Override
    public DeleteFile copy(boolean withStats) {
      return withStats ? copy() : copyWithoutStats();
    }

    @Override
    public DeleteFile copyWithoutStats() {
      return new TrackedEqualityDeleteFile(file().copyWithoutStats(), spec());
    }

    @Override
    public DeleteFile copyWithStats(Set<Integer> requestedColumnIds) {
      return new TrackedEqualityDeleteFile(file().copyWithStats(requestedColumnIds), spec());
    }
  }

  /**
   * Adapts the deletion vector from a TrackedFile DATA entry to the {@link DeleteFile} interface.
   */
  private static class TrackedDVDeleteFile extends TrackedFileAdapter<DeleteFile>
      implements DeleteFile {
    private final DeletionVector dv;

    private TrackedDVDeleteFile(TrackedFile file, PartitionSpec spec) {
      super(file, spec);
      Preconditions.checkArgument(
          file.deletionVector() != null, "Cannot create DV delete file: no deletion vector");
      this.dv = file.deletionVector();
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

    // From the spec: position deletes are required to be sorted by file and position, not a table
    // order, and should set sort order id to null
    @Override
    public Integer sortOrderId() {
      return null;
    }

    @Override
    public ByteBuffer keyMetadata() {
      return null;
    }

    @Override
    public List<Integer> equalityFieldIds() {
      return null;
    }

    @Override
    public String referencedDataFile() {
      return file().location();
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
      return new TrackedDVDeleteFile(file().copyWithoutStats(), spec());
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

  private static PartitionSpec resolveSpec(
      TrackedFile file, Map<Integer, PartitionSpec> specsById) {
    Integer specId = file.specId();
    if (specId != null) {
      PartitionSpec spec = specsById.get(specId);
      Preconditions.checkArgument(
          spec != null, "Cannot find partition spec for spec ID: %s", specId);
      return spec;
    }

    // A null spec ID means the file is unpartitioned; use the table's unpartitioned spec.
    for (PartitionSpec spec : specsById.values()) {
      if (spec.isUnpartitioned()) {
        return spec;
      }
    }

    throw new IllegalArgumentException(
        "Cannot find unpartitioned spec in specs: " + specsById.keySet());
  }
}
