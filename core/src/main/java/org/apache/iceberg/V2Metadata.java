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

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types;

import static org.apache.iceberg.types.Types.NestedField.required;

class V2Metadata {
  private V2Metadata() {
  }

  // fields for v2 write schema for required metadata
  static final Types.NestedField REQUIRED_SNAPSHOT_ID =
      required(503, "added_snapshot_id", Types.LongType.get());
  static final Types.NestedField REQUIRED_ADDED_FILES_COUNT =
      required(504, "added_data_files_count", Types.IntegerType.get());
  static final Types.NestedField REQUIRED_EXISTING_FILES_COUNT =
      required(505, "existing_data_files_count", Types.IntegerType.get());
  static final Types.NestedField REQUIRED_DELETED_FILES_COUNT =
      required(506, "deleted_data_files_count", Types.IntegerType.get());
  static final Types.NestedField REQUIRED_ADDED_ROWS_COUNT =
      required(512, "added_rows_count", Types.LongType.get());
  static final Types.NestedField REQUIRED_EXISTING_ROWS_COUNT =
      required(513, "existing_rows_count", Types.LongType.get());
  static final Types.NestedField REQUIRED_DELETED_ROWS_COUNT =
      required(514, "deleted_rows_count", Types.LongType.get());
  static final Types.NestedField REQUIRED_SEQUENCE_NUMBER =
      required(515, "sequence_number", Types.LongType.get());
  static final Types.NestedField REQUIRED_MIN_SEQUENCE_NUMBER =
      required(516, "min_sequence_number", Types.LongType.get());

  static final Schema MANIFEST_LIST_SCHEMA = new Schema(
      ManifestFile.PATH, ManifestFile.LENGTH, ManifestFile.SPEC_ID,
      REQUIRED_SEQUENCE_NUMBER, REQUIRED_MIN_SEQUENCE_NUMBER, REQUIRED_SNAPSHOT_ID,
      REQUIRED_ADDED_FILES_COUNT, REQUIRED_EXISTING_FILES_COUNT, REQUIRED_DELETED_FILES_COUNT,
      REQUIRED_ADDED_ROWS_COUNT, REQUIRED_EXISTING_ROWS_COUNT, REQUIRED_DELETED_ROWS_COUNT,
      ManifestFile.PARTITION_SUMMARIES);


  /**
   * A wrapper class to write any ManifestFile implementation to Avro using the v2 write schema.
   *
   * This is used to maintain compatibility with v2 by writing manifest list files with the old schema, instead of
   * writing a sequence number into metadata files in v2 tables.
   */
  static class IndexedManifestFile implements ManifestFile, IndexedRecord {
    private static final org.apache.avro.Schema AVRO_SCHEMA =
        AvroSchemaUtil.convert(MANIFEST_LIST_SCHEMA, "manifest_file");

    private final long commitSnapshotId;
    private final long sequenceNumber;
    private ManifestFile wrapped = null;

    IndexedManifestFile(long commitSnapshotId, long sequenceNumber) {
      this.commitSnapshotId = commitSnapshotId;
      this.sequenceNumber = sequenceNumber;
    }

    public ManifestFile wrap(ManifestFile file) {
      this.wrapped = file;
      return this;
    }

    @Override
    public org.apache.avro.Schema getSchema() {
      return AVRO_SCHEMA;
    }

    @Override
    public void put(int i, Object v) {
      throw new UnsupportedOperationException("Cannot read using IndexedManifestFile");
    }

    @Override
    public Object get(int pos) {
      switch (pos) {
        case 0:
          return wrapped.path();
        case 1:
          return wrapped.length();
        case 2:
          return wrapped.partitionSpecId();
        case 3:
          if (wrapped.sequenceNumber() == ManifestWriter.UNASSIGNED_SEQ) {
            // if the sequence number is being assigned here, then the manifest must be created by the current
            // operation. to validate this, check that the snapshot id matches the current commit
            Preconditions.checkState(commitSnapshotId == wrapped.snapshotId(),
                "Found unassigned sequence number for a manifest from snapshot: %s", wrapped.snapshotId());
            return sequenceNumber;
          } else {
            return wrapped.sequenceNumber();
          }
        case 4:
          if (wrapped.minSequenceNumber() == ManifestWriter.UNASSIGNED_SEQ) {
            // same sanity check as above
            Preconditions.checkState(commitSnapshotId == wrapped.snapshotId(),
                "Found unassigned sequence number for a manifest from snapshot: %s", wrapped.snapshotId());
            // if the min sequence number is not determined, then there was no assigned sequence number for any file
            // written to the wrapped manifest. replace the unassigned sequence number with the one for this commit
            return sequenceNumber;
          } else {
            return wrapped.minSequenceNumber();
          }
        case 5:
          return wrapped.snapshotId();
        case 6:
          return wrapped.addedFilesCount();
        case 7:
          return wrapped.existingFilesCount();
        case 8:
          return wrapped.deletedFilesCount();
        case 9:
          return wrapped.addedRowsCount();
        case 10:
          return wrapped.existingRowsCount();
        case 11:
          return wrapped.deletedRowsCount();
        case 12:
          return wrapped.partitions();
        default:
          throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
      }
    }

    @Override
    public String path() {
      return wrapped.path();
    }

    @Override
    public long length() {
      return wrapped.length();
    }

    @Override
    public int partitionSpecId() {
      return wrapped.partitionSpecId();
    }

    @Override
    public long sequenceNumber() {
      return wrapped.sequenceNumber();
    }

    @Override
    public long minSequenceNumber() {
      return wrapped.minSequenceNumber();
    }

    @Override
    public Long snapshotId() {
      return wrapped.snapshotId();
    }

    @Override
    public boolean hasAddedFiles() {
      return wrapped.hasAddedFiles();
    }

    @Override
    public Integer addedFilesCount() {
      return wrapped.addedFilesCount();
    }

    @Override
    public Long addedRowsCount() {
      return wrapped.addedRowsCount();
    }

    @Override
    public boolean hasExistingFiles() {
      return wrapped.hasExistingFiles();
    }

    @Override
    public Integer existingFilesCount() {
      return wrapped.existingFilesCount();
    }

    @Override
    public Long existingRowsCount() {
      return wrapped.existingRowsCount();
    }

    @Override
    public boolean hasDeletedFiles() {
      return wrapped.hasDeletedFiles();
    }

    @Override
    public Integer deletedFilesCount() {
      return wrapped.deletedFilesCount();
    }

    @Override
    public Long deletedRowsCount() {
      return wrapped.deletedRowsCount();
    }

    @Override
    public List<PartitionFieldSummary> partitions() {
      return wrapped.partitions();
    }

    @Override
    public ManifestFile copy() {
      return wrapped.copy();
    }
  }

  static Schema manifestSchema(Types.StructType partitionType) {
    // if v2 diverges from the read schema, then copy the schema here
    return new Schema(DataFile.getType(partitionType).fields());
  }

  /**
   * Wrapper used to write a DataFile to v2 metadata. This will override the file's status, snapshot id, and sequence
   * number with correct values for the commit that are passed when wrapping a file. This also implements Avro's
   * IndexedRecord for writing to Avro files.
   */
  static class IndexedDataFile implements DataFile, IndexedRecord {
    private final Long commitSnapshotId;
    private final org.apache.avro.Schema avroSchema;
    private final IndexedStructLike partitionWrapper;

    private FileStatus status = null;
    private Long snapshotId = null;
    private Long sequenceNumber = null;
    private DataFile wrapped = null;

    IndexedDataFile(Long commitSnapshotId, Types.StructType partitionType) {
      this.commitSnapshotId = commitSnapshotId;
      this.avroSchema = AvroSchemaUtil.convert(manifestSchema(partitionType), "data_file");
      this.partitionWrapper = new IndexedStructLike(avroSchema.getField("partition").schema());
    }

    DataFile wrapExisting(Long newSnapshotId, Long newSequenceNumber, DataFile file) {
      this.status = FileStatus.EXISTING;
      this.snapshotId = newSnapshotId;
      this.sequenceNumber = newSequenceNumber;
      this.wrapped = file;
      return this;
    }

    DataFile wrapAppend(Long newSnapshotId, DataFile file) {
      this.status = FileStatus.ADDED;
      this.snapshotId = newSnapshotId;
      this.sequenceNumber = null;
      this.wrapped = file;
      return this;
    }

    DataFile wrapDelete(Long newSnapshotId, Long newSequenceNumber, DataFile file) {
      this.status = FileStatus.DELETED;
      this.snapshotId = newSnapshotId;
      this.sequenceNumber = newSequenceNumber;
      this.wrapped = file;
      return this;
    }

    @Override
    public Object get(int pos) {
      switch (pos) {
        case 0:
          return status.id();
        case 1:
          return snapshotId;
        case 2:
          if (sequenceNumber == null) {
            // if the entry's sequence number is null, then it will inherit the sequence number of the current commit.
            // to validate that this is correct, check that the snapshot id is either null (will also be inherited) or
            // that it matches the id of the current commit.
            Preconditions.checkState(snapshotId == null || snapshotId.equals(commitSnapshotId),
                "Found unassigned sequence number for an entry from snapshot: %s", snapshotId);
            return null;
          }
          return sequenceNumber;
        case 3:
          return wrapped.path().toString();
        case 4:
          return wrapped.format() != null ? wrapped.format().toString() : null;
        case 5:
          return partitionWrapper.wrap(wrapped.partition());
        case 6:
          return wrapped.recordCount();
        case 7:
          return wrapped.fileSizeInBytes();
        case 8:
          return wrapped.columnSizes();
        case 9:
          return wrapped.valueCounts();
        case 10:
          return wrapped.nullValueCounts();
        case 11:
          return wrapped.lowerBounds();
        case 12:
          return wrapped.upperBounds();
        case 13:
          return wrapped.keyMetadata();
        case 14:
          return wrapped.splitOffsets();
      }
      throw new IllegalArgumentException("Unknown field ordinal: " + pos);
    }

    @Override
    public void put(int i, Object v) {
      throw new UnsupportedOperationException("Cannot read into IndexedDataFile");
    }

    @Override
    public org.apache.avro.Schema getSchema() {
      return avroSchema;
    }

    @Override
    public FileStatus status() {
      return status;
    }

    @Override
    public Long snapshotId() {
      return snapshotId;
    }

    @Override
    public Long sequenceNumber() {
      return sequenceNumber;
    }

    @Override
    public CharSequence path() {
      return wrapped.path();
    }

    @Override
    public FileFormat format() {
      return wrapped.format();
    }

    @Override
    public StructLike partition() {
      return wrapped.partition();
    }

    @Override
    public long recordCount() {
      return wrapped.recordCount();
    }

    @Override
    public long fileSizeInBytes() {
      return wrapped.fileSizeInBytes();
    }

    @Override
    public Map<Integer, Long> columnSizes() {
      return wrapped.columnSizes();
    }

    @Override
    public Map<Integer, Long> valueCounts() {
      return wrapped.valueCounts();
    }

    @Override
    public Map<Integer, Long> nullValueCounts() {
      return wrapped.nullValueCounts();
    }

    @Override
    public Map<Integer, ByteBuffer> lowerBounds() {
      return wrapped.lowerBounds();
    }

    @Override
    public Map<Integer, ByteBuffer> upperBounds() {
      return wrapped.upperBounds();
    }

    @Override
    public ByteBuffer keyMetadata() {
      return wrapped.keyMetadata();
    }

    @Override
    public List<Long> splitOffsets() {
      return wrapped.splitOffsets();
    }

    @Override
    public DataFile copy() {
      return wrapped.copy();
    }

    @Override
    public DataFile copyWithoutStats() {
      return wrapped.copyWithoutStats();
    }
  }
}
