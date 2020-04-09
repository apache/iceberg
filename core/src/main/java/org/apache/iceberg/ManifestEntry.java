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

import com.google.common.base.MoreObjects;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StructType;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

class ManifestEntry implements IndexedRecord, SpecificData.SchemaConstructable {
  enum Status {
    EXISTING(0),
    ADDED(1),
    DELETED(2);

    private final int id;

    Status(int id) {
      this.id = id;
    }

    public int id() {
      return id;
    }
  }

  private final org.apache.avro.Schema schema;
  private final IndexedDataFile fileWrapper;
  private Status status = Status.EXISTING;
  private Long snapshotId = null;
  private DataFile file = null;

  ManifestEntry(org.apache.avro.Schema schema) {
    this.schema = schema;
    this.fileWrapper = null; // do not use the file wrapper to read
  }

  ManifestEntry(StructType partitionType) {
    this.schema = AvroSchemaUtil.convert(getSchema(partitionType), "manifest_entry");
    this.fileWrapper = new IndexedDataFile(schema.getField("data_file").schema());
  }

  private ManifestEntry(ManifestEntry toCopy, boolean fullCopy) {
    this.schema = toCopy.schema;
    this.fileWrapper = new IndexedDataFile(schema.getField("data_file").schema());
    this.status = toCopy.status;
    this.snapshotId = toCopy.snapshotId;
    if (fullCopy) {
      this.file = toCopy.file().copy();
    } else {
      this.file = toCopy.file().copyWithoutStats();
    }
  }

  ManifestEntry wrapExisting(Long newSnapshotId, DataFile newFile) {
    this.status = Status.EXISTING;
    this.snapshotId = newSnapshotId;
    this.file = newFile;
    return this;
  }

  ManifestEntry wrapAppend(Long newSnapshotId, DataFile newFile) {
    this.status = Status.ADDED;
    this.snapshotId = newSnapshotId;
    this.file = newFile;
    return this;
  }

  ManifestEntry wrapDelete(Long newSnapshotId, DataFile newFile) {
    this.status = Status.DELETED;
    this.snapshotId = newSnapshotId;
    this.file = newFile;
    return this;
  }

  /**
   * @return the status of the file, whether EXISTING, ADDED, or DELETED
   */
  public Status status() {
    return status;
  }

  /**
   * @return id of the snapshot in which the file was added to the table
   */
  public Long snapshotId() {
    return snapshotId;
  }

  /**
   * @return a file
   */
  public DataFile file() {
    return file;
  }

  public ManifestEntry copy() {
    return new ManifestEntry(this, true /* full copy */);
  }

  public ManifestEntry copyWithoutStats() {
    return new ManifestEntry(this, false /* drop stats */);
  }

  public void setSnapshotId(Long snapshotId) {
    this.snapshotId = snapshotId;
  }

  @Override
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.status = Status.values()[(Integer) v];
        return;
      case 1:
        this.snapshotId = (Long) v;
        return;
      case 2:
        this.file = (DataFile) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return status.id();
      case 1:
        return snapshotId;
      case 2:
        if (fileWrapper == null || file instanceof GenericDataFile) {
          return file;
        } else {
          return fileWrapper.wrap(file);
        }
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }

  @Override
  public org.apache.avro.Schema getSchema() {
    return schema;
  }

  static Schema getSchema(StructType partitionType) {
    return wrapFileSchema(DataFile.getType(partitionType));
  }

  static Schema projectSchema(StructType partitionType, Collection<String> columns) {
    return wrapFileSchema(
        new Schema(DataFile.getType(partitionType).fields()).select(columns).asStruct());
  }

  static Schema wrapFileSchema(StructType fileStruct) {
    // ids for top-level columns are assigned from 1000
    return new Schema(
        required(0, "status", IntegerType.get()),
        optional(1, "snapshot_id", LongType.get()),
        required(2, "data_file", fileStruct));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("status", status)
        .add("snapshot_id", snapshotId)
        .add("file", file)
        .toString();
  }

  private static class IndexedStructLike implements StructLike, IndexedRecord {
    private final org.apache.avro.Schema avroSchema;
    private StructLike wrapped = null;

    IndexedStructLike(org.apache.avro.Schema avroSchema) {
      this.avroSchema = avroSchema;
    }

    public IndexedStructLike wrap(StructLike struct) {
      this.wrapped = struct;
      return this;
    }

    @Override
    public int size() {
      return wrapped.size();
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return wrapped.get(pos, javaClass);
    }

    @Override
    public Object get(int pos) {
      return get(pos, Object.class);
    }

    @Override
    public <T> void set(int pos, T value) {
      wrapped.set(pos, value);
    }

    @Override
    public void put(int pos, Object value) {
      set(pos, value);
    }

    @Override
    public org.apache.avro.Schema getSchema() {
      return avroSchema;
    }
  }

  private static class IndexedDataFile implements DataFile, IndexedRecord {
    private static final long DEFAULT_BLOCK_SIZE = 64 * 1024 * 1024;

    private final org.apache.avro.Schema avroSchema;
    private final IndexedStructLike partitionWrapper;
    private DataFile wrapped = null;

    IndexedDataFile(org.apache.avro.Schema avroSchema) {
      this.avroSchema = avroSchema;
      this.partitionWrapper = new IndexedStructLike(avroSchema.getField("partition").schema());
    }

    public IndexedDataFile wrap(DataFile file) {
      this.wrapped = file;
      return this;
    }

    @Override
    public Object get(int pos) {
      switch (pos) {
        case 0:
          return wrapped.path().toString();
        case 1:
          return wrapped.format() != null ? wrapped.format().toString() : null;
        case 2:
          return partitionWrapper.wrap(wrapped.partition());
        case 3:
          return wrapped.recordCount();
        case 4:
          return wrapped.fileSizeInBytes();
        case 5:
          return DEFAULT_BLOCK_SIZE;
        case 6:
          return wrapped.fileOrdinal();
        case 7:
          return wrapped.sortColumns();
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
    public Integer fileOrdinal() {
      return wrapped.fileOrdinal();
    }

    @Override
    public List<Integer> sortColumns() {
      return wrapped.sortColumns();
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
