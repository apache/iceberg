package org.apache.iceberg.delta;

import com.google.common.base.MoreObjects;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeltaFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types;

import java.util.Collection;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class DeltaManifestEntry implements IndexedRecord {

  private final org.apache.avro.Schema schema;
  private Long snapshotId = null;
  private DeltaFile deltaFile = null;

  DeltaManifestEntry(org.apache.avro.Schema schema) {
    this.schema = schema;
  }

  DeltaManifestEntry(Types.StructType partitionType) {
    this.schema = AvroSchemaUtil.convert(getSchema(partitionType), "delta_manifest_entry");
  }

  private DeltaManifestEntry(DeltaManifestEntry toCopy, boolean fullCopy) {
    this.schema = toCopy.schema;
    this.snapshotId = toCopy.snapshotId;
    if (fullCopy) {
      this.deltaFile = toCopy.file().copy();
    } else {
      this.deltaFile = toCopy.file().copyWithoutStats();
    }
  }

  DeltaManifestEntry wrap(Long newSnapshotId, DeltaFile newFile) {
    this.snapshotId = newSnapshotId;
    this.deltaFile = newFile;
    return this;
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
  public DeltaFile file() {
    return deltaFile;
  }

  public DeltaManifestEntry copy() {
    return new DeltaManifestEntry(this, true /* full copy */);
  }

  public DeltaManifestEntry copyWithoutStats() {
    return new DeltaManifestEntry(this, false /* drop stats */);
  }

  public void setSnapshotId(Long snapshotId) {
    this.snapshotId = snapshotId;
  }

  @Override
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.snapshotId = (Long) v;
        return;
      case 1:
        this.deltaFile = (DeltaFile) v;
        return;
      default:
        // ignore the object, it must be from a newer version of the format
    }
  }

  @Override
  public Object get(int i) {
    switch (i) {
      case 0:
        return snapshotId;
      case 1:
        return deltaFile;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }

  @Override
  public org.apache.avro.Schema getSchema() {
    return schema;
  }

  static Schema getSchema(Types.StructType partitionType) {
    return wrapFileSchema(DataFile.getType(partitionType));
  }

  static Schema projectSchema(Types.StructType partitionType, Collection<String> columns) {
    return wrapFileSchema(
        new Schema(DataFile.getType(partitionType).fields()).select(columns).asStruct());
  }

  static Schema wrapFileSchema(Types.StructType fileStruct) {
    // ids for top-level columns are assigned from 1000
    return new Schema(
        optional(0, "snapshot_id", Types.LongType.get()),
        required(1, "delta_file", fileStruct));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("snapshot_id", snapshotId)
        .add("deltaFile", deltaFile)
        .toString();
  }
}
