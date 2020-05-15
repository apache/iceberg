package org.apache.iceberg.delta;

import com.google.common.base.MoreObjects;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.DeltaFile;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types;


public class GenericDeltaManifestEntry implements DeltaManifestEntry, IndexedRecord {

  private final org.apache.avro.Schema schema;
  private Long snapshotId = null;
  private Long sequenceNumber = -1L;
  private DeltaFile deltaFile = null;

  GenericDeltaManifestEntry(org.apache.avro.Schema schema) {
    this.schema = schema;
  }

  GenericDeltaManifestEntry(Types.StructType partitionType, Types.StructType primaryKeyType) {
    this.schema = AvroSchemaUtil.convert(DeltaFile.getType(partitionType, primaryKeyType), "delta_manifest_entry");
  }

  private GenericDeltaManifestEntry(GenericDeltaManifestEntry toCopy, boolean fullCopy) {
    this.schema = toCopy.schema;
    this.snapshotId = toCopy.snapshotId;
    if (fullCopy) {
      this.deltaFile = toCopy.file().copy();
    } else {
      this.deltaFile = toCopy.file().copyWithoutStats();
    }
  }

  GenericDeltaManifestEntry wrap(Long newSnapshotId, DeltaFile newFile) {
    this.snapshotId = newSnapshotId;
    this.deltaFile = newFile;
    return this;
  }

  public Long snapshotId() {
    return snapshotId;
  }

  @Override
  public void setSnapshotId(long snapshotId) {
    this.snapshotId = snapshotId;
  }

  @Override
  public Long sequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public void setSequenceNumber(long sequenceNumber) {
    this.sequenceNumber = sequenceNumber;
  }

  public DeltaFile file() {
    return deltaFile;
  }

  public GenericDeltaManifestEntry copy() {
    return new GenericDeltaManifestEntry(this, true /* full copy */);
  }

  public GenericDeltaManifestEntry copyWithoutStats() {
    return new GenericDeltaManifestEntry(this, false /* drop stats */);
  }


  @Override
  public void put(int i, Object v) {
    switch (i) {
      case 0:
        this.snapshotId = (Long) v;
        return;
      case 1:
        this.sequenceNumber = (Long) v;
        return;
      case 2:
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
        return sequenceNumber;
      case 2:
        return deltaFile;
      default:
        throw new UnsupportedOperationException("Unknown field ordinal: " + i);
    }
  }

  @Override
  public org.apache.avro.Schema getSchema() {
    return schema;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("snapshot_id", snapshotId)
        .add("sequence_number", sequenceNumber)
        .add("deltaFile", deltaFile)
        .toString();
  }
}
