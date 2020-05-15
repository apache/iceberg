package org.apache.iceberg;

import org.apache.iceberg.types.Types;

import static org.apache.iceberg.types.Types.NestedField.required;

interface DeltaManifestEntry {
  Types.NestedField SNAPSHOT_ID = required(0, "snapshot_id", Types.LongType.get());
  Types.NestedField SEQUENCE_NUMBER = required(1, "sequence_number", Types.LongType.get());
  int DELTA_FILE_ID = 3;
  // next ID to assign: 4

  static Schema getSchema(Types.StructType partitionType, Types.StructType primaryKeyType) {
    return wrapFileSchema(DeltaFile.getType(partitionType, primaryKeyType));
  }

  static Schema wrapFileSchema(Types.StructType fileType) {
    return new Schema(SNAPSHOT_ID, SEQUENCE_NUMBER, required(DELTA_FILE_ID, "delta_file", fileType));
  }

  /**
   * @return id of the snapshot in which the delta file was added to the table
   */
  Long snapshotId();

  /**
   * Set the snapshot id for this manifest entry.
   *
   * @param snapshotId a long snapshot id
   */
  void setSnapshotId(long snapshotId);

  /**
   * @return the sequence number of the snapshot in which the delta file was added to the table
   */
  Long sequenceNumber();

  /**
   * Set the sequence number for this manifest entry.
   *
   * @param sequenceNumber a sequence number
   */
  void setSequenceNumber(long sequenceNumber);

  /**
   * @return a file
   */
  DeltaFile file();

  DeltaManifestEntry copy();

  DeltaManifestEntry copyWithoutStats();

}
