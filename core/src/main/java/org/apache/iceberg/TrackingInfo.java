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
import org.apache.iceberg.types.Types;

/**
 * Tracking information for a tracked file entry in a v4 manifest.
 *
 * <p>This groups the status, snapshot, and sequence number information for the entry. This enables
 * accessing the fields for the entry and provides an isolated structure that can be modified.
 */
interface TrackingInfo {
  Types.NestedField STATUS =
      Types.NestedField.required(
          0,
          "status",
          Types.IntegerType.get(),
          "Entry status: 0=existing, 1=added, 2=deleted, 3=replaced");
  Types.NestedField SNAPSHOT_ID =
      Types.NestedField.optional(
          1,
          "snapshot_id",
          Types.LongType.get(),
          "Snapshot ID where the file was added or deleted");
  Types.NestedField DV_SNAPSHOT_ID =
      Types.NestedField.optional(
          157,
          "dv_snapshot_id",
          Types.LongType.get(),
          "Snapshot ID where the DV was added. May only be defined when a deletion vector is present");
  Types.NestedField SEQUENCE_NUMBER =
      Types.NestedField.optional(
          3, "sequence_number", Types.LongType.get(), "Data sequence number of the file");
  Types.NestedField FILE_SEQUENCE_NUMBER =
      Types.NestedField.optional(
          4,
          "file_sequence_number",
          Types.LongType.get(),
          "File sequence number indicating when the file was added");
  Types.NestedField FIRST_ROW_ID =
      Types.NestedField.optional(
          142, "first_row_id", Types.LongType.get(), "ID of the first row in the data file");

  Types.NestedField CHANGED_POSITIONS_DELETED =
      Types.NestedField.optional(
          158,
          "deleted",
          Types.BinaryType.get(),
          "Bitmap of positions deleted in this specific snapshot");
  Types.NestedField CHANGED_POSITIONS_REPLACED =
      Types.NestedField.optional(
          159,
          "replaced",
          Types.BinaryType.get(),
          "Bitmap of positions replaced in this specific snapshot");

  Types.NestedField CHANGED_POSITIONS =
      Types.NestedField.optional(
          153,
          "changed_positions",
          Types.StructType.of(CHANGED_POSITIONS_DELETED, CHANGED_POSITIONS_REPLACED),
          "Bitmaps of deleted and replaced positions in this specific snapshot");

  static Types.StructType schema() {
    return Types.StructType.of(
        STATUS,
        SNAPSHOT_ID,
        DV_SNAPSHOT_ID,
        SEQUENCE_NUMBER,
        FILE_SEQUENCE_NUMBER,
        FIRST_ROW_ID,
        CHANGED_POSITIONS);
  }

  /**
   * Returns the status of the entry.
   *
   * <p>Status values:
   *
   * <ul>
   *   <li>0: EXISTING - file was already in the table
   *   <li>1: ADDED - file newly added
   *   <li>2: DELETED - file removed
   *   <li>3: REPLACED - entry replaced by a column update or DV change (v4 only)
   * </ul>
   *
   * <p>Only ADDED and EXISTING entries are considered live for scan planning. DELETED and REPLACED
   * entries are used for change detection but are not live.
   */
  ManifestEntryStatus status();

  /** Returns the snapshot ID where the file was added or deleted. */
  Long snapshotId();

  /**
   * Returns the snapshot ID where the DV was added.
   *
   * <p>Inherited when null. May only be defined when a deletion vector is present.
   */
  Long dvSnapshotId();

  /** Returns the data sequence number of the file. */
  Long dataSequenceNumber();

  /** Returns the file sequence number indicating when the file was added. */
  Long fileSequenceNumber();

  /** Returns the ID of the first row in the data file. */
  Long firstRowId();

  /**
   * Returns the bitmap of positions in the referenced manifest that were deleted in this specific
   * snapshot.
   *
   * <p>If position at index i is set in this bitmap, it cannot be set in the replaced positions
   * bitmap.
   */
  ByteBuffer deletedPositions();

  /**
   * Returns the bitmap of positions in the referenced manifest that were replaced in this specific
   * snapshot.
   *
   * <p>If position at index i is set in this bitmap, it cannot be set in the deleted positions
   * bitmap. Files that have been replaced will have a corresponding EXISTING entry for the same
   * location.
   */
  ByteBuffer replacedPositions();

  /** Returns the path of the manifest which this entry was read from. */
  String manifestLocation();

  /** Returns the ordinal position of this entry within the manifest. */
  long manifestPos();
}
