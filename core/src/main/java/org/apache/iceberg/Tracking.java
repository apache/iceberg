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

/** Tracking information for a manifest entry. */
interface Tracking {
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
  Types.NestedField SEQUENCE_NUMBER =
      Types.NestedField.optional(
          3, "sequence_number", Types.LongType.get(), "Data sequence number of the file");
  Types.NestedField FILE_SEQUENCE_NUMBER =
      Types.NestedField.optional(
          4,
          "file_sequence_number",
          Types.LongType.get(),
          "File sequence number indicating when the file was added");
  Types.NestedField DV_SNAPSHOT_ID =
      Types.NestedField.optional(
          5,
          "dv_snapshot_id",
          Types.LongType.get(),
          "Snapshot ID where the DV was added; null if there is no DV");
  Types.NestedField FIRST_ROW_ID =
      Types.NestedField.optional(
          142, "first_row_id", Types.LongType.get(), "ID of the first row in the data file");
  Types.NestedField DELETED_POSITIONS =
      Types.NestedField.optional(
          6,
          "deleted_positions",
          Types.BinaryType.get(),
          "Bitmap of positions deleted in this snapshot");
  Types.NestedField REPLACED_POSITIONS =
      Types.NestedField.optional(
          7,
          "replaced_positions",
          Types.BinaryType.get(),
          "Bitmap of positions replaced in this snapshot");

  static Types.StructType schema() {
    return Types.StructType.of(
        STATUS,
        SNAPSHOT_ID,
        SEQUENCE_NUMBER,
        FILE_SEQUENCE_NUMBER,
        DV_SNAPSHOT_ID,
        FIRST_ROW_ID,
        DELETED_POSITIONS,
        REPLACED_POSITIONS);
  }

  /** Returns the status of the entry. */
  EntryStatus status();

  /** Returns whether this entry is live. */
  default boolean isLive() {
    return status() == EntryStatus.ADDED || status() == EntryStatus.EXISTING;
  }

  /** Returns the snapshot ID where the file was added or deleted. */
  Long snapshotId();

  /** Returns the data sequence number of the file. */
  Long dataSequenceNumber();

  /** Returns the file sequence number indicating when the file was added. */
  Long fileSequenceNumber();

  /** Returns the snapshot ID where the DV was added; null if there is no DV. */
  Long dvSnapshotId();

  /** Returns the ID of the first row in the data file. */
  Long firstRowId();

  /** Returns the bitmap of positions deleted in this snapshot. */
  ByteBuffer deletedPositions();

  /** Returns the bitmap of positions replaced in this snapshot. */
  ByteBuffer replacedPositions();
}
