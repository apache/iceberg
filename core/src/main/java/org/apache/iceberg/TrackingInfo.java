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

import org.apache.iceberg.types.Types;

/**
 * Tracking information for a tracked file entry in a V4 manifest.
 *
 * <p>This groups the status, snapshot, and sequence number information for the entry. This enables
 * accessing the fields for the entry and provides an isolated structure that can be modified.
 */
interface TrackingInfo {
  /** Status of an entry in a tracked file */
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

  Types.NestedField STATUS =
      Types.NestedField.required(
          0, "status", Types.IntegerType.get(), "Entry status: 0=existing, 1=added, 2=deleted");
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
  Types.NestedField FIRST_ROW_ID =
      Types.NestedField.optional(
          142, "first_row_id", Types.LongType.get(), "ID of the first row in the data file");

  static Types.StructType schema() {
    return Types.StructType.of(
        STATUS, SNAPSHOT_ID, SEQUENCE_NUMBER, FILE_SEQUENCE_NUMBER, FIRST_ROW_ID);
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
   * </ul>
   */
  Status status();

  /** Returns the snapshot ID where the file was added or deleted. */
  Long snapshotId();

  /** Returns the data sequence number of the file. */
  Long dataSequenceNumber();

  /** Returns the file sequence number indicating when the file was added. */
  Long fileSequenceNumber();

  /** Returns the ID of the first row in the data file. */
  Long firstRowId();

  /** Returns the path of the manifest which this entry was read from. */
  String manifestLocation();

  /** Returns the ordinal position of this entry within the manifest. */
  long manifestPos();
}
