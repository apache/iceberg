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
import org.apache.iceberg.types.Types.StructType;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

interface ManifestEntry {
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

  // ids for data-file columns are assigned from 1000
  Types.NestedField STATUS = required(0, "status", Types.IntegerType.get());
  Types.NestedField SNAPSHOT_ID = optional(1, "snapshot_id", Types.LongType.get());
  Types.NestedField SEQUENCE_NUMBER = optional(3, "sequence_number", Types.LongType.get());
  int DATA_FILE_ID = 2;
  // next ID to assign: 4

  static Schema getSchema(StructType partitionType) {
    return wrapFileSchema(DataFile.getType(partitionType));
  }

  static Schema wrapFileSchema(StructType fileType) {
    return new Schema(STATUS, SNAPSHOT_ID, SEQUENCE_NUMBER, required(DATA_FILE_ID, "data_file", fileType));
  }

  /**
   * @return the status of the file, whether EXISTING, ADDED, or DELETED
   */
  Status status();

  /**
   * @return id of the snapshot in which the file was added to the table
   */
  Long snapshotId();

  /**
   * Set the snapshot id for this manifest entry.
   *
   * @param snapshotId a long snapshot id
   */
  void setSnapshotId(long snapshotId);

  /**
   * @return the sequence number of the snapshot in which the file was added to the table
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
  DataFile file();

  ManifestEntry copy();

  ManifestEntry copyWithoutStats();
}
