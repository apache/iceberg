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

/** Interface for partition statistics returned from a {@link PartitionStatisticsScan}. */
public interface PartitionStatistics extends StructLike {

  /* The positions of each statistics within the full schema of partition statistics. */
  int PARTITION_POSITION = 0;
  int SPEC_ID_POSITION = 1;
  int DATA_RECORD_COUNT_POSITION = 2;
  int DATA_FILE_COUNT_POSITION = 3;
  int TOTAL_DATA_FILE_SIZE_IN_BYTES_POSITION = 4;
  int POSITION_DELETE_RECORD_COUNT_POSITION = 5;
  int POSITION_DELETE_FILE_COUNT_POSITION = 6;
  int EQUALITY_DELETE_RECORD_COUNT_POSITION = 7;
  int EQUALITY_DELETE_FILE_COUNT_POSITION = 8;
  int TOTAL_RECORD_COUNT_POSITION = 9;
  int LAST_UPDATED_AT_POSITION = 10;
  int LAST_UPDATED_SNAPSHOT_ID_POSITION = 11;
  int DV_COUNT_POSITION = 12;

  /** Returns the partition of these partition statistics */
  StructLike partition();

  /** Returns the spec ID of the partition of these partition statistics */
  Integer specId();

  /** Returns the number of data records in the partition */
  Long dataRecordCount();

  /** Returns the number of data files in the partition */
  Integer dataFileCount();

  /** Returns the total size of data files in bytes in the partition */
  Long totalDataFileSizeInBytes();

  /**
   * Returns the number of positional delete records in the partition. Also includes dv record count
   * as per spec
   */
  Long positionDeleteRecordCount();

  /** Returns the number of positional delete files in the partition */
  Integer positionDeleteFileCount();

  /** Returns the number of equality delete records in the partition */
  Long equalityDeleteRecordCount();

  /** Returns the number of equality delete files in the partition */
  Integer equalityDeleteFileCount();

  /** Returns the total number of records in the partition */
  Long totalRecords();

  /** Returns the timestamp in milliseconds when the partition was last updated */
  Long lastUpdatedAt();

  /** Returns the ID of the snapshot that last updated this partition */
  Long lastUpdatedSnapshotId();

  /** Returns the number of delete vectors in the partition */
  Integer dvCount();
}
