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
package org.apache.iceberg.spark;

/** Spark DF read options */
public class SparkReadOptions {

  private SparkReadOptions() {}

  // Snapshot ID of the table snapshot to read
  public static final String SNAPSHOT_ID = "snapshot-id";

  // Start snapshot ID used in incremental scans (exclusive)
  public static final String START_SNAPSHOT_ID = "start-snapshot-id";

  // End snapshot ID used in incremental scans (inclusive)
  public static final String END_SNAPSHOT_ID = "end-snapshot-id";

  // Start timestamp used in multi-snapshot scans (exclusive)
  public static final String START_TIMESTAMP = "start-timestamp";

  // End timestamp used in multi-snapshot scans (inclusive)
  public static final String END_TIMESTAMP = "end-timestamp";

  // A timestamp in milliseconds; the snapshot used will be the snapshot current at this time.
  public static final String AS_OF_TIMESTAMP = "as-of-timestamp";

  // Overrides the table's read.split.target-size and read.split.metadata-target-size
  public static final String SPLIT_SIZE = "split-size";

  // Overrides the table's read.split.planning-lookback
  public static final String LOOKBACK = "lookback";

  // Overrides the table's read.split.open-file-cost
  public static final String FILE_OPEN_COST = "file-open-cost";

  // Overrides table's vectorization enabled properties
  public static final String VECTORIZATION_ENABLED = "vectorization-enabled";

  // Overrides the table's read.parquet.vectorization.batch-size
  public static final String VECTORIZATION_BATCH_SIZE = "batch-size";

  // Set ID that is used to fetch file scan tasks
  public static final String FILE_SCAN_TASK_SET_ID = "file-scan-task-set-id";

  // skip snapshots of type delete while reading stream out of iceberg table
  public static final String STREAMING_SKIP_DELETE_SNAPSHOTS = "streaming-skip-delete-snapshots";
  public static final boolean STREAMING_SKIP_DELETE_SNAPSHOTS_DEFAULT = false;

  // skip snapshots of type overwrite while reading stream out of iceberg table
  public static final String STREAMING_SKIP_OVERWRITE_SNAPSHOTS =
      "streaming-skip-overwrite-snapshots";
  public static final boolean STREAMING_SKIP_OVERWRITE_SNAPSHOTS_DEFAULT = false;

  // Controls whether to allow reading timestamps without zone info
  public static final String HANDLE_TIMESTAMP_WITHOUT_TIMEZONE =
      "handle-timestamp-without-timezone";

  // Controls whether to report locality information to Spark while allocating input partitions
  public static final String LOCALITY = "locality";

  // Timestamp in milliseconds; start a stream from the snapshot that occurs after this timestamp
  public static final String STREAM_FROM_TIMESTAMP = "stream-from-timestamp";
}
