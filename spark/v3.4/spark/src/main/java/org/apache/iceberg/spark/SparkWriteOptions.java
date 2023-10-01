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

/** Spark DF write options */
public class SparkWriteOptions {

  private SparkWriteOptions() {}

  // Fileformat for write operations(default: Table write.format.default )
  public static final String WRITE_FORMAT = "write-format";

  // Overrides this table's write.target-file-size-bytes
  public static final String TARGET_FILE_SIZE_BYTES = "target-file-size-bytes";

  // Overrides the default file format for delete files
  public static final String DELETE_FORMAT = "delete-format";

  // Overrides the default size for delete files
  public static final String TARGET_DELETE_FILE_SIZE_BYTES = "target-delete-file-size-bytes";

  //  Sets the nullable check on fields(default: true)
  public static final String CHECK_NULLABILITY = "check-nullability";

  // Adds an entry with custom-key and corresponding value in the snapshot summary
  // ex: df.write().format(iceberg)
  //     .option(SparkWriteOptions.SNAPSHOT_PROPERTY_PREFIX."key1", "value1")
  //     .save(location)
  public static final String SNAPSHOT_PROPERTY_PREFIX = "snapshot-property";

  // Overrides table property write.spark.fanout.enabled(default: false)
  public static final String FANOUT_ENABLED = "fanout-enabled";

  // Checks if input schema and table schema are same(default: true)
  public static final String CHECK_ORDERING = "check-ordering";

  // File scan task set ID that indicates which files must be replaced
  public static final String REWRITTEN_FILE_SCAN_TASK_SET_ID = "rewritten-file-scan-task-set-id";

  // Controls whether to allow writing timestamps without zone info
  @Deprecated
  public static final String HANDLE_TIMESTAMP_WITHOUT_TIMEZONE =
      "handle-timestamp-without-timezone";

  public static final String OUTPUT_SPEC_ID = "output-spec-id";

  public static final String OVERWRITE_MODE = "overwrite-mode";

  // Overrides the default distribution mode for a write operation
  public static final String DISTRIBUTION_MODE = "distribution-mode";

  // Controls whether to take into account the table distribution and sort order during a write
  // operation
  public static final String USE_TABLE_DISTRIBUTION_AND_ORDERING =
      "use-table-distribution-and-ordering";
  public static final boolean USE_TABLE_DISTRIBUTION_AND_ORDERING_DEFAULT = true;

  public static final String MERGE_SCHEMA = "merge-schema";
  public static final String SPARK_MERGE_SCHEMA = "mergeSchema";
  public static final boolean MERGE_SCHEMA_DEFAULT = false;

  // Identifies snapshot from which to start validating conflicting changes
  public static final String VALIDATE_FROM_SNAPSHOT_ID = "validate-from-snapshot-id";

  // Isolation Level for DataFrame calls. Currently supported by overwritePartitions
  public static final String ISOLATION_LEVEL = "isolation-level";

  // Controls write compress options
  public static final String COMPRESSION_CODEC = "compression-codec";
  public static final String COMPRESSION_LEVEL = "compression-level";
  public static final String COMPRESSION_STRATEGY = "compression-strategy";
}
