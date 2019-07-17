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

public class TableProperties {

  private TableProperties() {}

  public static final String COMMIT_NUM_RETRIES = "commit.retry.num-retries";
  public static final int COMMIT_NUM_RETRIES_DEFAULT = 4;

  public static final String COMMIT_MIN_RETRY_WAIT_MS = "commit.retry.min-wait-ms";
  public static final int COMMIT_MIN_RETRY_WAIT_MS_DEFAULT = 100;

  public static final String COMMIT_MAX_RETRY_WAIT_MS = "commit.retry.max-wait-ms";
  public static final int COMMIT_MAX_RETRY_WAIT_MS_DEFAULT = 60000; // 1 minute

  public static final String COMMIT_TOTAL_RETRY_TIME_MS = "commit.retry.total-timeout-ms";
  public static final int COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT = 1800000; // 30 minutes

  public static final String MANIFEST_TARGET_SIZE_BYTES = "commit.manifest.target-size-bytes";
  public static final long MANIFEST_TARGET_SIZE_BYTES_DEFAULT = 8388608; // 8 MB

  public static final String MANIFEST_MIN_MERGE_COUNT = "commit.manifest.min-count-to-merge";
  public static final int MANIFEST_MIN_MERGE_COUNT_DEFAULT = 100;

  public static final String DEFAULT_FILE_FORMAT = "write.format.default";
  public static final String DEFAULT_FILE_FORMAT_DEFAULT = "parquet";

  public static final String PARQUET_ROW_GROUP_SIZE_BYTES = "write.parquet.row-group-size-bytes";
  public static final String PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT = "134217728"; // 128 MB

  public static final String PARQUET_PAGE_SIZE_BYTES = "write.parquet.page-size-bytes";
  public static final String PARQUET_PAGE_SIZE_BYTES_DEFAULT = "1048576"; // 1 MB

  public static final String PARQUET_DICT_SIZE_BYTES = "write.parquet.dict-size-bytes";
  public static final String PARQUET_DICT_SIZE_BYTES_DEFAULT = "2097152"; // 2 MB

  public static final String PARQUET_COMPRESSION = "write.parquet.compression-codec";
  public static final String PARQUET_COMPRESSION_DEFAULT = "gzip";

  public static final String AVRO_COMPRESSION = "write.avro.compression-codec";
  public static final String AVRO_COMPRESSION_DEFAULT = "gzip";

  public static final String SPLIT_SIZE = "read.split.target-size";
  public static final long SPLIT_SIZE_DEFAULT = 134217728; // 128 MB

  public static final String SPLIT_LOOKBACK = "read.split.planning-lookback";
  public static final int SPLIT_LOOKBACK_DEFAULT = 10;

  public static final String SPLIT_OPEN_FILE_COST = "read.split.open-file-cost";
  public static final long SPLIT_OPEN_FILE_COST_DEFAULT = 4 * 1024 * 1024; // 4MB

  public static final String OBJECT_STORE_ENABLED = "write.object-storage.enabled";
  public static final boolean OBJECT_STORE_ENABLED_DEFAULT = false;

  public static final String OBJECT_STORE_PATH = "write.object-storage.path";

  // This only applies to files written after this property is set. Files previously written aren't
  // relocated to reflect this parameter.
  // If not set, defaults to a "data" folder underneath the root path of the table.
  public static final String WRITE_NEW_DATA_LOCATION = "write.folder-storage.path";

  // This only applies to files written after this property is set. Files previously written aren't
  // relocated to reflect this parameter.
  // If not set, defaults to a "meatdata" folder underneath the root path of the table.
  public static final String WRITE_METADATA_LOCATION = "write.metadata.path";

  public static final String MANIFEST_LISTS_ENABLED = "write.manifest-lists.enabled";
  public static final boolean MANIFEST_LISTS_ENABLED_DEFAULT = true;

  public static final String METADATA_COMPRESSION = "write.metadata.compression-codec";
  public static final String METADATA_COMPRESSION_DEFAULT = "none";

  public static final String DEFAULT_WRITE_METRICS_MODE = "write.metadata.metrics.default";
  public static final String DEFAULT_WRITE_METRICS_MODE_DEFAULT = "truncate(16)";
}
