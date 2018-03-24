/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg;

public class TableProperties {
  public static final String COMMIT_NUM_RETRIES = "commit.retry.num-retries";
  public static final int COMMIT_NUM_RETRIES_DEFAULT = 4;

  public static final String COMMIT_MIN_RETRY_WAIT_MS = "commit.retry.min-wait-ms";
  public static final int COMMIT_MIN_RETRY_WAIT_MS_DEFAULT = 100;

  public static final String COMMIT_MAX_RETRY_WAIT_MS = "commit.retry.max-wait-ms";
  public static final int COMMIT_MAX_RETRY_WAIT_MS_DEFAULT = 60000; // 1 minute

  public static final String COMMIT_TOTAL_RETRY_TIME_MS = "commit.retry.total-timeout-ms";
  public static final int COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT = 60000; // 1 minute

  public static final String MANIFEST_TARGET_SIZE_BYTES = "commit.manifest.target-size-bytes";
  public static final long MANIFEST_TARGET_SIZE_BYTES_DEFAULT = 4194304; // 4 MB

  public static final String MANIFEST_MIN_MERGE_COUNT = "commit.manifest.min-count-to-merge";
  public static final int MANIFEST_MIN_MERGE_COUNT_DEFAULT = 20;

  public static final String DEFAULT_FILE_FORMAT = "write.format.default";
  public static final String DEFAULT_FILE_FORMAT_DEFAULT = "parquet";

  public static final String SPLIT_SIZE = "read.split.target-size";
  public static final long SPLIT_SIZE_DEFAULT = 134217728;

  public static final String SPLIT_LOOKBACK = "read.split.planning-lookback";
  public static final int SPLIT_LOOKBACK_DEFAULT = 10;
}
