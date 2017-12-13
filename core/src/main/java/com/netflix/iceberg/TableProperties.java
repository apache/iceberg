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

class TableProperties {
  public static final String COMMIT_NUM_RETRIES = "commit.retry.num-retries";
  public static final int COMMIT_NUM_RETRIES_DEFAULT = 4;

  public static final String COMMIT_MIN_RETRY_WAIT_MS = "commit.retry.min-wait-ms";
  public static final int COMMIT_MIN_RETRY_WAIT_MS_DEFAULT = 100;

  public static final String COMMIT_MAX_RETRY_WAIT_MS = "commit.retry.max-wait-ms";
  public static final int COMMIT_MAX_RETRY_WAIT_MS_DEFAULT = 60000; // 1 minute

  public static final String COMMIT_TOTAL_RETRY_TIME_MS = "commit.retry.total-timeout-ms";
  public static final int COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT = 60000; // 1 minute
}
