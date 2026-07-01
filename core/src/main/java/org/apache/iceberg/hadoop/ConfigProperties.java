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
package org.apache.iceberg.hadoop;

public class ConfigProperties {

  private ConfigProperties() {}

  public static final String ENGINE_HIVE_ENABLED = "iceberg.engine.hive.enabled";
  public static final String LOCK_HIVE_ENABLED = "iceberg.engine.hive.lock-enabled";
  public static final String KEEP_HIVE_STATS = "iceberg.hive.keep.stats";

  public static final String VERSION_HINT_NUM_RETRIES = "iceberg.version-hint.retry.num-retries";
  public static final int VERSION_HINT_NUM_RETRIES_DEFAULT = 2;
  public static final String VERSION_HINT_RETRY_MIN_WAIT_MS =
      "iceberg.version-hint.retry.min-wait-ms";
  public static final long VERSION_HINT_RETRY_MIN_WAIT_MS_DEFAULT = 100L;
  public static final String VERSION_HINT_RETRY_MAX_WAIT_MS =
      "iceberg.version-hint.retry.max-wait-ms";
  public static final long VERSION_HINT_RETRY_MAX_WAIT_MS_DEFAULT = 800L;
  public static final String VERSION_HINT_RETRY_TOTAL_TIMEOUT_MS =
      "iceberg.version-hint.retry.total-timeout-ms";
  public static final long VERSION_HINT_RETRY_TOTAL_TIMEOUT_MS_DEFAULT = 2_000L;
}
