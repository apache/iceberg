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
package org.apache.iceberg.flink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;

/** Flink source read options */
public class FlinkReadOptions {
  private static final String PREFIX = "connector.iceberg.";

  private FlinkReadOptions() {}

  public static final ConfigOption<Long> SNAPSHOT_ID =
      ConfigOptions.key("snapshot-id").longType().defaultValue(null);

  public static final String CASE_SENSITIVE = "case-sensitive";
  public static final ConfigOption<Boolean> CASE_SENSITIVE_OPTION =
      ConfigOptions.key(PREFIX + CASE_SENSITIVE).booleanType().defaultValue(false);

  public static final ConfigOption<Long> AS_OF_TIMESTAMP =
      ConfigOptions.key("as-of-timestamp").longType().defaultValue(null);

  public static final String STARTING_STRATEGY = "starting-strategy";
  public static final ConfigOption<StreamingStartingStrategy> STARTING_STRATEGY_OPTION =
      ConfigOptions.key(PREFIX + STARTING_STRATEGY)
          .enumType(StreamingStartingStrategy.class)
          .defaultValue(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT);

  public static final ConfigOption<Long> START_SNAPSHOT_TIMESTAMP =
      ConfigOptions.key("start-snapshot-timestamp").longType().defaultValue(null);

  public static final ConfigOption<Long> START_SNAPSHOT_ID =
      ConfigOptions.key("start-snapshot-id").longType().defaultValue(null);

  public static final ConfigOption<Long> END_SNAPSHOT_ID =
      ConfigOptions.key("end-snapshot-id").longType().defaultValue(null);

  public static final String SPLIT_SIZE = "split-size";
  public static final ConfigOption<Long> SPLIT_SIZE_OPTION =
      ConfigOptions.key(PREFIX + SPLIT_SIZE)
          .longType()
          .defaultValue(TableProperties.SPLIT_SIZE_DEFAULT);

  public static final String SPLIT_LOOKBACK = "split-lookback";
  public static final ConfigOption<Integer> SPLIT_LOOKBACK_OPTION =
      ConfigOptions.key(PREFIX + SPLIT_LOOKBACK)
          .intType()
          .defaultValue(TableProperties.SPLIT_LOOKBACK_DEFAULT);

  public static final String SPLIT_FILE_OPEN_COST = "split-file-open-cost";
  public static final ConfigOption<Long> SPLIT_FILE_OPEN_COST_OPTION =
      ConfigOptions.key(PREFIX + SPLIT_FILE_OPEN_COST)
          .longType()
          .defaultValue(TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);

  public static final String STREAMING = "streaming";
  public static final ConfigOption<Boolean> STREAMING_OPTION =
      ConfigOptions.key(PREFIX + STREAMING).booleanType().defaultValue(false);

  public static final String MONITOR_INTERVAL = "monitor-interval";
  public static final ConfigOption<String> MONITOR_INTERVAL_OPTION =
      ConfigOptions.key(PREFIX + MONITOR_INTERVAL).stringType().defaultValue("60s");

  public static final String INCLUDE_COLUMN_STATS = "include-column-stats";
  public static final ConfigOption<Boolean> INCLUDE_COLUMN_STATS_OPTION =
      ConfigOptions.key(PREFIX + INCLUDE_COLUMN_STATS).booleanType().defaultValue(false);

  public static final String MAX_PLANNING_SNAPSHOT_COUNT = "max-planning-snapshot-count";
  public static final ConfigOption<Integer> MAX_PLANNING_SNAPSHOT_COUNT_OPTION =
      ConfigOptions.key(PREFIX + MAX_PLANNING_SNAPSHOT_COUNT)
          .intType()
          .defaultValue(Integer.MAX_VALUE);

  public static final String LIMIT = "limit";
  public static final ConfigOption<Long> LIMIT_OPTION =
      ConfigOptions.key(PREFIX + LIMIT).longType().defaultValue(-1L);
}
