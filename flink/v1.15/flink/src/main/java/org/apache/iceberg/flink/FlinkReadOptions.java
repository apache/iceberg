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
  private FlinkReadOptions() {}

  public static final ConfigOption<Long> SNAPSHOT_ID =
      ConfigOptions.key("snapshot-id").longType().defaultValue(null);

  public static final ConfigOption<Boolean> CASE_SENSITIVE =
      ConfigOptions.key("case-sensitive").booleanType().defaultValue(false);

  public static final ConfigOption<Long> AS_OF_TIMESTAMP =
      ConfigOptions.key("as-of-timestamp").longType().defaultValue(null);

  public static final ConfigOption<StreamingStartingStrategy> STARTING_STRATEGY =
      ConfigOptions.key("starting-strategy")
          .enumType(StreamingStartingStrategy.class)
          .defaultValue(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT);

  public static final ConfigOption<Long> START_SNAPSHOT_TIMESTAMP =
      ConfigOptions.key("start-snapshot-timestamp").longType().defaultValue(null);

  public static final ConfigOption<Long> START_SNAPSHOT_ID =
      ConfigOptions.key("start-snapshot-id").longType().defaultValue(null);

  public static final ConfigOption<Long> END_SNAPSHOT_ID =
      ConfigOptions.key("end-snapshot-id").longType().defaultValue(null);

  public static final ConfigOption<Long> SPLIT_SIZE =
      ConfigOptions.key("split-size").longType().defaultValue(TableProperties.SPLIT_SIZE_DEFAULT);

  public static final ConfigOption<Integer> SPLIT_LOOKBACK =
      ConfigOptions.key("split-lookback")
          .intType()
          .defaultValue(TableProperties.SPLIT_LOOKBACK_DEFAULT);

  public static final ConfigOption<Long> SPLIT_FILE_OPEN_COST =
      ConfigOptions.key("split-file-open-cost")
          .longType()
          .defaultValue(TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT);

  public static final ConfigOption<Boolean> STREAMING =
      ConfigOptions.key("streaming").booleanType().defaultValue(false);

  public static final ConfigOption<String> MONITOR_INTERVAL =
      ConfigOptions.key("monitor-interval").stringType().defaultValue("10s");

  public static final ConfigOption<Boolean> INCLUDE_COLUMN_STATS =
      ConfigOptions.key("include-column-stats").booleanType().defaultValue(false);

  public static final ConfigOption<Integer> MAX_PLANNING_SNAPSHOT_COUNT =
      ConfigOptions.key("max-planning-snapshot-count").intType().defaultValue(Integer.MAX_VALUE);
}
