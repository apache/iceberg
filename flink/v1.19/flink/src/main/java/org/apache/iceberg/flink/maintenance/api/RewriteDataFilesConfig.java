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
package org.apache.iceberg.flink.maintenance.api;

import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.flink.FlinkConfParser;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class RewriteDataFilesConfig {
  public static final String PREFIX = FlinkMaintenanceConfig.PREFIX + "rewrite.";

  public static final String MAX_BYTES = PREFIX + "max-bytes";
  public static final ConfigOption<Long> MAX_BYTES_OPTION =
      ConfigOptions.key(MAX_BYTES)
          .longType()
          .defaultValue(Long.MAX_VALUE)
          .withDescription(
              "The maximum number of bytes allowed for a rewrite operation. "
                  + "If the total size of data files exceeds this limit, the rewrites within one scheduled compaction "
                  + "will be limited in size to restrict the resources used by the compaction.");

  public static final ConfigOption<Integer> PARTIAL_PROGRESS_MAX_COMMITS_OPTION =
      ConfigOptions.key(PREFIX + RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS)
          .intType()
          .defaultValue(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT)
          .withDescription(
              "The maximum number of commits allowed when partial progress is enabled. "
                  + "This configuration controls how many file groups "
                  + "are committed per run when partial progress is enabled.");

  public static final ConfigOption<Boolean> PARTIAL_PROGRESS_ENABLED_OPTION =
      ConfigOptions.key(PREFIX + RewriteDataFiles.PARTIAL_PROGRESS_ENABLED)
          .booleanType()
          .defaultValue(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED_DEFAULT)
          .withDescription(
              "Whether to enable partial progress commits. "
                  + "When enabled, the rewrite operation will commit by file group, "
                  + "allowing progress even if some file groups fail to commit.");

  public static final String SCHEDULE_ON_COMMIT_COUNT = PREFIX + "schedule.commit-count";
  public static final ConfigOption<Integer> SCHEDULE_ON_COMMIT_COUNT_OPTION =
      ConfigOptions.key(SCHEDULE_ON_COMMIT_COUNT)
          .intType()
          .defaultValue(10)
          .withDescription(
              "The number of commits after which to trigger a new rewrite operation. "
                  + "This setting controls the frequency of rewrite operations.");

  public static final String SCHEDULE_ON_DATA_FILE_COUNT = PREFIX + "schedule.data-file-count";
  public static final ConfigOption<Integer> SCHEDULE_ON_DATA_FILE_COUNT_OPTION =
      ConfigOptions.key(SCHEDULE_ON_DATA_FILE_COUNT)
          .intType()
          .defaultValue(1000)
          .withDescription("The number of data files that should trigger a new rewrite operation.");

  public static final String SCHEDULE_ON_DATA_FILE_SIZE = PREFIX + "schedule.data-file-size";
  public static final ConfigOption<Long> SCHEDULE_ON_DATA_FILE_SIZE_OPTION =
      ConfigOptions.key(SCHEDULE_ON_DATA_FILE_SIZE)
          .longType()
          .defaultValue(100L * 1024 * 1024 * 1024) // Default is 100 GB
          .withDescription(
              "The total size of data files that should trigger a new rewrite operation.");

  public static final String SCHEDULE_ON_INTERVAL_SECOND = PREFIX + "schedule.interval-second";
  public static final ConfigOption<Long> SCHEDULE_ON_INTERVAL_SECOND_OPTION =
      ConfigOptions.key(SCHEDULE_ON_INTERVAL_SECOND)
          .longType()
          .defaultValue(10 * 60L) // Default is 10 minutes
          .withDescription(
              "The time interval (in seconds) between two consecutive rewrite operations. "
                  + "This ensures periodic scheduling of rewrite tasks.");

  private final FlinkConfParser confParser;
  private final Map<String, String> writeProperties;

  public RewriteDataFilesConfig(
      Table table, Map<String, String> writeOptions, ReadableConfig readableConfig) {
    this.writeProperties = writeOptions;
    this.confParser = new FlinkConfParser(table, writeOptions, readableConfig);
  }

  /** Gets the number of commits that trigger a rewrite operation. */
  public int scheduleOnCommitCount() {
    return confParser
        .intConf()
        .option(SCHEDULE_ON_COMMIT_COUNT)
        .flinkConfig(SCHEDULE_ON_COMMIT_COUNT_OPTION)
        .defaultValue(SCHEDULE_ON_COMMIT_COUNT_OPTION.defaultValue())
        .parse();
  }

  /** Gets the number of data files that trigger a rewrite operation. */
  public int scheduleOnDataFileCount() {
    return confParser
        .intConf()
        .option(SCHEDULE_ON_DATA_FILE_COUNT)
        .flinkConfig(SCHEDULE_ON_DATA_FILE_COUNT_OPTION)
        .defaultValue(SCHEDULE_ON_DATA_FILE_COUNT_OPTION.defaultValue())
        .parse();
  }

  /** Gets the total size of data files that trigger a rewrite operation. */
  public long scheduleOnDataFileSize() {
    return confParser
        .longConf()
        .option(SCHEDULE_ON_DATA_FILE_SIZE)
        .flinkConfig(SCHEDULE_ON_DATA_FILE_SIZE_OPTION)
        .defaultValue(SCHEDULE_ON_DATA_FILE_SIZE_OPTION.defaultValue())
        .parse();
  }

  /** Gets the time interval (in seconds) between two consecutive rewrite operations. */
  public long scheduleOnIntervalSecond() {
    return confParser
        .longConf()
        .option(SCHEDULE_ON_INTERVAL_SECOND)
        .flinkConfig(SCHEDULE_ON_INTERVAL_SECOND_OPTION)
        .defaultValue(SCHEDULE_ON_INTERVAL_SECOND_OPTION.defaultValue())
        .parse();
  }

  /** Gets whether partial progress commits are enabled. */
  public boolean partialProgressEnable() {
    return confParser
        .booleanConf()
        .option(PARTIAL_PROGRESS_ENABLED_OPTION.key())
        .flinkConfig(PARTIAL_PROGRESS_ENABLED_OPTION)
        .defaultValue(PARTIAL_PROGRESS_ENABLED_OPTION.defaultValue())
        .parse();
  }

  /** Gets the maximum number of commits allowed for partial progress. */
  public int partialProgressMaxCommits() {
    return confParser
        .intConf()
        .option(PARTIAL_PROGRESS_MAX_COMMITS_OPTION.key())
        .flinkConfig(PARTIAL_PROGRESS_MAX_COMMITS_OPTION)
        .defaultValue(PARTIAL_PROGRESS_MAX_COMMITS_OPTION.defaultValue())
        .parse();
  }

  /** Gets the maximum rewrite bytes allowed for a single rewrite operation. */
  public long maxRewriteBytes() {
    return confParser
        .longConf()
        .option(MAX_BYTES)
        .flinkConfig(MAX_BYTES_OPTION)
        .defaultValue(MAX_BYTES_OPTION.defaultValue())
        .parse();
  }

  public Map<String, String> properties() {
    return writeProperties.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(PREFIX))
        .collect(
            Collectors.toMap(
                entry -> entry.getKey().substring(PREFIX.length()),
                Map.Entry::getValue,
                (existing, replacement) -> existing,
                Maps::newHashMap));
  }
}
