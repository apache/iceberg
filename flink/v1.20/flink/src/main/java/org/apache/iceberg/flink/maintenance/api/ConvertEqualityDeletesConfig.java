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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkConfParser;

public class ConvertEqualityDeletesConfig {
  public static final String PREFIX = FlinkMaintenanceConfig.PREFIX + "convert-equality-deletes.";

  public static final String TARGET_BRANCH = PREFIX + "target-branch";
  public static final ConfigOption<String> TARGET_BRANCH_OPTION =
      ConfigOptions.key(TARGET_BRANCH)
          .stringType()
          .noDefaultValue()
          .withDescription(
              "The branch where converted data files and deletion vectors are committed. "
                  + "Defaults to the sink's write branch, i.e. an in-place conversion.");

  public static final String SCHEDULE_ON_COMMIT_COUNT = PREFIX + "schedule.commit-count";
  public static final ConfigOption<Integer> SCHEDULE_ON_COMMIT_COUNT_OPTION =
      ConfigOptions.key(SCHEDULE_ON_COMMIT_COUNT)
          .intType()
          .defaultValue(1)
          .withDescription(
              "The number of commits after which to trigger a new equality delete conversion.");

  public static final String SCHEDULE_ON_INTERVAL_SECOND = PREFIX + "schedule.interval-second";
  public static final ConfigOption<Long> SCHEDULE_ON_INTERVAL_SECOND_OPTION =
      ConfigOptions.key(SCHEDULE_ON_INTERVAL_SECOND)
          .longType()
          .noDefaultValue()
          .withDescription(
              "The time interval (in seconds) between two consecutive equality delete conversions.");

  private final FlinkConfParser confParser;

  public ConvertEqualityDeletesConfig(
      Table table, Map<String, String> writeOptions, ReadableConfig readableConfig) {
    this.confParser = new FlinkConfParser(table, writeOptions, readableConfig);
  }

  /** Gets the target branch, or null to convert in place on the sink's write branch. */
  public String targetBranch() {
    return confParser
        .stringConf()
        .option(TARGET_BRANCH)
        .flinkConfig(TARGET_BRANCH_OPTION)
        .parseOptional();
  }

  /** Gets the number of commits that trigger a conversion. */
  public int scheduleOnCommitCount() {
    return confParser
        .intConf()
        .option(SCHEDULE_ON_COMMIT_COUNT)
        .flinkConfig(SCHEDULE_ON_COMMIT_COUNT_OPTION)
        .defaultValue(SCHEDULE_ON_COMMIT_COUNT_OPTION.defaultValue())
        .parse();
  }

  /** Gets the time interval (in seconds) between conversions, or null when not set. */
  public Long scheduleOnIntervalSecond() {
    return confParser
        .longConf()
        .option(SCHEDULE_ON_INTERVAL_SECOND)
        .flinkConfig(SCHEDULE_ON_INTERVAL_SECOND_OPTION)
        .parseOptional();
  }
}
