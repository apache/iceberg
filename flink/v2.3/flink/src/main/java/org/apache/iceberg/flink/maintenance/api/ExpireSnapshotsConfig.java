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
import org.apache.iceberg.util.ThreadPools;

public class ExpireSnapshotsConfig {
  public static final String PREFIX = FlinkMaintenanceConfig.PREFIX + "expire-snapshots.";

  public static final String SCHEDULE_ON_COMMIT_COUNT = PREFIX + "schedule.commit-count";
  public static final ConfigOption<Integer> SCHEDULE_ON_COMMIT_COUNT_OPTION =
      ConfigOptions.key(SCHEDULE_ON_COMMIT_COUNT)
          .intType()
          .defaultValue(10)
          .withDescription(
              "The number of commits after which to trigger a new expire snapshots operation.");

  public static final String SCHEDULE_ON_INTERVAL_SECOND = PREFIX + "schedule.interval-second";
  public static final ConfigOption<Long> SCHEDULE_ON_INTERVAL_SECOND_OPTION =
      ConfigOptions.key(SCHEDULE_ON_INTERVAL_SECOND)
          .longType()
          .defaultValue(60 * 60L) // Default is 1 hour
          .withDescription(
              "The time interval (in seconds) between two consecutive expire snapshots operations.");

  public static final String MAX_SNAPSHOT_AGE_SECONDS = PREFIX + "max-snapshot-age-seconds";
  public static final ConfigOption<Long> MAX_SNAPSHOT_AGE_SECONDS_OPTION =
      ConfigOptions.key(MAX_SNAPSHOT_AGE_SECONDS)
          .longType()
          .noDefaultValue()
          .withDescription(
              "The maximum age (in seconds) of snapshots to retain. "
                  + "Snapshots older than this will be expired.");

  public static final String RETAIN_LAST = PREFIX + "retain-last";
  public static final ConfigOption<Integer> RETAIN_LAST_OPTION =
      ConfigOptions.key(RETAIN_LAST)
          .intType()
          .noDefaultValue()
          .withDescription("The minimum number of snapshots to retain.");

  public static final String DELETE_BATCH_SIZE = PREFIX + "delete-batch-size";
  public static final ConfigOption<Integer> DELETE_BATCH_SIZE_OPTION =
      ConfigOptions.key(DELETE_BATCH_SIZE)
          .intType()
          .defaultValue(1000)
          .withDescription("The batch size used for deleting expired files.");

  public static final String CLEAN_EXPIRED_METADATA = PREFIX + "clean-expired-metadata";
  public static final ConfigOption<Boolean> CLEAN_EXPIRED_METADATA_OPTION =
      ConfigOptions.key(CLEAN_EXPIRED_METADATA)
          .booleanType()
          .defaultValue(true)
          .withDescription(
              "Whether to clean expired metadata such as partition specs and schemas.");

  public static final String PLANNING_WORKER_POOL_SIZE = PREFIX + "planning-worker-pool-size";
  public static final ConfigOption<Integer> PLANNING_WORKER_POOL_SIZE_OPTION =
      ConfigOptions.key(PLANNING_WORKER_POOL_SIZE)
          .intType()
          .noDefaultValue()
          .withDescription(
              "The worker pool size used to calculate the files to delete. "
                  + "If not set, the shared worker pool is used.");

  private final FlinkConfParser confParser;

  public ExpireSnapshotsConfig(
      Table table, Map<String, String> writeOptions, ReadableConfig readableConfig) {
    this.confParser = new FlinkConfParser(table, writeOptions, readableConfig);
  }

  public int scheduleOnCommitCount() {
    return confParser
        .intConf()
        .option(SCHEDULE_ON_COMMIT_COUNT)
        .flinkConfig(SCHEDULE_ON_COMMIT_COUNT_OPTION)
        .defaultValue(SCHEDULE_ON_COMMIT_COUNT_OPTION.defaultValue())
        .parse();
  }

  public long scheduleOnIntervalSecond() {
    return confParser
        .longConf()
        .option(SCHEDULE_ON_INTERVAL_SECOND)
        .flinkConfig(SCHEDULE_ON_INTERVAL_SECOND_OPTION)
        .defaultValue(SCHEDULE_ON_INTERVAL_SECOND_OPTION.defaultValue())
        .parse();
  }

  public Long maxSnapshotAgeSeconds() {
    return confParser
        .longConf()
        .option(MAX_SNAPSHOT_AGE_SECONDS)
        .flinkConfig(MAX_SNAPSHOT_AGE_SECONDS_OPTION)
        .parseOptional();
  }

  public Integer retainLast() {
    return confParser.intConf().option(RETAIN_LAST).flinkConfig(RETAIN_LAST_OPTION).parseOptional();
  }

  public int deleteBatchSize() {
    return confParser
        .intConf()
        .option(DELETE_BATCH_SIZE)
        .flinkConfig(DELETE_BATCH_SIZE_OPTION)
        .defaultValue(DELETE_BATCH_SIZE_OPTION.defaultValue())
        .parse();
  }

  public Boolean cleanExpiredMetadata() {
    return confParser
        .booleanConf()
        .option(CLEAN_EXPIRED_METADATA)
        .flinkConfig(CLEAN_EXPIRED_METADATA_OPTION)
        .defaultValue(CLEAN_EXPIRED_METADATA_OPTION.defaultValue())
        .parse();
  }

  public Integer planningWorkerPoolSize() {
    return confParser
        .intConf()
        .option(PLANNING_WORKER_POOL_SIZE)
        .flinkConfig(PLANNING_WORKER_POOL_SIZE_OPTION)
        .defaultValue(ThreadPools.WORKER_THREAD_POOL_SIZE)
        .parse();
  }
}
