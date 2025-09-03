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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkConfParser;

public class FlinkMaintenanceConfig {

  public static final String PREFIX = "flink-maintenance.";

  public static final String LOCK_CHECK_DELAY = PREFIX + "lock-check-delay-seconds";
  public static final ConfigOption<Long> LOCK_CHECK_DELAY_OPTION =
      ConfigOptions.key(LOCK_CHECK_DELAY)
          .longType()
          .defaultValue(TableMaintenance.LOCK_CHECK_DELAY_SECOND_DEFAULT)
          .withDescription(
              "The delay time (in seconds) between each lock check during maintenance operations such as "
                  + "rewriting data files, manifest files, expiring snapshots, and deleting orphan files.");

  public static final String PARALLELISM = PREFIX + "parallelism";
  public static final ConfigOption<Integer> PARALLELISM_OPTION =
      ConfigOptions.key(PARALLELISM)
          .intType()
          .defaultValue(ExecutionConfig.PARALLELISM_DEFAULT)
          .withDescription("The number of parallel tasks for the maintenance action.");

  public static final String RATE_LIMIT = PREFIX + "rate-limit-seconds";
  public static final ConfigOption<Long> RATE_LIMIT_OPTION =
      ConfigOptions.key(RATE_LIMIT)
          .longType()
          .defaultValue(TableMaintenance.RATE_LIMIT_SECOND_DEFAULT)
          .withDescription(
              "The rate limit (in seconds) for maintenance operations. "
                  + "This controls how many operations can be performed per second.");

  public static final String SLOT_SHARING_GROUP = PREFIX + "slot-sharing-group";
  public static final ConfigOption<String> SLOT_SHARING_GROUP_OPTION =
      ConfigOptions.key(SLOT_SHARING_GROUP)
          .stringType()
          .defaultValue(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)
          .withDescription(
              "The slot sharing group for maintenance tasks. "
                  + "Determines which operators can share slots in the Flink execution environment.");

  private final FlinkConfParser confParser;
  private final Table table;
  private final Map<String, String> writeProperties;
  private final ReadableConfig readableConfig;

  public FlinkMaintenanceConfig(
      Table table, Map<String, String> writeOptions, ReadableConfig readableConfig) {
    this.table = table;
    this.readableConfig = readableConfig;
    this.writeProperties = writeOptions;
    this.confParser = new FlinkConfParser(table, writeOptions, readableConfig);
  }

  /** Gets the rate limit value (in seconds) for maintenance operations. */
  public long rateLimit() {
    return confParser
        .longConf()
        .option(RATE_LIMIT)
        .flinkConfig(RATE_LIMIT_OPTION)
        .defaultValue(RATE_LIMIT_OPTION.defaultValue())
        .parse();
  }

  /** Gets the parallelism value for maintenance tasks. */
  public int parallelism() {
    return confParser
        .intConf()
        .option(PARALLELISM)
        .flinkConfig(PARALLELISM_OPTION)
        .defaultValue(PARALLELISM_OPTION.defaultValue())
        .parse();
  }

  /** Gets the lock check delay value (in seconds). */
  public long lockCheckDelay() {
    return confParser
        .longConf()
        .option(LOCK_CHECK_DELAY)
        .flinkConfig(LOCK_CHECK_DELAY_OPTION)
        .defaultValue(LOCK_CHECK_DELAY_OPTION.defaultValue())
        .parse();
  }

  /** Gets the slot sharing group value for maintenance tasks. */
  public String slotSharingGroup() {
    return confParser
        .stringConf()
        .option(SLOT_SHARING_GROUP)
        .flinkConfig(SLOT_SHARING_GROUP_OPTION)
        .defaultValue(SLOT_SHARING_GROUP_OPTION.defaultValue())
        .parse();
  }

  public RewriteDataFilesConfig createRewriteDataFilesConfig() {
    return new RewriteDataFilesConfig(table, writeProperties, readableConfig);
  }

  public LockConfig createLockConfig() {
    return new LockConfig(table, writeProperties, readableConfig);
  }
}
