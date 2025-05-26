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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkConfParser;
import org.apache.iceberg.flink.maintenance.operator.LockConfig;

public class FlinkMaintenanceConfig {

  public static final String PREFIX = "flink-maintenance.";
  public static final String RATE_LIMIT = PREFIX + "rate-limit-seconds";
  public static final String SLOT_SHARING_GROUP = PREFIX + "slot-sharing-group";
  public static final String LOCK_CHECK_DELAY = PREFIX + "lock-check-delay-seconds";
  public static final String PARALLELISM = PREFIX + "parallelism";

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

  public long rateLimit() {
    return confParser
        .longConf()
        .option(RATE_LIMIT)
        .defaultValue(TableMaintenance.RATE_LIMIT_SECOND_DEFAULT)
        .parse();
  }

  public int parallelism() {
    return confParser
        .intConf()
        .option(PARALLELISM)
        .defaultValue(ExecutionConfig.PARALLELISM_DEFAULT)
        .parse();
  }

  public long lockCheckDelay() {
    return confParser
        .longConf()
        .option(LOCK_CHECK_DELAY)
        .defaultValue(TableMaintenance.LOCK_CHECK_DELAY_SECOND_DEFAULT)
        .parse();
  }

  public String slotSharingGroup() {
    return confParser
        .stringConf()
        .option(SLOT_SHARING_GROUP)
        .defaultValue(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)
        .parse();
  }

  public RewriteDataFilesConfig createRewriteDataFilesConfig() {
    return new RewriteDataFilesConfig(table, writeProperties, readableConfig);
  }

  public LockConfig createLockConfig() {
    return new LockConfig(writeProperties, readableConfig);
  }
}
