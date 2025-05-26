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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.flink.FlinkConfParser;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class RewriteDataFilesConfig {
  public static final String PREFIX = FlinkMaintenanceConfig.PREFIX + "rewrite.";

  public static final String MAX_BYTES = PREFIX + "max-bytes";

  public static final String SCHEDULE_ON_COMMIT_COUNT = PREFIX + "schedule.commit-count";
  public static final int SCHEDULE_ON_COMMIT_COUNT_DEFAULT = 10;

  public static final String SCHEDULE_ON_DATA_FILE_COUNT = PREFIX + "schedule.data-file-count";
  public static final int SCHEDULE_ON_DATA_FILE_COUNT_DEFAULT = 1000;

  public static final String SCHEDULE_ON_DATA_FILE_SIZE = PREFIX + "schedule.data-file-size";
  public static final long SCHEDULE_ON_DATA_FILE_SIZE_DEFAULT = 100L * 1024 * 1024 * 1024; // 100G

  public static final String SCHEDULE_ON_INTERVAL_SECOND = PREFIX + "schedule.interval-second";
  public static final int SCHEDULE_ON_INTERVAL_SECOND_DEFAULT = 10 * 60; // 10 minutes

  private final FlinkConfParser confParser;
  private final Map<String, String> writeProperties;

  public RewriteDataFilesConfig(
      Table table, Map<String, String> writeOptions, ReadableConfig readableConfig) {
    this.writeProperties = writeOptions;
    this.confParser = new FlinkConfParser(table, writeOptions, readableConfig);
  }

  public int scheduleOnCommitCount() {
    return confParser
        .intConf()
        .option(SCHEDULE_ON_COMMIT_COUNT)
        .defaultValue(SCHEDULE_ON_COMMIT_COUNT_DEFAULT)
        .parse();
  }

  public int scheduleOnDataFileCount() {
    return confParser
        .intConf()
        .option(SCHEDULE_ON_DATA_FILE_COUNT)
        .defaultValue(SCHEDULE_ON_DATA_FILE_COUNT_DEFAULT)
        .parse();
  }

  public long scheduleOnDataFileSize() {
    return confParser
        .longConf()
        .option(SCHEDULE_ON_DATA_FILE_SIZE)
        .defaultValue(SCHEDULE_ON_DATA_FILE_SIZE_DEFAULT)
        .parse();
  }

  public int scheduleOnIntervalSecond() {
    return confParser
        .intConf()
        .option(SCHEDULE_ON_INTERVAL_SECOND)
        .defaultValue(SCHEDULE_ON_INTERVAL_SECOND_DEFAULT)
        .parse();
  }

  public boolean partialProgressEnable() {
    return confParser
        .booleanConf()
        .option(PREFIX + RewriteDataFiles.PARTIAL_PROGRESS_ENABLED)
        .defaultValue(RewriteDataFiles.PARTIAL_PROGRESS_ENABLED_DEFAULT)
        .parse();
  }

  public int partialProgressMaxCommits() {
    return confParser
        .intConf()
        .option(PREFIX + RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS)
        .defaultValue(RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT)
        .parse();
  }

  public long maxRewriteBytes() {
    return confParser.longConf().option(MAX_BYTES).defaultValue(Long.MAX_VALUE).parse();
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
