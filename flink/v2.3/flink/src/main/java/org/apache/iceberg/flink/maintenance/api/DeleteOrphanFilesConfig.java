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

import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.DeleteOrphanFiles.PrefixMismatchMode;
import org.apache.iceberg.flink.FlinkConfParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.ThreadPools;

public class DeleteOrphanFilesConfig {
  public static final String PREFIX = FlinkMaintenanceConfig.PREFIX + "delete-orphan-files.";

  private static final Splitter COMMA_SPLITTER = Splitter.on(",");
  private static final Splitter EQUALS_SPLITTER = Splitter.on("=").limit(2);

  public static final String SCHEDULE_ON_INTERVAL_SECOND = PREFIX + "schedule.interval-second";
  public static final ConfigOption<Long> SCHEDULE_ON_INTERVAL_SECOND_OPTION =
      ConfigOptions.key(SCHEDULE_ON_INTERVAL_SECOND)
          .longType()
          .defaultValue(60 * 60L) // Default is 1 hour
          .withDescription(
              "The time interval (in seconds) between two consecutive delete orphan files operations.");

  public static final String MIN_AGE_SECONDS = PREFIX + "min-age-seconds";
  public static final ConfigOption<Long> MIN_AGE_SECONDS_OPTION =
      ConfigOptions.key(MIN_AGE_SECONDS)
          .longType()
          .defaultValue(3L * 24 * 60 * 60) // Default is 3 days
          .withDescription(
              "The minimum age (in seconds) of files to be considered for deletion. "
                  + "Files newer than this will not be removed.");

  public static final String DELETE_BATCH_SIZE = PREFIX + "delete-batch-size";
  public static final ConfigOption<Integer> DELETE_BATCH_SIZE_OPTION =
      ConfigOptions.key(DELETE_BATCH_SIZE)
          .intType()
          .defaultValue(1000)
          .withDescription("The batch size used for deleting orphan files.");

  public static final String LOCATION = PREFIX + "location";
  public static final ConfigOption<String> LOCATION_OPTION =
      ConfigOptions.key(LOCATION)
          .stringType()
          .noDefaultValue()
          .withDescription(
              "The location to start recursive listing of candidate files for removal. "
                  + "By default, the table location is used.");

  public static final String USE_PREFIX_LISTING = PREFIX + "use-prefix-listing";
  public static final ConfigOption<Boolean> USE_PREFIX_LISTING_OPTION =
      ConfigOptions.key(USE_PREFIX_LISTING)
          .booleanType()
          .defaultValue(true)
          .withDescription(
              "Whether to use prefix listing when listing files from the file system.");

  public static final String PLANNING_WORKER_POOL_SIZE = PREFIX + "planning-worker-pool-size";
  public static final ConfigOption<Integer> PLANNING_WORKER_POOL_SIZE_OPTION =
      ConfigOptions.key(PLANNING_WORKER_POOL_SIZE)
          .intType()
          .noDefaultValue()
          .withDescription(
              "The worker pool size used for planning the scan of the ALL_FILES table. "
                  + "If not set, the shared worker pool is used.");

  public static final String EQUAL_SCHEMES = PREFIX + "equal-schemes";
  public static final ConfigOption<String> EQUAL_SCHEMES_OPTION =
      ConfigOptions.key(EQUAL_SCHEMES)
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Schemes that should be considered equal, in the format 'scheme1=scheme2,scheme3=scheme4'.");

  public static final String EQUAL_AUTHORITIES = PREFIX + "equal-authorities";
  public static final ConfigOption<String> EQUAL_AUTHORITIES_OPTION =
      ConfigOptions.key(EQUAL_AUTHORITIES)
          .stringType()
          .noDefaultValue()
          .withDescription(
              "Authorities that should be considered equal, in the format 'auth1=auth2,auth3=auth4'.");

  public static final String PREFIX_MISMATCH_MODE = PREFIX + "prefix-mismatch-mode";
  public static final ConfigOption<String> PREFIX_MISMATCH_MODE_OPTION =
      ConfigOptions.key(PREFIX_MISMATCH_MODE)
          .stringType()
          .defaultValue(PrefixMismatchMode.ERROR.name())
          .withDescription(
              "Action behavior when location prefixes (schemes/authorities) mismatch. "
                  + "Valid values: ERROR, IGNORE, DELETE.");

  private final FlinkConfParser confParser;

  public DeleteOrphanFilesConfig(
      Table table, Map<String, String> writeOptions, ReadableConfig readableConfig) {
    this.confParser = new FlinkConfParser(table, writeOptions, readableConfig);
  }

  public long scheduleOnIntervalSecond() {
    return confParser
        .longConf()
        .option(SCHEDULE_ON_INTERVAL_SECOND)
        .flinkConfig(SCHEDULE_ON_INTERVAL_SECOND_OPTION)
        .defaultValue(SCHEDULE_ON_INTERVAL_SECOND_OPTION.defaultValue())
        .parse();
  }

  public long minAgeSeconds() {
    return confParser
        .longConf()
        .option(MIN_AGE_SECONDS)
        .flinkConfig(MIN_AGE_SECONDS_OPTION)
        .defaultValue(MIN_AGE_SECONDS_OPTION.defaultValue())
        .parse();
  }

  public int deleteBatchSize() {
    return confParser
        .intConf()
        .option(DELETE_BATCH_SIZE)
        .flinkConfig(DELETE_BATCH_SIZE_OPTION)
        .defaultValue(DELETE_BATCH_SIZE_OPTION.defaultValue())
        .parse();
  }

  public String location() {
    return confParser.stringConf().option(LOCATION).flinkConfig(LOCATION_OPTION).parseOptional();
  }

  public boolean usePrefixListing() {
    return confParser
        .booleanConf()
        .option(USE_PREFIX_LISTING)
        .flinkConfig(USE_PREFIX_LISTING_OPTION)
        .defaultValue(USE_PREFIX_LISTING_OPTION.defaultValue())
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

  public Map<String, String> equalSchemes() {
    String equalSchemes =
        confParser
            .stringConf()
            .option(EQUAL_SCHEMES)
            .flinkConfig(EQUAL_SCHEMES_OPTION)
            .parseOptional();

    return equalSchemes != null
        ? parseKeyValuePairs(equalSchemes)
        : Maps.newHashMap(DeleteOrphanFiles.DEFAULT_EQUAL_SCHEMES);
  }

  public Map<String, String> equalAuthorities() {
    String equalAuthorities =
        confParser
            .stringConf()
            .option(EQUAL_AUTHORITIES)
            .flinkConfig(EQUAL_AUTHORITIES_OPTION)
            .parseOptional();

    return equalAuthorities != null ? parseKeyValuePairs(equalAuthorities) : Map.of();
  }

  public PrefixMismatchMode prefixMismatchMode() {
    String value =
        confParser
            .stringConf()
            .option(PREFIX_MISMATCH_MODE)
            .flinkConfig(PREFIX_MISMATCH_MODE_OPTION)
            .defaultValue(PREFIX_MISMATCH_MODE_OPTION.defaultValue())
            .parse();
    return PrefixMismatchMode.valueOf(value);
  }

  private static Map<String, String> parseKeyValuePairs(String value) {
    Map<String, String> result = Maps.newHashMap();
    for (String pair : COMMA_SPLITTER.split(value)) {
      List<String> parts = EQUALS_SPLITTER.splitToList(pair);
      Preconditions.checkArgument(parts.size() == 2, "Invalid key-value pair: %s", pair);
      result.put(parts.get(0).trim(), parts.get(1).trim());
    }

    return result;
  }
}
