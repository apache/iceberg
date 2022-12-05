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

import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;

import java.time.Duration;
import java.util.Map;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.TimeUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;

public class FlinkReadConf {

  private final FlinkConfParser confParser;

  public FlinkReadConf(
      Table table, Map<String, String> readOptions, ReadableConfig readableConfig) {
    this.confParser = new FlinkConfParser(table, readOptions, readableConfig);
  }

  public Long snapshotId() {
    return confParser.longConf().option(FlinkReadOptions.SNAPSHOT_ID.key()).parseOptional();
  }

  public boolean caseSensitive() {
    return confParser
        .booleanConf()
        .option(FlinkReadOptions.CASE_SENSITIVE.key())
        .flinkConfig(FlinkReadOptions.CASE_SENSITIVE)
        .defaultValue(FlinkReadOptions.CASE_SENSITIVE.defaultValue())
        .parse();
  }

  public Long asOfTimestamp() {
    return confParser.longConf().option(FlinkReadOptions.AS_OF_TIMESTAMP.key()).parseOptional();
  }

  public StreamingStartingStrategy startingStrategy() {
    return confParser
        .enumConfParser(StreamingStartingStrategy.class)
        .option(FlinkReadOptions.STARTING_STRATEGY.key())
        .flinkConfig(FlinkReadOptions.STARTING_STRATEGY)
        .defaultValue(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT)
        .parse();
  }

  public Long startSnapshotTimestamp() {
    return confParser
        .longConf()
        .option(FlinkReadOptions.START_SNAPSHOT_TIMESTAMP.key())
        .parseOptional();
  }

  public Long startSnapshotId() {
    return confParser.longConf().option(FlinkReadOptions.START_SNAPSHOT_ID.key()).parseOptional();
  }

  public Long endSnapshotId() {
    return confParser.longConf().option(FlinkReadOptions.END_SNAPSHOT_ID.key()).parseOptional();
  }

  public long splitSize() {
    return confParser
        .longConf()
        .option(FlinkReadOptions.SPLIT_SIZE.key())
        .flinkConfig(FlinkReadOptions.SPLIT_SIZE)
        .tableProperty(TableProperties.SPLIT_SIZE)
        .defaultValue(TableProperties.SPLIT_SIZE_DEFAULT)
        .parse();
  }

  public int splitLookback() {
    return confParser
        .intConf()
        .option(FlinkReadOptions.SPLIT_LOOKBACK.key())
        .flinkConfig(FlinkReadOptions.SPLIT_LOOKBACK)
        .tableProperty(TableProperties.SPLIT_LOOKBACK)
        .defaultValue(TableProperties.SPLIT_LOOKBACK_DEFAULT)
        .parse();
  }

  public long splitFileOpenCost() {
    return confParser
        .longConf()
        .option(FlinkReadOptions.SPLIT_FILE_OPEN_COST.key())
        .flinkConfig(FlinkReadOptions.SPLIT_FILE_OPEN_COST)
        .tableProperty(TableProperties.SPLIT_OPEN_FILE_COST)
        .defaultValue(TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT)
        .parse();
  }

  public boolean streaming() {
    return confParser
        .booleanConf()
        .option(FlinkReadOptions.STREAMING.key())
        .flinkConfig(FlinkReadOptions.STREAMING)
        .defaultValue(FlinkReadOptions.STREAMING.defaultValue())
        .parse();
  }

  public Duration monitorInterval() {
    String duration =
        confParser
            .stringConf()
            .option(FlinkReadOptions.MONITOR_INTERVAL.key())
            .flinkConfig(FlinkReadOptions.MONITOR_INTERVAL)
            .defaultValue(FlinkReadOptions.MONITOR_INTERVAL.defaultValue())
            .parse();

    return TimeUtils.parseDuration(duration);
  }

  public boolean includeColumnStats() {
    return confParser
        .booleanConf()
        .option(FlinkReadOptions.INCLUDE_COLUMN_STATS.key())
        .flinkConfig(FlinkReadOptions.INCLUDE_COLUMN_STATS)
        .defaultValue(FlinkReadOptions.INCLUDE_COLUMN_STATS.defaultValue())
        .parse();
  }

  public int maxPlanningSnapshotCount() {
    return confParser
        .intConf()
        .option(FlinkReadOptions.MAX_PLANNING_SNAPSHOT_COUNT.key())
        .flinkConfig(FlinkReadOptions.MAX_PLANNING_SNAPSHOT_COUNT)
        .defaultValue(FlinkReadOptions.MAX_PLANNING_SNAPSHOT_COUNT.defaultValue())
        .parse();
  }

  public String nameMapping() {
    return confParser.stringConf().option(DEFAULT_NAME_MAPPING).parseOptional();
  }

  public long limit() {
    return confParser
        .longConf()
        .option(FlinkReadOptions.LIMIT.key())
        .defaultValue(FlinkReadOptions.LIMIT.defaultValue())
        .parse();
  }

  public int workerPoolSize() {
    return confParser
        .intConf()
        .flinkConfig(FlinkConfigOptions.TABLE_EXEC_ICEBERG_WORKER_POOL_SIZE)
        .defaultValue(FlinkConfigOptions.TABLE_EXEC_ICEBERG_WORKER_POOL_SIZE.defaultValue())
        .parse();
  }
}
