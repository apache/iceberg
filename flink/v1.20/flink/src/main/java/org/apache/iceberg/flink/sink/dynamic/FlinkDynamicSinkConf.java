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
package org.apache.iceberg.flink.sink.dynamic;

import java.util.Map;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.iceberg.flink.FlinkConfParser;

/**
 * A class for common Dynamic Iceberg sink configs for Flink writes.
 *
 * <p>If a config is set at multiple levels, the following order of precedence is used (top to
 * bottom):
 *
 * <ol>
 *   <li>Write options
 *   <li>Flink ReadableConfig
 *   <li>Default values
 * </ol>
 *
 * The most specific value is set in write options and takes precedence over all other configs. If
 * no write option is provided, this class checks the flink configuration for any overrides. If no
 * applicable value is found in the write options, this class uses the default values.
 */
class FlinkDynamicSinkConf {

  private final FlinkConfParser confParser;

  FlinkDynamicSinkConf(Map<String, String> writeOptions, ReadableConfig readableConfig) {
    this.confParser = new FlinkConfParser(writeOptions, readableConfig);
  }

  int cacheMaxSize() {
    return confParser
        .intConf()
        .option(FlinkDynamicSinkOptions.CACHE_MAX_SIZE.key())
        .flinkConfig(FlinkDynamicSinkOptions.CACHE_MAX_SIZE)
        .defaultValue(FlinkDynamicSinkOptions.CACHE_MAX_SIZE.defaultValue())
        .parse();
  }

  boolean immediateTableUpdate() {
    return confParser
        .booleanConf()
        .option(FlinkDynamicSinkOptions.IMMEDIATE_TABLE_UPDATE.key())
        .flinkConfig(FlinkDynamicSinkOptions.IMMEDIATE_TABLE_UPDATE)
        .defaultValue(FlinkDynamicSinkOptions.IMMEDIATE_TABLE_UPDATE.defaultValue())
        .parse();
  }

  boolean dropUnusedColumns() {
    return confParser
        .booleanConf()
        .option(FlinkDynamicSinkOptions.DROP_UNUSED_COLUMNS.key())
        .flinkConfig(FlinkDynamicSinkOptions.DROP_UNUSED_COLUMNS)
        .defaultValue(FlinkDynamicSinkOptions.DROP_UNUSED_COLUMNS.defaultValue())
        .parse();
  }

  long cacheRefreshMs() {
    return confParser
        .longConf()
        .option(FlinkDynamicSinkOptions.CACHE_REFRESH_MS.key())
        .flinkConfig(FlinkDynamicSinkOptions.CACHE_REFRESH_MS)
        .defaultValue(FlinkDynamicSinkOptions.CACHE_REFRESH_MS.defaultValue())
        .parse();
  }

  int inputSchemasPerTableCacheMaxSize() {
    return confParser
        .intConf()
        .option(FlinkDynamicSinkOptions.INPUT_SCHEMAS_PER_TABLE_CACHE_MAX_SIZE.key())
        .flinkConfig(FlinkDynamicSinkOptions.INPUT_SCHEMAS_PER_TABLE_CACHE_MAX_SIZE)
        .defaultValue(FlinkDynamicSinkOptions.INPUT_SCHEMAS_PER_TABLE_CACHE_MAX_SIZE.defaultValue())
        .parse();
  }

  boolean caseSensitive() {
    return confParser
        .booleanConf()
        .option(FlinkDynamicSinkOptions.CASE_SENSITIVE.key())
        .flinkConfig(FlinkDynamicSinkOptions.CASE_SENSITIVE)
        .defaultValue(FlinkDynamicSinkOptions.CASE_SENSITIVE.defaultValue())
        .parse();
  }
}
