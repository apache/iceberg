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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

@Experimental
public class FlinkDynamicSinkOptions {

  private FlinkDynamicSinkOptions() {}

  public static final ConfigOption<Integer> CACHE_MAX_SIZE =
      ConfigOptions.key("dynamic-sink.cache-max-size")
          .intType()
          .defaultValue(100)
          .withDescription(
              "Maximum size of the caches used in Dynamic Sink for table data and serializers.");

  public static final ConfigOption<Boolean> IMMEDIATE_TABLE_UPDATE =
      ConfigOptions.key("dynamic-sink.immediate-table-update")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "Controls whether table schema and partition updates should be applied immediately in Dynamic Sink.");

  public static final ConfigOption<Boolean> DROP_UNUSED_COLUMNS =
      ConfigOptions.key("dynamic-sink.drop-unused-columns")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "Allows dropping unused columns during schema evolution in Dynamic Sink.");

  public static final ConfigOption<Long> CACHE_REFRESH_MS =
      ConfigOptions.key("dynamic-sink.cache-refresh-ms")
          .longType()
          .defaultValue(1_000L)
          .withDescription(
              "Cache refresh interval for dynamic table metadata in Dynamic Sink in milliseconds.");

  public static final ConfigOption<Integer> INPUT_SCHEMAS_PER_TABLE_CACHE_MAX_SIZE =
      ConfigOptions.key("dynamic-sink.input-schemas-per-table-cache-max-size")
          .intType()
          .defaultValue(10)
          .withDescription(
              "Maximum input schema objects to cache per each table in Dynamic Sink for performance.");

  public static final ConfigOption<Boolean> CASE_SENSITIVE =
      ConfigOptions.key("dynamic-sink.case-sensitive")
          .booleanType()
          .defaultValue(true)
          .withDescription(
              "Controls whether schema field name matching should be case-sensitive in Dynamic Sink.");
}
