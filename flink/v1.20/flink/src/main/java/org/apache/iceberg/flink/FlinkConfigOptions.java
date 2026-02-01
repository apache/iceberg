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

import java.time.Duration;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.iceberg.flink.source.assigner.SplitAssignerType;
import org.apache.iceberg.util.ThreadPools;

/**
 * When constructing Flink Iceberg source via Java API, configs can be set in {@link Configuration}
 * passed to source builder. E.g.
 *
 * <pre>
 *   configuration.setBoolean(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, true);
 *   FlinkSource.forRowData()
 *       .flinkConf(configuration)
 *       ...
 * </pre>
 *
 * <p>When using Flink SQL/table API, connector options can be set in Flink's {@link
 * TableEnvironment}.
 *
 * <pre>
 *   TableEnvironment tEnv = createTableEnv();
 *   tEnv.getConfig()
 *        .getConfiguration()
 *        .setBoolean(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, true);
 * </pre>
 */
public class FlinkConfigOptions {

  private FlinkConfigOptions() {}

  public static final ConfigOption<Boolean> TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM =
      ConfigOptions.key("table.exec.iceberg.infer-source-parallelism")
          .booleanType()
          .defaultValue(true)
          .withDescription(
              "If is false, parallelism of source are set by config.\n"
                  + "If is true, source parallelism is inferred according to splits number.\n");

  public static final ConfigOption<Integer> TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM_MAX =
      ConfigOptions.key("table.exec.iceberg.infer-source-parallelism.max")
          .intType()
          .defaultValue(100)
          .withDescription("Sets max infer parallelism for source operator.");

  public static final ConfigOption<Boolean> TABLE_EXEC_ICEBERG_EXPOSE_SPLIT_LOCALITY_INFO =
      ConfigOptions.key("table.exec.iceberg.expose-split-locality-info")
          .booleanType()
          .noDefaultValue()
          .withDescription(
              "Expose split host information to use Flink's locality aware split assigner.");

  public static final ConfigOption<Integer> SOURCE_READER_FETCH_BATCH_RECORD_COUNT =
      ConfigOptions.key("table.exec.iceberg.fetch-batch-record-count")
          .intType()
          .defaultValue(2048)
          .withDescription("The target number of records for Iceberg reader fetch batch.");

  public static final ConfigOption<Integer> TABLE_EXEC_ICEBERG_WORKER_POOL_SIZE =
      ConfigOptions.key("table.exec.iceberg.worker-pool-size")
          .intType()
          .defaultValue(ThreadPools.WORKER_THREAD_POOL_SIZE)
          .withDescription("The size of workers pool used to plan or scan manifests.");

  public static final ConfigOption<Boolean> TABLE_EXEC_ICEBERG_USE_FLIP27_SOURCE =
      ConfigOptions.key("table.exec.iceberg.use-flip27-source")
          .booleanType()
          .defaultValue(true)
          .withDescription("Use the FLIP-27 based Iceberg source implementation.");

  public static final ConfigOption<Boolean> TABLE_EXEC_ICEBERG_USE_V2_SINK =
      ConfigOptions.key("table.exec.iceberg.use-v2-sink")
          .booleanType()
          .defaultValue(false)
          .withDescription("Use the SinkV2 API based Iceberg sink implementation.");

  public static final ConfigOption<SplitAssignerType> TABLE_EXEC_SPLIT_ASSIGNER_TYPE =
      ConfigOptions.key("table.exec.iceberg.split-assigner-type")
          .enumType(SplitAssignerType.class)
          .defaultValue(SplitAssignerType.SIMPLE)
          .withDescription(
              Description.builder()
                  .text("Split assigner type that determine how splits are assigned to readers.")
                  .linebreak()
                  .list(
                      TextElement.text(
                          SplitAssignerType.SIMPLE
                              + ": simple assigner that doesn't provide any guarantee on order or locality."))
                  .build());

  // ==================== Lookup Join Configuration Options ====================

  /** Lookup mode enum: ALL (full load) or PARTIAL (on-demand query) */
  public enum LookupMode {
    /** Full load mode: loads the entire dimension table into memory at startup */
    ALL,
    /** On-demand query mode: reads matching records from Iceberg table only when queried */
    PARTIAL
  }

  public static final ConfigOption<LookupMode> LOOKUP_MODE =
      ConfigOptions.key("lookup.mode")
          .enumType(LookupMode.class)
          .defaultValue(LookupMode.PARTIAL)
          .withDescription(
              Description.builder()
                  .text("Lookup mode:")
                  .linebreak()
                  .list(
                      TextElement.text(LookupMode.ALL + ": Full load mode, loads the entire dimension table into memory at startup"),
                      TextElement.text(LookupMode.PARTIAL + ": On-demand query mode, reads matching records from Iceberg table only when queried"))
                  .build());

  public static final ConfigOption<Duration> LOOKUP_CACHE_TTL =
      ConfigOptions.key("lookup.cache.ttl")
          .durationType()
          .defaultValue(Duration.ofMinutes(10))
          .withDescription("Time-to-live (TTL) for cache entries. Cache entries will automatically expire and reload after this time. Default is 10 minutes.");

  public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS =
      ConfigOptions.key("lookup.cache.max-rows")
          .longType()
          .defaultValue(10000L)
          .withDescription("Maximum number of rows in cache (only effective in PARTIAL mode). Uses LRU eviction when exceeded. Default is 10000.");

  public static final ConfigOption<Duration> LOOKUP_CACHE_RELOAD_INTERVAL =
      ConfigOptions.key("lookup.cache.reload-interval")
          .durationType()
          .defaultValue(Duration.ofMinutes(10))
          .withDescription("Cache periodic reload interval (only effective in ALL mode). The system will periodically reload the latest data from the entire table at this interval. Default is 10 minutes.");

  public static final ConfigOption<Boolean> LOOKUP_ASYNC =
      ConfigOptions.key("lookup.async")
          .booleanType()
          .defaultValue(false)
          .withDescription("Whether to enable async lookup (only effective in PARTIAL mode). When enabled, async IO will be used for lookup queries to improve throughput. Default is false.");

  public static final ConfigOption<Integer> LOOKUP_ASYNC_CAPACITY =
      ConfigOptions.key("lookup.async.capacity")
          .intType()
          .defaultValue(100)
          .withDescription("Maximum number of concurrent async lookup requests (only effective when lookup.async=true). Default is 100.");

  public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES =
      ConfigOptions.key("lookup.max-retries")
          .intType()
          .defaultValue(3)
          .withDescription("Maximum number of retries when lookup query fails. Default is 3.");
}
