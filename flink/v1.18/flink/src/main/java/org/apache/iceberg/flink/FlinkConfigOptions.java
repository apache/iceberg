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
          .defaultValue(false)
          .withDescription("Use the FLIP-27 based Iceberg source implementation.");

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

  // ==================== Lookup Join 配置选项 ====================

  /** Lookup 模式枚举：ALL（全量加载）或 PARTIAL（按需查询） */
  public enum LookupMode {
    /** 全量加载模式：启动时将整个维表加载到内存 */
    ALL,
    /** 按需查询模式：仅在查询时按需从 Iceberg 表读取匹配的记录 */
    PARTIAL
  }

  public static final ConfigOption<LookupMode> LOOKUP_MODE =
      ConfigOptions.key("lookup.mode")
          .enumType(LookupMode.class)
          .defaultValue(LookupMode.PARTIAL)
          .withDescription(
              Description.builder()
                  .text("Lookup 模式：")
                  .linebreak()
                  .list(
                      TextElement.text(LookupMode.ALL + ": 全量加载模式，启动时将整个维表加载到内存"),
                      TextElement.text(LookupMode.PARTIAL + ": 按需查询模式，仅在查询时按需从 Iceberg 表读取匹配的记录"))
                  .build());

  public static final ConfigOption<Duration> LOOKUP_CACHE_TTL =
      ConfigOptions.key("lookup.cache.ttl")
          .durationType()
          .defaultValue(Duration.ofMinutes(10))
          .withDescription("缓存条目的存活时间（TTL），超过此时间后缓存条目将自动失效并重新加载。默认值为 10 分钟。");

  public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS =
      ConfigOptions.key("lookup.cache.max-rows")
          .longType()
          .defaultValue(10000L)
          .withDescription("缓存的最大行数（仅在 PARTIAL 模式下生效）。超出后采用 LRU 策略淘汰。默认值为 10000。");

  public static final ConfigOption<Duration> LOOKUP_CACHE_RELOAD_INTERVAL =
      ConfigOptions.key("lookup.cache.reload-interval")
          .durationType()
          .defaultValue(Duration.ofMinutes(10))
          .withDescription("缓存定期刷新间隔（仅在 ALL 模式下生效）。系统将按照此间隔定期重新加载整个表的最新数据。默认值为 10 分钟。");

  public static final ConfigOption<Boolean> LOOKUP_ASYNC =
      ConfigOptions.key("lookup.async")
          .booleanType()
          .defaultValue(false)
          .withDescription("是否启用异步查询（仅在 PARTIAL 模式下生效）。启用后将使用异步 IO 执行 Lookup 查询以提高吞吐量。默认值为 false。");

  public static final ConfigOption<Integer> LOOKUP_ASYNC_CAPACITY =
      ConfigOptions.key("lookup.async.capacity")
          .intType()
          .defaultValue(100)
          .withDescription("异步查询的最大并发请求数（仅在 lookup.async=true 时生效）。默认值为 100。");

  public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES =
      ConfigOptions.key("lookup.max-retries")
          .intType()
          .defaultValue(3)
          .withDescription("查询失败时的最大重试次数。默认值为 3。");
}
