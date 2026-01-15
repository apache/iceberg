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
package org.apache.iceberg.flink.source;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkFilters;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.assigner.SplitAssignerType;
import org.apache.iceberg.flink.source.lookup.IcebergAllLookupFunction;
import org.apache.iceberg.flink.source.lookup.IcebergAsyncLookupFunction;
import org.apache.iceberg.flink.source.lookup.IcebergPartialLookupFunction;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Flink Iceberg table source. */
@Internal
public class IcebergTableSource
    implements ScanTableSource,
        LookupTableSource,
        SupportsProjectionPushDown,
        SupportsFilterPushDown,
        SupportsLimitPushDown {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergTableSource.class);

  private int[] projectedFields;
  private Long limit;
  private List<Expression> filters;

  private final TableLoader loader;
  private final TableSchema schema;
  private final Map<String, String> properties;
  private final boolean isLimitPushDown;
  private final ReadableConfig readableConfig;

  private IcebergTableSource(IcebergTableSource toCopy) {
    this.loader = toCopy.loader;
    this.schema = toCopy.schema;
    this.properties = toCopy.properties;
    this.projectedFields = toCopy.projectedFields;
    this.isLimitPushDown = toCopy.isLimitPushDown;
    this.limit = toCopy.limit;
    this.filters = toCopy.filters;
    this.readableConfig = toCopy.readableConfig;
  }

  public IcebergTableSource(
      TableLoader loader,
      TableSchema schema,
      Map<String, String> properties,
      ReadableConfig readableConfig) {
    this(loader, schema, properties, null, false, null, ImmutableList.of(), readableConfig);
  }

  private IcebergTableSource(
      TableLoader loader,
      TableSchema schema,
      Map<String, String> properties,
      int[] projectedFields,
      boolean isLimitPushDown,
      Long limit,
      List<Expression> filters,
      ReadableConfig readableConfig) {
    this.loader = loader;
    this.schema = schema;
    this.properties = properties;
    this.projectedFields = projectedFields;
    this.isLimitPushDown = isLimitPushDown;
    this.limit = limit;
    this.filters = filters;
    this.readableConfig = readableConfig;
  }

  @Override
  public void applyProjection(int[][] projectFields) {
    this.projectedFields = new int[projectFields.length];
    for (int i = 0; i < projectFields.length; i++) {
      Preconditions.checkArgument(
          projectFields[i].length == 1, "Don't support nested projection in iceberg source now.");
      this.projectedFields[i] = projectFields[i][0];
    }
  }

  private DataStream<RowData> createDataStream(StreamExecutionEnvironment execEnv) {
    return FlinkSource.forRowData()
        .env(execEnv)
        .tableLoader(loader)
        .properties(properties)
        .project(getProjectedSchema())
        .limit(limit)
        .filters(filters)
        .flinkConf(readableConfig)
        .build();
  }

  private DataStreamSource<RowData> createFLIP27Stream(StreamExecutionEnvironment env) {
    SplitAssignerType assignerType =
        readableConfig.get(FlinkConfigOptions.TABLE_EXEC_SPLIT_ASSIGNER_TYPE);
    IcebergSource<RowData> source =
        IcebergSource.forRowData()
            .tableLoader(loader)
            .assignerFactory(assignerType.factory())
            .properties(properties)
            .project(getProjectedSchema())
            .limit(limit)
            .filters(filters)
            .flinkConfig(readableConfig)
            .build();
    DataStreamSource stream =
        env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            source.name(),
            TypeInformation.of(RowData.class));
    return stream;
  }

  private TableSchema getProjectedSchema() {
    if (projectedFields == null) {
      return schema;
    } else {
      String[] fullNames = schema.getFieldNames();
      DataType[] fullTypes = schema.getFieldDataTypes();
      return TableSchema.builder()
          .fields(
              Arrays.stream(projectedFields).mapToObj(i -> fullNames[i]).toArray(String[]::new),
              Arrays.stream(projectedFields).mapToObj(i -> fullTypes[i]).toArray(DataType[]::new))
          .build();
    }
  }

  @Override
  public void applyLimit(long newLimit) {
    this.limit = newLimit;
  }

  @Override
  public Result applyFilters(List<ResolvedExpression> flinkFilters) {
    List<ResolvedExpression> acceptedFilters = Lists.newArrayList();
    List<Expression> expressions = Lists.newArrayList();

    for (ResolvedExpression resolvedExpression : flinkFilters) {
      Optional<Expression> icebergExpression = FlinkFilters.convert(resolvedExpression);
      if (icebergExpression.isPresent()) {
        expressions.add(icebergExpression.get());
        acceptedFilters.add(resolvedExpression);
      }
    }

    this.filters = expressions;
    return Result.of(acceptedFilters, flinkFilters);
  }

  @Override
  public boolean supportsNestedProjection() {
    // TODO: support nested projection
    return false;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
    return new DataStreamScanProvider() {
      @Override
      public DataStream<RowData> produceDataStream(
          ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
        if (readableConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_FLIP27_SOURCE)) {
          return createFLIP27Stream(execEnv);
        } else {
          return createDataStream(execEnv);
        }
      }

      @Override
      public boolean isBounded() {
        return FlinkSource.isBounded(properties);
      }
    };
  }

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
    // 获取 Lookup 键信息
    int[][] lookupKeys = context.getKeys();
    Preconditions.checkArgument(
        lookupKeys.length > 0, "At least one lookup key is required for Lookup Join");

    // 提取 Lookup 键索引（原始表 Schema 中的索引）和名称
    int[] originalKeyIndices = new int[lookupKeys.length];
    String[] lookupKeyNames = new String[lookupKeys.length];
    String[] fieldNames = schema.getFieldNames();

    for (int i = 0; i < lookupKeys.length; i++) {
      Preconditions.checkArgument(
          lookupKeys[i].length == 1,
          "Nested lookup keys are not supported, lookup key: %s",
          Arrays.toString(lookupKeys[i]));
      int keyIndex = lookupKeys[i][0];
      originalKeyIndices[i] = keyIndex;
      lookupKeyNames[i] = fieldNames[keyIndex];
    }

    LOG.info("Creating Lookup runtime provider with keys: {}", Arrays.toString(lookupKeyNames));

    // 获取投影后的 Schema
    Schema icebergProjectedSchema = getIcebergProjectedSchema();

    // 计算 lookup key 在投影后 Schema 中的索引
    // 如果有投影（projectedFields != null），需要映射到新索引
    // 如果没有投影，索引保持不变
    int[] lookupKeyIndices = computeProjectedKeyIndices(originalKeyIndices);

    LOG.info(
        "Lookup key indices - original: {}, projected: {}",
        Arrays.toString(originalKeyIndices),
        Arrays.toString(lookupKeyIndices));

    // 获取 Lookup 配置
    FlinkConfigOptions.LookupMode lookupMode = getLookupMode();
    Duration cacheTtl = getCacheTtl();
    long cacheMaxRows = getCacheMaxRows();
    Duration reloadInterval = getReloadInterval();
    boolean asyncEnabled = isAsyncLookupEnabled();
    int asyncCapacity = getAsyncCapacity();
    int maxRetries = getMaxRetries();

    LOG.info(
        "Lookup configuration - mode: {}, cacheTtl: {}, cacheMaxRows: {}, reloadInterval: {}, async: {}, asyncCapacity: {}, maxRetries: {}",
        lookupMode,
        cacheTtl,
        cacheMaxRows,
        reloadInterval,
        asyncEnabled,
        asyncCapacity,
        maxRetries);

    // 根据配置创建对应的 LookupFunction
    if (lookupMode == FlinkConfigOptions.LookupMode.ALL) {
      // ALL 模式：全量加载
      IcebergAllLookupFunction lookupFunction =
          new IcebergAllLookupFunction(
              loader.clone(),
              icebergProjectedSchema,
              lookupKeyIndices,
              lookupKeyNames,
              true, // caseSensitive
              reloadInterval);
      return TableFunctionProvider.of(lookupFunction);

    } else {
      // PARTIAL 模式：按需查询
      if (asyncEnabled) {
        // 异步模式
        IcebergAsyncLookupFunction asyncLookupFunction =
            new IcebergAsyncLookupFunction(
                loader.clone(),
                icebergProjectedSchema,
                lookupKeyIndices,
                lookupKeyNames,
                true, // caseSensitive
                cacheTtl,
                cacheMaxRows,
                maxRetries,
                asyncCapacity);
        return AsyncLookupFunctionProvider.of(asyncLookupFunction);

      } else {
        // 同步模式
        IcebergPartialLookupFunction lookupFunction =
            new IcebergPartialLookupFunction(
                loader.clone(),
                icebergProjectedSchema,
                lookupKeyIndices,
                lookupKeyNames,
                true, // caseSensitive
                cacheTtl,
                cacheMaxRows,
                maxRetries);
        return TableFunctionProvider.of(lookupFunction);
      }
    }
  }

  /**
   * 计算 lookup key 在投影后 Schema 中的索引
   *
   * @param originalKeyIndices 原始表 Schema 中的 lookup key 索引
   * @return 投影后 Schema 中的 lookup key 索引
   */
  private int[] computeProjectedKeyIndices(int[] originalKeyIndices) {
    if (projectedFields == null) {
      // 没有投影，索引保持不变
      return originalKeyIndices;
    }

    int[] projectedKeyIndices = new int[originalKeyIndices.length];
    for (int i = 0; i < originalKeyIndices.length; i++) {
      int originalIndex = originalKeyIndices[i];
      int projectedIndex = -1;

      // 在 projectedFields 中查找原始索引的位置
      for (int j = 0; j < projectedFields.length; j++) {
        if (projectedFields[j] == originalIndex) {
          projectedIndex = j;
          break;
        }
      }

      Preconditions.checkArgument(
          projectedIndex >= 0,
          "Lookup key at original index %s is not in projected fields: %s",
          originalIndex,
          Arrays.toString(projectedFields));

      projectedKeyIndices[i] = projectedIndex;
    }

    return projectedKeyIndices;
  }

  /**
   * 获取 Iceberg 投影 Schema（保留原始字段 ID）
   *
   * <p>重要：必须使用原始 Iceberg 表的字段 ID，否则 RowDataFileScanTaskReader 无法正确投影数据
   */
  private Schema getIcebergProjectedSchema() {
    // 加载原始 Iceberg 表获取其 Schema
    if (!loader.isOpen()) {
      loader.open();
    }
    Schema icebergTableSchema = loader.loadTable().schema();

    if (projectedFields == null) {
      // 没有投影，返回完整 Schema
      return icebergTableSchema;
    }

    // 根据投影字段选择原始 Iceberg Schema 中的列
    String[] fullNames = schema.getFieldNames();
    List<String> projectedNames = Lists.newArrayList();
    for (int fieldIndex : projectedFields) {
      projectedNames.add(fullNames[fieldIndex]);
    }

    // 使用 Iceberg 的 Schema.select() 方法，保留原始字段 ID
    return icebergTableSchema.select(projectedNames);
  }

  /** 获取 Lookup 模式配置 */
  private FlinkConfigOptions.LookupMode getLookupMode() {
    // 优先从表属性读取，然后从 readableConfig 读取
    String modeStr = properties.get("lookup.mode");
    if (modeStr != null) {
      try {
        return FlinkConfigOptions.LookupMode.valueOf(modeStr.toUpperCase());
      } catch (IllegalArgumentException e) {
        LOG.debug("Invalid lookup.mode value: {}, using default", modeStr, e);
      }
    }
    return readableConfig.get(FlinkConfigOptions.LOOKUP_MODE);
  }

  /** 获取缓存 TTL 配置 */
  private Duration getCacheTtl() {
    String ttlStr = properties.get("lookup.cache.ttl");
    if (ttlStr != null) {
      try {
        return parseDuration(ttlStr);
      } catch (Exception e) {
        LOG.debug("Invalid lookup.cache.ttl value: {}, using default", ttlStr, e);
      }
    }
    return readableConfig.get(FlinkConfigOptions.LOOKUP_CACHE_TTL);
  }

  /** 获取缓存最大行数配置 */
  private long getCacheMaxRows() {
    String maxRowsStr = properties.get("lookup.cache.max-rows");
    if (maxRowsStr != null) {
      try {
        return Long.parseLong(maxRowsStr);
      } catch (NumberFormatException e) {
        LOG.debug("Invalid lookup.cache.max-rows value: {}, using default", maxRowsStr, e);
      }
    }
    return readableConfig.get(FlinkConfigOptions.LOOKUP_CACHE_MAX_ROWS);
  }

  /** 获取缓存刷新间隔配置 */
  private Duration getReloadInterval() {
    String intervalStr = properties.get("lookup.cache.reload-interval");
    if (intervalStr != null) {
      try {
        return parseDuration(intervalStr);
      } catch (Exception e) {
        LOG.debug("Invalid lookup.cache.reload-interval value: {}, using default", intervalStr, e);
      }
    }
    return readableConfig.get(FlinkConfigOptions.LOOKUP_CACHE_RELOAD_INTERVAL);
  }

  /** 是否启用异步 Lookup */
  private boolean isAsyncLookupEnabled() {
    String asyncStr = properties.get("lookup.async");
    if (asyncStr != null) {
      return Boolean.parseBoolean(asyncStr);
    }
    return readableConfig.get(FlinkConfigOptions.LOOKUP_ASYNC);
  }

  /** 获取异步 Lookup 并发容量 */
  private int getAsyncCapacity() {
    String capacityStr = properties.get("lookup.async.capacity");
    if (capacityStr != null) {
      try {
        return Integer.parseInt(capacityStr);
      } catch (NumberFormatException e) {
        LOG.debug("Invalid lookup.async.capacity value: {}, using default", capacityStr, e);
      }
    }
    return readableConfig.get(FlinkConfigOptions.LOOKUP_ASYNC_CAPACITY);
  }

  /** 获取最大重试次数 */
  private int getMaxRetries() {
    String retriesStr = properties.get("lookup.max-retries");
    if (retriesStr != null) {
      try {
        return Integer.parseInt(retriesStr);
      } catch (NumberFormatException e) {
        LOG.debug("Invalid lookup.max-retries value: {}, using default", retriesStr, e);
      }
    }
    return readableConfig.get(FlinkConfigOptions.LOOKUP_MAX_RETRIES);
  }

  /** 解析 Duration 字符串 支持格式：10m, 1h, 30s, PT10M 等 */
  private Duration parseDuration(String durationStr) {
    String normalized = durationStr.trim().toLowerCase();

    // 尝试 ISO-8601 格式
    if (normalized.startsWith("pt")) {
      return Duration.parse(normalized.toUpperCase());
    }

    // 简单格式解析
    char unit = normalized.charAt(normalized.length() - 1);
    long value = Long.parseLong(normalized.substring(0, normalized.length() - 1));

    switch (unit) {
      case 's':
        return Duration.ofSeconds(value);
      case 'm':
        return Duration.ofMinutes(value);
      case 'h':
        return Duration.ofHours(value);
      case 'd':
        return Duration.ofDays(value);
      default:
        // 默认为毫秒
        return Duration.ofMillis(Long.parseLong(durationStr));
    }
  }

  @Override
  public DynamicTableSource copy() {
    return new IcebergTableSource(this);
  }

  @Override
  public String asSummaryString() {
    return "Iceberg table source";
  }
}
