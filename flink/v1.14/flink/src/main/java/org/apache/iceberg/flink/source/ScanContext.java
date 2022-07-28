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

import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkConfigOptions;

/** Context object with optional arguments for a Flink Scan. */
@Internal
public class ScanContext implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final ConfigOption<Long> SNAPSHOT_ID =
      ConfigOptions.key("snapshot-id").longType().defaultValue(null);

  private static final ConfigOption<Boolean> CASE_SENSITIVE =
      ConfigOptions.key("case-sensitive").booleanType().defaultValue(false);

  private static final ConfigOption<Long> AS_OF_TIMESTAMP =
      ConfigOptions.key("as-of-timestamp").longType().defaultValue(null);

  private static final ConfigOption<StreamingStartingStrategy> STARTING_STRATEGY =
      ConfigOptions.key("starting-strategy")
          .enumType(StreamingStartingStrategy.class)
          .defaultValue(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT);

  private static final ConfigOption<Long> START_SNAPSHOT_TIMESTAMP =
      ConfigOptions.key("start-snapshot-timestamp").longType().defaultValue(null);

  private static final ConfigOption<Long> START_SNAPSHOT_ID =
      ConfigOptions.key("start-snapshot-id").longType().defaultValue(null);

  private static final ConfigOption<Long> END_SNAPSHOT_ID =
      ConfigOptions.key("end-snapshot-id").longType().defaultValue(null);

  private static final ConfigOption<Long> SPLIT_SIZE =
      ConfigOptions.key("split-size").longType().defaultValue(null);

  private static final ConfigOption<Integer> SPLIT_LOOKBACK =
      ConfigOptions.key("split-lookback").intType().defaultValue(null);

  private static final ConfigOption<Long> SPLIT_FILE_OPEN_COST =
      ConfigOptions.key("split-file-open-cost").longType().defaultValue(null);

  private static final ConfigOption<Boolean> STREAMING =
      ConfigOptions.key("streaming").booleanType().defaultValue(false);

  private static final ConfigOption<Duration> MONITOR_INTERVAL =
      ConfigOptions.key("monitor-interval").durationType().defaultValue(Duration.ofSeconds(10));

  private static final ConfigOption<Boolean> INCLUDE_COLUMN_STATS =
      ConfigOptions.key("include-column-stats").booleanType().defaultValue(false);

  private final boolean caseSensitive;
  private final boolean exposeLocality;
  private final Long snapshotId;
  private final StreamingStartingStrategy startingStrategy;
  private final Long startSnapshotId;
  private final Long startSnapshotTimestamp;
  private final Long endSnapshotId;
  private final Long asOfTimestamp;
  private final Long splitSize;
  private final Integer splitLookback;
  private final Long splitOpenFileCost;
  private final boolean isStreaming;
  private final Duration monitorInterval;

  private final String nameMapping;
  private final Schema schema;
  private final List<Expression> filters;
  private final long limit;
  private final boolean includeColumnStats;
  private final Integer planParallelism;

  private ScanContext(
      boolean caseSensitive,
      Long snapshotId,
      StreamingStartingStrategy startingStrategy,
      Long startSnapshotTimestamp,
      Long startSnapshotId,
      Long endSnapshotId,
      Long asOfTimestamp,
      Long splitSize,
      Integer splitLookback,
      Long splitOpenFileCost,
      boolean isStreaming,
      Duration monitorInterval,
      String nameMapping,
      Schema schema,
      List<Expression> filters,
      long limit,
      boolean includeColumnStats,
      boolean exposeLocality,
      Integer planParallelism) {
    this.caseSensitive = caseSensitive;
    this.snapshotId = snapshotId;
    this.startingStrategy = startingStrategy;
    this.startSnapshotTimestamp = startSnapshotTimestamp;
    this.startSnapshotId = startSnapshotId;
    this.endSnapshotId = endSnapshotId;
    this.asOfTimestamp = asOfTimestamp;
    this.splitSize = splitSize;
    this.splitLookback = splitLookback;
    this.splitOpenFileCost = splitOpenFileCost;
    this.isStreaming = isStreaming;
    this.monitorInterval = monitorInterval;

    this.nameMapping = nameMapping;
    this.schema = schema;
    this.filters = filters;
    this.limit = limit;
    this.includeColumnStats = includeColumnStats;
    this.exposeLocality = exposeLocality;
    this.planParallelism = planParallelism;

    validate();
  }

  private void validate() {
    if (isStreaming) {
      if (startingStrategy == StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_ID) {
        Preconditions.checkArgument(
            startSnapshotId != null,
            "Invalid starting snapshot id for SPECIFIC_START_SNAPSHOT_ID strategy: null");
        Preconditions.checkArgument(
            startSnapshotTimestamp == null,
            "Invalid starting snapshot timestamp for SPECIFIC_START_SNAPSHOT_ID strategy: not null");
      }
      if (startingStrategy == StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP) {
        Preconditions.checkArgument(
            startSnapshotTimestamp != null,
            "Invalid starting snapshot timestamp for SPECIFIC_START_SNAPSHOT_TIMESTAMP strategy: null");
        Preconditions.checkArgument(
            startSnapshotId == null,
            "Invalid starting snapshot id for SPECIFIC_START_SNAPSHOT_ID strategy: not null");
      }
    }
  }

  public boolean caseSensitive() {
    return caseSensitive;
  }

  public Long snapshotId() {
    return snapshotId;
  }

  public StreamingStartingStrategy streamingStartingStrategy() {
    return startingStrategy;
  }

  public Long startSnapshotTimestamp() {
    return startSnapshotTimestamp;
  }

  public Long startSnapshotId() {
    return startSnapshotId;
  }

  public Long endSnapshotId() {
    return endSnapshotId;
  }

  public Long asOfTimestamp() {
    return asOfTimestamp;
  }

  public Long splitSize() {
    return splitSize;
  }

  public Integer splitLookback() {
    return splitLookback;
  }

  public Long splitOpenFileCost() {
    return splitOpenFileCost;
  }

  public boolean isStreaming() {
    return isStreaming;
  }

  public Duration monitorInterval() {
    return monitorInterval;
  }

  public String nameMapping() {
    return nameMapping;
  }

  public Schema project() {
    return schema;
  }

  public List<Expression> filters() {
    return filters;
  }

  public long limit() {
    return limit;
  }

  public boolean includeColumnStats() {
    return includeColumnStats;
  }

  public boolean exposeLocality() {
    return exposeLocality;
  }

  public Integer planParallelism() {
    return planParallelism;
  }

  public ScanContext copyWithAppendsBetween(Long newStartSnapshotId, long newEndSnapshotId) {
    return ScanContext.builder()
        .caseSensitive(caseSensitive)
        .useSnapshotId(null)
        .startSnapshotId(newStartSnapshotId)
        .endSnapshotId(newEndSnapshotId)
        .asOfTimestamp(null)
        .splitSize(splitSize)
        .splitLookback(splitLookback)
        .splitOpenFileCost(splitOpenFileCost)
        .streaming(isStreaming)
        .monitorInterval(monitorInterval)
        .nameMapping(nameMapping)
        .project(schema)
        .filters(filters)
        .limit(limit)
        .includeColumnStats(includeColumnStats)
        .exposeLocality(exposeLocality)
        .planParallelism(planParallelism)
        .build();
  }

  public ScanContext copyWithSnapshotId(long newSnapshotId) {
    return ScanContext.builder()
        .caseSensitive(caseSensitive)
        .useSnapshotId(newSnapshotId)
        .startSnapshotId(null)
        .endSnapshotId(null)
        .asOfTimestamp(null)
        .splitSize(splitSize)
        .splitLookback(splitLookback)
        .splitOpenFileCost(splitOpenFileCost)
        .streaming(isStreaming)
        .monitorInterval(monitorInterval)
        .nameMapping(nameMapping)
        .project(schema)
        .filters(filters)
        .limit(limit)
        .includeColumnStats(includeColumnStats)
        .exposeLocality(exposeLocality)
        .planParallelism(planParallelism)
        .build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private boolean caseSensitive = CASE_SENSITIVE.defaultValue();
    private Long snapshotId = SNAPSHOT_ID.defaultValue();
    private StreamingStartingStrategy startingStrategy = STARTING_STRATEGY.defaultValue();
    private Long startSnapshotTimestamp = START_SNAPSHOT_TIMESTAMP.defaultValue();
    private Long startSnapshotId = START_SNAPSHOT_ID.defaultValue();
    private Long endSnapshotId = END_SNAPSHOT_ID.defaultValue();
    private Long asOfTimestamp = AS_OF_TIMESTAMP.defaultValue();
    private Long splitSize = SPLIT_SIZE.defaultValue();
    private Integer splitLookback = SPLIT_LOOKBACK.defaultValue();
    private Long splitOpenFileCost = SPLIT_FILE_OPEN_COST.defaultValue();
    private boolean isStreaming = STREAMING.defaultValue();
    private Duration monitorInterval = MONITOR_INTERVAL.defaultValue();
    private String nameMapping;
    private Schema projectedSchema;
    private List<Expression> filters;
    private long limit = -1L;
    private boolean includeColumnStats = INCLUDE_COLUMN_STATS.defaultValue();
    private boolean exposeLocality;
    private Integer planParallelism =
        FlinkConfigOptions.TABLE_EXEC_ICEBERG_WORKER_POOL_SIZE.defaultValue();

    private Builder() {}

    public Builder caseSensitive(boolean newCaseSensitive) {
      this.caseSensitive = newCaseSensitive;
      return this;
    }

    public Builder useSnapshotId(Long newSnapshotId) {
      this.snapshotId = newSnapshotId;
      return this;
    }

    public Builder startingStrategy(StreamingStartingStrategy newStartingStrategy) {
      this.startingStrategy = newStartingStrategy;
      return this;
    }

    public Builder startSnapshotTimestamp(Long newStartSnapshotTimestamp) {
      this.startSnapshotTimestamp = newStartSnapshotTimestamp;
      return this;
    }

    public Builder startSnapshotId(Long newStartSnapshotId) {
      this.startSnapshotId = newStartSnapshotId;
      return this;
    }

    public Builder endSnapshotId(Long newEndSnapshotId) {
      this.endSnapshotId = newEndSnapshotId;
      return this;
    }

    public Builder asOfTimestamp(Long newAsOfTimestamp) {
      this.asOfTimestamp = newAsOfTimestamp;
      return this;
    }

    public Builder splitSize(Long newSplitSize) {
      this.splitSize = newSplitSize;
      return this;
    }

    public Builder splitLookback(Integer newSplitLookback) {
      this.splitLookback = newSplitLookback;
      return this;
    }

    public Builder splitOpenFileCost(Long newSplitOpenFileCost) {
      this.splitOpenFileCost = newSplitOpenFileCost;
      return this;
    }

    public Builder streaming(boolean streaming) {
      this.isStreaming = streaming;
      return this;
    }

    public Builder monitorInterval(Duration newMonitorInterval) {
      this.monitorInterval = newMonitorInterval;
      return this;
    }

    public Builder nameMapping(String newNameMapping) {
      this.nameMapping = newNameMapping;
      return this;
    }

    public Builder project(Schema newProjectedSchema) {
      this.projectedSchema = newProjectedSchema;
      return this;
    }

    public Builder filters(List<Expression> newFilters) {
      this.filters = newFilters;
      return this;
    }

    public Builder limit(long newLimit) {
      this.limit = newLimit;
      return this;
    }

    public Builder includeColumnStats(boolean newIncludeColumnStats) {
      this.includeColumnStats = newIncludeColumnStats;
      return this;
    }

    public Builder exposeLocality(boolean newExposeLocality) {
      this.exposeLocality = newExposeLocality;
      return this;
    }

    public Builder planParallelism(Integer parallelism) {
      this.planParallelism = parallelism;
      return this;
    }

    public Builder fromProperties(Map<String, String> properties) {
      Configuration config = new Configuration();
      properties.forEach(config::setString);

      return this.useSnapshotId(config.get(SNAPSHOT_ID))
          .caseSensitive(config.get(CASE_SENSITIVE))
          .asOfTimestamp(config.get(AS_OF_TIMESTAMP))
          .startingStrategy(config.get(STARTING_STRATEGY))
          .startSnapshotTimestamp(config.get(START_SNAPSHOT_TIMESTAMP))
          .startSnapshotId(config.get(START_SNAPSHOT_ID))
          .endSnapshotId(config.get(END_SNAPSHOT_ID))
          .splitSize(config.get(SPLIT_SIZE))
          .splitLookback(config.get(SPLIT_LOOKBACK))
          .splitOpenFileCost(config.get(SPLIT_FILE_OPEN_COST))
          .streaming(config.get(STREAMING))
          .monitorInterval(config.get(MONITOR_INTERVAL))
          .nameMapping(properties.get(DEFAULT_NAME_MAPPING))
          .includeColumnStats(config.get(INCLUDE_COLUMN_STATS));
    }

    public ScanContext build() {
      return new ScanContext(
          caseSensitive,
          snapshotId,
          startingStrategy,
          startSnapshotTimestamp,
          startSnapshotId,
          endSnapshotId,
          asOfTimestamp,
          splitSize,
          splitLookback,
          splitOpenFileCost,
          isStreaming,
          monitorInterval,
          nameMapping,
          projectedSchema,
          filters,
          limit,
          includeColumnStats,
          exposeLocality,
          planParallelism);
    }
  }
}
