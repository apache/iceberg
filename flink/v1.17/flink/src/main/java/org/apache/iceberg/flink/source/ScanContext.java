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

import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TimeUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkReadConf;
import org.apache.iceberg.flink.FlinkReadOptions;

/** Context object with optional arguments for a Flink Scan. */
@Internal
public class ScanContext implements Serializable {

  private static final long serialVersionUID = 1L;

  private final boolean caseSensitive;
  private final boolean exposeLocality;
  private final Long snapshotId;
  private final String branch;
  private final String tag;
  private final StreamingStartingStrategy startingStrategy;
  private final Long startSnapshotId;
  private final Long startSnapshotTimestamp;
  private final Long endSnapshotTimestamp;
  private final Long endSnapshotId;
  private final Long asOfTimestamp;
  private final String startTag;
  private final String endTag;
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
  private final Collection<String> includeStatsForColumns;
  private final Integer planParallelism;
  private final int maxPlanningSnapshotCount;
  private final int maxAllowedPlanningFailures;
  private final String watermarkColumn;
  private final TimeUnit watermarkColumnTimeUnit;

  private ScanContext(
      boolean caseSensitive,
      Long snapshotId,
      StreamingStartingStrategy startingStrategy,
      Long startSnapshotTimestamp,
      Long endSnapshotTimestamp,
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
      Collection<String> includeStatsForColumns,
      boolean exposeLocality,
      Integer planParallelism,
      int maxPlanningSnapshotCount,
      int maxAllowedPlanningFailures,
      String watermarkColumn,
      TimeUnit watermarkColumnTimeUnit,
      String branch,
      String tag,
      String startTag,
      String endTag) {
    this.caseSensitive = caseSensitive;
    this.snapshotId = snapshotId;
    this.tag = tag;
    this.branch = branch;
    this.startingStrategy = startingStrategy;
    this.startSnapshotTimestamp = startSnapshotTimestamp;
    this.endSnapshotTimestamp = endSnapshotTimestamp;
    this.startSnapshotId = startSnapshotId;
    this.endSnapshotId = endSnapshotId;
    this.asOfTimestamp = asOfTimestamp;
    this.startTag = startTag;
    this.endTag = endTag;
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
    this.includeStatsForColumns = includeStatsForColumns;
    this.exposeLocality = exposeLocality;
    this.planParallelism = planParallelism;
    this.maxPlanningSnapshotCount = maxPlanningSnapshotCount;
    this.maxAllowedPlanningFailures = maxAllowedPlanningFailures;
    this.watermarkColumn = watermarkColumn;
    this.watermarkColumnTimeUnit = watermarkColumnTimeUnit;
  }

  void validate() {
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

      Preconditions.checkArgument(
          tag == null,
          String.format("Cannot scan table using ref %s configured for streaming reader", tag));
      Preconditions.checkArgument(
          snapshotId == null, "Cannot set snapshot-id option for streaming reader");
      Preconditions.checkArgument(
          asOfTimestamp == null, "Cannot set as-of-timestamp option for streaming reader");
      Preconditions.checkArgument(
          endSnapshotId == null, "Cannot set end-snapshot-id option for streaming reader");
      Preconditions.checkArgument(endTag == null, "Cannot set end-tag option for streaming reader");
    }

    Preconditions.checkArgument(
        atMostOneNonNull(startSnapshotId(), startTag, startSnapshotTimestamp()),
        "Cannot specify more than one of start-snapshot-id, start-tag, or start-snapshot-timestamp.");

    Preconditions.checkArgument(
        atMostOneNonNull(endSnapshotId(), endTag, endSnapshotTimestamp()),
        "Cannot specify more than one of end-snapshot-id, end-tag, or end-snapshot-timestamp.");

    Preconditions.checkArgument(
        maxAllowedPlanningFailures >= -1,
        "Cannot set maxAllowedPlanningFailures to a negative number other than -1.");
  }

  private static boolean atMostOneNonNull(Object... objects) {
    int count = 0;
    for (Object obj : objects) {
      if (obj != null) {
        count++;
      }

      if (count > 1) {
        return false;
      }
    }
    return true;
  }

  public boolean caseSensitive() {
    return caseSensitive;
  }

  public Long snapshotId() {
    return snapshotId;
  }

  public String branch() {
    return branch;
  }

  public String tag() {
    return tag;
  }

  public String startTag() {
    return startTag;
  }

  public String endTag() {
    return endTag;
  }

  public StreamingStartingStrategy streamingStartingStrategy() {
    return startingStrategy;
  }

  public Long startSnapshotTimestamp() {
    return startSnapshotTimestamp;
  }

  public Long endSnapshotTimestamp() {
    return endSnapshotTimestamp;
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

  public Collection<String> includeStatsForColumns() {
    return includeStatsForColumns;
  }

  public boolean exposeLocality() {
    return exposeLocality;
  }

  public Integer planParallelism() {
    return planParallelism;
  }

  public int maxPlanningSnapshotCount() {
    return maxPlanningSnapshotCount;
  }

  public int maxAllowedPlanningFailures() {
    return maxAllowedPlanningFailures;
  }

  public String watermarkColumn() {
    return watermarkColumn;
  }

  public TimeUnit watermarkColumnTimeUnit() {
    return watermarkColumnTimeUnit;
  }

  public ScanContext copyWithAppendsBetween(Long newStartSnapshotId, long newEndSnapshotId) {
    return ScanContext.builder()
        .caseSensitive(caseSensitive)
        .useSnapshotId(null)
        .useBranch(branch)
        .useTag(null)
        .startSnapshotId(newStartSnapshotId)
        .endSnapshotId(newEndSnapshotId)
        .startTag(null)
        .endTag(null)
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
        .includeColumnStats(includeStatsForColumns)
        .exposeLocality(exposeLocality)
        .planParallelism(planParallelism)
        .maxPlanningSnapshotCount(maxPlanningSnapshotCount)
        .maxAllowedPlanningFailures(maxAllowedPlanningFailures)
        .watermarkColumn(watermarkColumn)
        .watermarkColumnTimeUnit(watermarkColumnTimeUnit)
        .build();
  }

  public ScanContext copyWithSnapshotId(long newSnapshotId) {
    return ScanContext.builder()
        .caseSensitive(caseSensitive)
        .useSnapshotId(newSnapshotId)
        .useBranch(branch)
        .useTag(tag)
        .startSnapshotId(null)
        .endSnapshotId(null)
        .startTag(null)
        .endTag(null)
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
        .includeColumnStats(includeStatsForColumns)
        .exposeLocality(exposeLocality)
        .planParallelism(planParallelism)
        .maxPlanningSnapshotCount(maxPlanningSnapshotCount)
        .maxAllowedPlanningFailures(maxAllowedPlanningFailures)
        .watermarkColumn(watermarkColumn)
        .watermarkColumnTimeUnit(watermarkColumnTimeUnit)
        .build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private boolean caseSensitive = FlinkReadOptions.CASE_SENSITIVE_OPTION.defaultValue();
    private Long snapshotId = FlinkReadOptions.SNAPSHOT_ID.defaultValue();
    private String branch = FlinkReadOptions.BRANCH.defaultValue();
    private String tag = FlinkReadOptions.TAG.defaultValue();
    private String startTag = FlinkReadOptions.START_TAG.defaultValue();
    private String endTag = FlinkReadOptions.END_TAG.defaultValue();
    private StreamingStartingStrategy startingStrategy =
        FlinkReadOptions.STARTING_STRATEGY_OPTION.defaultValue();
    private Long startSnapshotTimestamp = FlinkReadOptions.START_SNAPSHOT_TIMESTAMP.defaultValue();
    private Long endSnapshotTimestamp = FlinkReadOptions.END_SNAPSHOT_TIMESTAMP.defaultValue();
    private Long startSnapshotId = FlinkReadOptions.START_SNAPSHOT_ID.defaultValue();
    private Long endSnapshotId = FlinkReadOptions.END_SNAPSHOT_ID.defaultValue();
    private Long asOfTimestamp = FlinkReadOptions.AS_OF_TIMESTAMP.defaultValue();
    private Long splitSize = FlinkReadOptions.SPLIT_SIZE_OPTION.defaultValue();
    private Integer splitLookback = FlinkReadOptions.SPLIT_LOOKBACK_OPTION.defaultValue();
    private Long splitOpenFileCost = FlinkReadOptions.SPLIT_FILE_OPEN_COST_OPTION.defaultValue();
    private boolean isStreaming = FlinkReadOptions.STREAMING_OPTION.defaultValue();
    private Duration monitorInterval =
        TimeUtils.parseDuration(FlinkReadOptions.MONITOR_INTERVAL_OPTION.defaultValue());
    private String nameMapping;
    private Schema projectedSchema;
    private List<Expression> filters;
    private long limit = FlinkReadOptions.LIMIT_OPTION.defaultValue();
    private boolean includeColumnStats =
        FlinkReadOptions.INCLUDE_COLUMN_STATS_OPTION.defaultValue();
    private Collection<String> includeStatsForColumns = null;
    private boolean exposeLocality;
    private Integer planParallelism =
        FlinkConfigOptions.TABLE_EXEC_ICEBERG_WORKER_POOL_SIZE.defaultValue();
    private int maxPlanningSnapshotCount =
        FlinkReadOptions.MAX_PLANNING_SNAPSHOT_COUNT_OPTION.defaultValue();
    private int maxAllowedPlanningFailures =
        FlinkReadOptions.MAX_ALLOWED_PLANNING_FAILURES_OPTION.defaultValue();
    private String watermarkColumn = FlinkReadOptions.WATERMARK_COLUMN_OPTION.defaultValue();
    private TimeUnit watermarkColumnTimeUnit =
        FlinkReadOptions.WATERMARK_COLUMN_TIME_UNIT_OPTION.defaultValue();

    private Builder() {}

    public Builder caseSensitive(boolean newCaseSensitive) {
      this.caseSensitive = newCaseSensitive;
      return this;
    }

    public Builder useSnapshotId(Long newSnapshotId) {
      this.snapshotId = newSnapshotId;
      return this;
    }

    public Builder useTag(String newTag) {
      this.tag = newTag;
      return this;
    }

    public Builder useBranch(String newBranch) {
      this.branch = newBranch;
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

    public Builder endSnapshotTimestamp(Long newEndSnapshotTimestamp) {
      this.endSnapshotTimestamp = newEndSnapshotTimestamp;
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

    public Builder startTag(String newStartTag) {
      this.startTag = newStartTag;
      return this;
    }

    public Builder endTag(String newEndTag) {
      this.endTag = newEndTag;
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

    public Builder includeColumnStats(Collection<String> newIncludeStatsForColumns) {
      this.includeStatsForColumns = newIncludeStatsForColumns;
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

    public Builder maxPlanningSnapshotCount(int newMaxPlanningSnapshotCount) {
      this.maxPlanningSnapshotCount = newMaxPlanningSnapshotCount;
      return this;
    }

    public Builder maxAllowedPlanningFailures(int newMaxAllowedPlanningFailures) {
      this.maxAllowedPlanningFailures = newMaxAllowedPlanningFailures;
      return this;
    }

    public Builder watermarkColumn(String newWatermarkColumn) {
      this.watermarkColumn = newWatermarkColumn;
      return this;
    }

    public Builder watermarkColumnTimeUnit(TimeUnit newWatermarkTimeUnit) {
      this.watermarkColumnTimeUnit = newWatermarkTimeUnit;
      return this;
    }

    public Builder resolveConfig(
        Table table, Map<String, String> readOptions, ReadableConfig readableConfig) {
      FlinkReadConf flinkReadConf = new FlinkReadConf(table, readOptions, readableConfig);

      return this.useSnapshotId(flinkReadConf.snapshotId())
          .useTag(flinkReadConf.tag())
          .useBranch(flinkReadConf.branch())
          .startTag(flinkReadConf.startTag())
          .endTag(flinkReadConf.endTag())
          .caseSensitive(flinkReadConf.caseSensitive())
          .asOfTimestamp(flinkReadConf.asOfTimestamp())
          .startingStrategy(flinkReadConf.startingStrategy())
          .startSnapshotTimestamp(flinkReadConf.startSnapshotTimestamp())
          .endSnapshotTimestamp(flinkReadConf.endSnapshotTimestamp())
          .startSnapshotId(flinkReadConf.startSnapshotId())
          .endSnapshotId(flinkReadConf.endSnapshotId())
          .splitSize(flinkReadConf.splitSize())
          .splitLookback(flinkReadConf.splitLookback())
          .splitOpenFileCost(flinkReadConf.splitFileOpenCost())
          .streaming(flinkReadConf.streaming())
          .monitorInterval(flinkReadConf.monitorInterval())
          .nameMapping(flinkReadConf.nameMapping())
          .limit(flinkReadConf.limit())
          .planParallelism(flinkReadConf.workerPoolSize())
          .includeColumnStats(flinkReadConf.includeColumnStats())
          .maxPlanningSnapshotCount(flinkReadConf.maxPlanningSnapshotCount())
          .maxAllowedPlanningFailures(maxAllowedPlanningFailures)
          .watermarkColumn(flinkReadConf.watermarkColumn())
          .watermarkColumnTimeUnit(flinkReadConf.watermarkColumnTimeUnit());
    }

    public ScanContext build() {
      return new ScanContext(
          caseSensitive,
          snapshotId,
          startingStrategy,
          startSnapshotTimestamp,
          endSnapshotTimestamp,
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
          includeStatsForColumns,
          exposeLocality,
          planParallelism,
          maxPlanningSnapshotCount,
          maxAllowedPlanningFailures,
          watermarkColumn,
          watermarkColumnTimeUnit,
          branch,
          tag,
          startTag,
          endTag);
    }
  }
}
