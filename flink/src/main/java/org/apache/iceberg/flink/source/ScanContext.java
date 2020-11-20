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
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;

import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;

/**
 * Context object with optional arguments for a Flink Scan.
 */
class ScanContext implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final ConfigOption<Long> SNAPSHOT_ID =
      ConfigOptions.key("snapshot-id").longType().defaultValue(null);

  private static final ConfigOption<Boolean> CASE_SENSITIVE =
      ConfigOptions.key("case-sensitive").booleanType().defaultValue(false);

  private static final ConfigOption<Long> AS_OF_TIMESTAMP =
      ConfigOptions.key("as-of-timestamp").longType().defaultValue(null);

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

  public static final ConfigOption<Boolean> STREAMING =
      ConfigOptions.key("streaming").booleanType().defaultValue(false);

  public static final ConfigOption<Duration> MONITOR_INTERVAL =
      ConfigOptions.key("monitor-interval").durationType().defaultValue(Duration.ofSeconds(10));

  private final boolean caseSensitive;
  private final Long snapshotId;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private final Long asOfTimestamp;
  private final Long splitSize;
  private final Integer splitLookback;
  private final Long splitOpenFileCost;
  private final boolean isStreaming;
  private final Duration monitorInterval;

  private final String nameMapping;
  private final Schema projectedSchema;
  private final List<Expression> filterExpressions;
  private final Long limit;

  ScanContext() {
    this.caseSensitive = CASE_SENSITIVE.defaultValue();
    this.snapshotId = SNAPSHOT_ID.defaultValue();
    this.startSnapshotId = START_SNAPSHOT_ID.defaultValue();
    this.endSnapshotId = END_SNAPSHOT_ID.defaultValue();
    this.asOfTimestamp = AS_OF_TIMESTAMP.defaultValue();
    this.splitSize = SPLIT_SIZE.defaultValue();
    this.splitLookback = SPLIT_LOOKBACK.defaultValue();
    this.splitOpenFileCost = SPLIT_FILE_OPEN_COST.defaultValue();
    this.isStreaming = STREAMING.defaultValue();
    this.monitorInterval = MONITOR_INTERVAL.defaultValue();

    this.nameMapping = null;
    this.projectedSchema = null;
    this.filterExpressions = null;
    this.limit = null;
  }

  private ScanContext(boolean caseSensitive, Long snapshotId, Long startSnapshotId, Long endSnapshotId,
                      Long asOfTimestamp, Long splitSize, Integer splitLookback, Long splitOpenFileCost,
                      boolean isStreaming, Duration monitorInterval, String nameMapping,
                      Schema projectedSchema, List<Expression> filterExpressions, Long limit) {
    this.caseSensitive = caseSensitive;
    this.snapshotId = snapshotId;
    this.startSnapshotId = startSnapshotId;
    this.endSnapshotId = endSnapshotId;
    this.asOfTimestamp = asOfTimestamp;
    this.splitSize = splitSize;
    this.splitLookback = splitLookback;
    this.splitOpenFileCost = splitOpenFileCost;
    this.isStreaming = isStreaming;
    this.monitorInterval = monitorInterval;

    this.nameMapping = nameMapping;
    this.projectedSchema = projectedSchema;
    this.filterExpressions = filterExpressions;
    this.limit = limit;
  }

  boolean caseSensitive() {
    return caseSensitive;
  }

  Long snapshotId() {
    return snapshotId;
  }

  Long startSnapshotId() {
    return startSnapshotId;
  }

  Long endSnapshotId() {
    return endSnapshotId;
  }

  Long asOfTimestamp() {
    return asOfTimestamp;
  }

  Long splitSize() {
    return splitSize;
  }

  Integer splitLookback() {
    return splitLookback;
  }

  Long splitOpenFileCost() {
    return splitOpenFileCost;
  }

  boolean isStreaming() {
    return isStreaming;
  }

  Duration monitorInterval() {
    return monitorInterval;
  }

  String nameMapping() {
    return nameMapping;
  }

  Schema projectedSchema() {
    return projectedSchema;
  }

  List<Expression> filterExpressions() {
    return filterExpressions;
  }

  long limit() {
    return limit;
  }

  ScanContext copyWithAppendsBetween(long newStartSnapshotId, long newEndSnapshotId) {
    return ScanContext.builder()
        .caseSensitive(caseSensitive)
        .useSnapshotId(null)
        .startSnapshotId(newStartSnapshotId)
        .endSnapshotId(newEndSnapshotId)
        .asOfTimestamp(null)
        .splitSize(splitSize)
        .splitOpenFileCost(splitOpenFileCost)
        .nameMapping(nameMapping)
        .streaming(isStreaming)
        .monitorInterval(monitorInterval)
        .build();
  }

  ScanContext copyWithSnapshotId(long newSnapshotId) {
    return ScanContext.builder()
        .caseSensitive(caseSensitive)
        .useSnapshotId(newSnapshotId)
        .startSnapshotId(null)
        .endSnapshotId(null)
        .asOfTimestamp(null)
        .splitSize(splitSize)
        .splitOpenFileCost(splitOpenFileCost)
        .nameMapping(nameMapping)
        .streaming(isStreaming)
        .monitorInterval(monitorInterval)
        .build();
  }

  static Builder builder() {
    return new Builder();
  }

  static class Builder {
    private boolean caseSensitive;
    private Long snapshotId;
    private Long startSnapshotId;
    private Long endSnapshotId;
    private Long asOfTimestamp;
    private Long splitSize;
    private Integer splitLookback;
    private Long splitOpenFileCost;
    private boolean isStreaming;
    private Duration monitorInterval;
    private String nameMapping;
    private Schema projectedSchema;
    private List<Expression> filterExpressions;
    private Long limit;

    private Builder() {
    }

    Builder caseSensitive(boolean newCaseSensitive) {
      this.caseSensitive = newCaseSensitive;
      return this;
    }

    Builder useSnapshotId(Long newSnapshotId) {
      this.snapshotId = newSnapshotId;
      return this;
    }

    Builder startSnapshotId(Long newStartSnapshotId) {
      this.startSnapshotId = newStartSnapshotId;
      return this;
    }

    Builder endSnapshotId(Long newEndsnapshotId) {
      this.endSnapshotId = newEndsnapshotId;
      return this;
    }

    Builder asOfTimestamp(Long newAsOfTimestamp) {
      this.asOfTimestamp = newAsOfTimestamp;
      return this;
    }

    Builder splitSize(Long newSplitSize) {
      this.splitSize = newSplitSize;
      return this;
    }

    Builder splitLookback(Integer newSplitLookback) {
      this.splitLookback = newSplitLookback;
      return this;
    }

    Builder splitOpenFileCost(Long newSplitOpenFileCost) {
      this.splitOpenFileCost = newSplitOpenFileCost;
      return this;
    }

    Builder streaming(boolean streaming) {
      this.isStreaming = streaming;
      return this;
    }

    Builder monitorInterval(Duration newMonitorInterval) {
      this.monitorInterval = newMonitorInterval;
      return this;
    }

    Builder nameMapping(String newNameMapping) {
      this.nameMapping = newNameMapping;
      return this;
    }

    Builder projectedSchema(Schema newProjectedSchema) {
      this.projectedSchema = newProjectedSchema;
      return this;
    }

    Builder filterExpression(List<Expression> newFilterExpressions) {
      this.filterExpressions = newFilterExpressions;
      return this;
    }

    public Builder limit(long newLimit) {
      this.limit = newLimit;
      return this;
    }

    Builder fromProperties(Map<String, String> properties) {
      Configuration config = new Configuration();
      properties.forEach(config::setString);

      return new Builder()
          .caseSensitive(config.get(CASE_SENSITIVE))
          .useSnapshotId(config.get(SNAPSHOT_ID))
          .startSnapshotId(config.get(START_SNAPSHOT_ID))
          .endSnapshotId(config.get(END_SNAPSHOT_ID))
          .asOfTimestamp(config.get(AS_OF_TIMESTAMP))
          .splitSize(config.get(SPLIT_SIZE))
          .splitLookback(config.get(SPLIT_LOOKBACK))
          .splitOpenFileCost(config.get(SPLIT_FILE_OPEN_COST))
          .nameMapping(properties.get(DEFAULT_NAME_MAPPING));
    }

    public ScanContext build() {
      return new ScanContext(caseSensitive, snapshotId, startSnapshotId,
          endSnapshotId, asOfTimestamp, splitSize, splitLookback,
          splitOpenFileCost, isStreaming, monitorInterval, nameMapping, projectedSchema,
          filterExpressions, limit);
    }
  }
}
