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
import java.util.Map;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;

public class ScanOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final ConfigOption<Long> SNAPSHOT_ID =
      ConfigOptions.key("snapshot-id").longType().defaultValue(null);

  public static final ConfigOption<Boolean> CASE_SENSITIVE =
      ConfigOptions.key("case-sensitive").booleanType().defaultValue(false);

  public static final ConfigOption<Long> AS_OF_TIMESTAMP =
      ConfigOptions.key("as-of-timestamp").longType().defaultValue(null);

  public static final ConfigOption<Long> START_SNAPSHOT_ID =
      ConfigOptions.key("start-snapshot-id").longType().defaultValue(null);

  public static final ConfigOption<Long> END_SNAPSHOT_ID =
      ConfigOptions.key("end-snapshot-id").longType().defaultValue(null);

  public static final ConfigOption<Long> SPLIT_SIZE =
      ConfigOptions.key("split-size").longType().defaultValue(null);

  public static final ConfigOption<Integer> SPLIT_LOOKBACK =
      ConfigOptions.key("split-lookback").intType().defaultValue(null);

  public static final ConfigOption<Long> SPLIT_FILE_OPEN_COST =
      ConfigOptions.key("split-file-open-cost").longType().defaultValue(null);

  private final boolean caseSensitive;
  private final Long snapshotId;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private final Long asOfTimestamp;
  private final Long splitSize;
  private final Integer splitLookback;
  private final Long splitOpenFileCost;
  private final String nameMapping;

  public ScanOptions(boolean caseSensitive, Long snapshotId, Long startSnapshotId, Long endSnapshotId,
                     Long asOfTimestamp, Long splitSize, Integer splitLookback, Long splitOpenFileCost,
                     String nameMapping) {
    this.caseSensitive = caseSensitive;
    this.snapshotId = snapshotId;
    this.startSnapshotId = startSnapshotId;
    this.endSnapshotId = endSnapshotId;
    this.asOfTimestamp = asOfTimestamp;
    this.splitSize = splitSize;
    this.splitLookback = splitLookback;
    this.splitOpenFileCost = splitOpenFileCost;
    this.nameMapping = nameMapping;
  }

  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  public Long getSnapshotId() {
    return snapshotId;
  }

  public Long getStartSnapshotId() {
    return startSnapshotId;
  }

  public Long getEndSnapshotId() {
    return endSnapshotId;
  }

  public Long getAsOfTimestamp() {
    return asOfTimestamp;
  }

  public Long getSplitSize() {
    return splitSize;
  }

  public Integer getSplitLookback() {
    return splitLookback;
  }

  public Long getSplitOpenFileCost() {
    return splitOpenFileCost;
  }

  public String getNameMapping() {
    return nameMapping;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static ScanOptions of(Map<String, String> options) {
    return builder().options(options).build();
  }

  public static final class Builder {
    private boolean caseSensitive = CASE_SENSITIVE.defaultValue();
    private Long snapshotId = SNAPSHOT_ID.defaultValue();
    private Long startSnapshotId = START_SNAPSHOT_ID.defaultValue();
    private Long endSnapshotId = END_SNAPSHOT_ID.defaultValue();
    private Long asOfTimestamp = AS_OF_TIMESTAMP.defaultValue();
    private Long splitSize = SPLIT_SIZE.defaultValue();
    private Integer splitLookback = SPLIT_LOOKBACK.defaultValue();
    private Long splitOpenFileCost = SPLIT_FILE_OPEN_COST.defaultValue();
    private String nameMapping;

    private Builder() {
    }

    public Builder options(Map<String, String> options) {
      Configuration config = new Configuration();
      options.forEach(config::setString);
      this.caseSensitive = config.get(CASE_SENSITIVE);
      this.snapshotId = config.get(SNAPSHOT_ID);
      this.asOfTimestamp = config.get(AS_OF_TIMESTAMP);
      this.startSnapshotId = config.get(START_SNAPSHOT_ID);
      this.endSnapshotId = config.get(END_SNAPSHOT_ID);
      this.splitSize = config.get(SPLIT_SIZE);
      this.splitLookback = config.get(SPLIT_LOOKBACK);
      this.splitOpenFileCost = config.get(SPLIT_FILE_OPEN_COST);
      this.nameMapping = options.get(DEFAULT_NAME_MAPPING);
      return this;
    }

    public Builder caseSensitive(boolean newCaseSensitive) {
      this.caseSensitive = newCaseSensitive;
      return this;
    }

    public Builder snapshotId(Long newSnapshotId) {
      this.snapshotId = newSnapshotId;
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

    public Builder nameMapping(String newNameMapping) {
      this.nameMapping = newNameMapping;
      return this;
    }

    public ScanOptions build() {
      if (snapshotId != null && asOfTimestamp != null) {
        throw new IllegalArgumentException(
            "Cannot scan using both snapshot-id and as-of-timestamp to select the table snapshot");
      }

      if (snapshotId != null || asOfTimestamp != null) {
        if (startSnapshotId != null || endSnapshotId != null) {
          throw new IllegalArgumentException(
              "Cannot specify start-snapshot-id and end-snapshot-id to do incremental scan when either snapshot-id" +
                  " or as-of-timestamp is specified");
        }
      } else {
        if (startSnapshotId == null && endSnapshotId != null) {
          throw new IllegalArgumentException("Cannot only specify option end-snapshot-id to do incremental scan");
        }
      }
      return new ScanOptions(caseSensitive, snapshotId, startSnapshotId, endSnapshotId, asOfTimestamp, splitSize,
                             splitLookback, splitOpenFileCost, nameMapping);
    }
  }
}
