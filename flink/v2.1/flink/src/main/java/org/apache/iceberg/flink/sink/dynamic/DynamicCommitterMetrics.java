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
import org.apache.flink.metrics.MetricGroup;
import org.apache.iceberg.flink.sink.CommitSummary;
import org.apache.iceberg.flink.sink.IcebergFilesCommitterMetrics;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

class DynamicCommitterMetrics {

  private final Map<String, IcebergFilesCommitterMetrics> metrics;
  private final MetricGroup mainMetricsGroup;

  DynamicCommitterMetrics(MetricGroup mainMetricsGroup) {
    this.mainMetricsGroup = mainMetricsGroup;
    this.metrics = Maps.newHashMap();
  }

  public void commitDuration(String fullTableName, long commitDurationMs) {
    committerMetrics(fullTableName).commitDuration(commitDurationMs);
  }

  /** This is called upon a successful commit. */
  public void updateCommitSummary(String fullTableName, CommitSummary stats) {
    committerMetrics(fullTableName).updateCommitSummary(stats);
  }

  private IcebergFilesCommitterMetrics committerMetrics(String fullTableName) {
    return metrics.computeIfAbsent(
        fullTableName, tableName -> new IcebergFilesCommitterMetrics(mainMetricsGroup, tableName));
  }
}
