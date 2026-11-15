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
package org.apache.iceberg.spark.source;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.metrics.InMemoryMetricsReporter;
import org.apache.iceberg.relocated.com.google.common.collect.ObjectArrays;
import org.apache.iceberg.spark.SparkWriteUtil;
import org.apache.iceberg.spark.source.metrics.NanoDurationMetric.MaxTaskWriteDuration;
import org.apache.iceberg.spark.source.metrics.NanoDurationMetric.MedianTaskWriteDuration;
import org.apache.iceberg.spark.source.metrics.NanoDurationMetric.MinTaskWriteDuration;
import org.apache.iceberg.spark.source.metrics.NanoDurationMetric.WriteCloseDuration;
import org.apache.iceberg.spark.source.metrics.NanoDurationMetric.WriteDuration;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;

/**
 * Write times of one task.
 *
 * <p>Spark collects task metrics before it calls {@code DataWriter.commit()}, where files are
 * closed and most storage time is spent. So tasks send their times to the driver in their commit
 * messages, and the driver reports them.
 */
class WriteDurations implements Serializable {
  private final long writeNanos;
  private final long closeNanos;

  private WriteDurations(long writeNanos, long closeNanos) {
    this.writeNanos = writeNanos;
    this.closeNanos = closeNanos;
  }

  static WriteDurations of(long writeNanos, long closeNanos) {
    return new WriteDurations(writeNanos, closeNanos);
  }

  static CustomTaskMetric[] driverMetrics(
      InMemoryMetricsReporter metricsReporter, List<WriteDurations> taskDurations) {
    long[] sortedWriteNanos = new long[taskDurations.size()];
    long writeNanos = 0L;
    long closeNanos = 0L;
    for (int i = 0; i < sortedWriteNanos.length; i += 1) {
      WriteDurations durations = taskDurations.get(i);
      sortedWriteNanos[i] = durations.writeNanos;
      writeNanos += durations.writeNanos;
      closeNanos += durations.closeNanos;
    }

    Arrays.sort(sortedWriteNanos);
    int numTasks = sortedWriteNanos.length;
    CustomTaskMetric[] durationMetrics = {
      taskMetric(WriteDuration.NAME, writeNanos),
      taskMetric(MinTaskWriteDuration.NAME, numTasks > 0 ? sortedWriteNanos[0] : 0L),
      taskMetric(MedianTaskWriteDuration.NAME, numTasks > 0 ? sortedWriteNanos[numTasks / 2] : 0L),
      taskMetric(MaxTaskWriteDuration.NAME, numTasks > 0 ? sortedWriteNanos[numTasks - 1] : 0L),
      taskMetric(WriteCloseDuration.NAME, closeNanos)
    };

    return ObjectArrays.concat(
        durationMetrics, SparkWriteUtil.customTaskMetrics(metricsReporter), CustomTaskMetric.class);
  }

  private static CustomTaskMetric taskMetric(String name, long value) {
    return new CustomTaskMetric() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public long value() {
        return value;
      }
    };
  }
}
