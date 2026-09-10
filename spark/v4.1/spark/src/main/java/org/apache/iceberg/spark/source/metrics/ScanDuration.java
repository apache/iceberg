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
package org.apache.iceberg.spark.source.metrics;

import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.connector.metric.CustomSumMetric;

public class ScanDuration extends CustomSumMetric {

  public static final String NAME = "scanDuration";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String description() {
    return "total scan duration";
  }

  @Override
  public String aggregateTaskMetrics(long[] taskMetrics) {
    if (taskMetrics.length == 0) {
      return format(0L);
    }

    long totalNanos = 0L;
    for (long taskMetric : taskMetrics) {
      totalNanos += taskMetric;
    }

    // sort a copy: Spark reuses the array it hands us
    long[] sorted = Arrays.copyOf(taskMetrics, taskMetrics.length);
    Arrays.sort(sorted);

    // mirrors MetricUtils.stringValue for Spark's own timing metrics, so a straggler task is
    // visible instead of being hidden by the total
    return String.format(
        Locale.ROOT,
        "total (min, med, max)\n%s (%s, %s, %s)",
        format(totalNanos),
        format(sorted[0]),
        format(sorted[sorted.length / 2]),
        format(sorted[sorted.length - 1]));
  }

  private String format(long nanos) {
    // raw nanos are unreadable on the UI, scale to the largest meaningful unit
    if (nanos < TimeUnit.MICROSECONDS.toNanos(1)) {
      return nanos + " ns";
    } else if (nanos < TimeUnit.MILLISECONDS.toNanos(1)) {
      return TimeUnit.NANOSECONDS.toMicros(nanos) + " us";
    } else if (nanos < TimeUnit.SECONDS.toNanos(1)) {
      return TimeUnit.NANOSECONDS.toMillis(nanos) + " ms";
    } else {
      return String.format(Locale.ROOT, "%.1f s", nanos / (double) TimeUnit.SECONDS.toNanos(1));
    }
  }
}
