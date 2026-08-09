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
    long totalNanos = 0L;
    for (long taskMetric : taskMetrics) {
      totalNanos += taskMetric;
    }

    // Values are nanoseconds, which are unreadable raw on the UI. Scale to the largest unit that
    // keeps the number meaningful, matching how Spark renders its own duration metrics.
    if (totalNanos < TimeUnit.MICROSECONDS.toNanos(1)) {
      return totalNanos + " ns";
    } else if (totalNanos < TimeUnit.MILLISECONDS.toNanos(1)) {
      return TimeUnit.NANOSECONDS.toMicros(totalNanos) + " us";
    } else if (totalNanos < TimeUnit.SECONDS.toNanos(1)) {
      return TimeUnit.NANOSECONDS.toMillis(totalNanos) + " ms";
    } else {
      return String.format("%.1f s", totalNanos / (double) TimeUnit.SECONDS.toNanos(1));
    }
  }
}
