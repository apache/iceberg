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

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.connector.metric.CustomSumMetric;

/**
 * Base class for metrics that report a nanosecond duration, and the write duration metrics.
 *
 * <p>Spark recreates a metric from its class name to aggregate it, so each metric is its own class.
 */
public abstract class NanoDurationMetric extends CustomSumMetric {
  private final String name;
  private final String description;

  NanoDurationMetric(String name, String description) {
    this.name = name;
    this.description = description;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String description() {
    return description;
  }

  @Override
  public String aggregateTaskMetrics(long[] taskMetrics) {
    long totalNanos = 0L;
    for (long taskMetric : taskMetrics) {
      totalNanos += taskMetric;
    }

    return format(totalNanos);
  }

  private static String format(long nanos) {
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

  public static class WriteDuration extends NanoDurationMetric {
    public static final String NAME = "writeDuration";

    public WriteDuration() {
      super(NAME, "total storage write duration");
    }
  }

  public static class MinTaskWriteDuration extends NanoDurationMetric {
    public static final String NAME = "minTaskWriteDuration";

    public MinTaskWriteDuration() {
      super(NAME, "storage write duration (min task)");
    }
  }

  public static class MedianTaskWriteDuration extends NanoDurationMetric {
    public static final String NAME = "medianTaskWriteDuration";

    public MedianTaskWriteDuration() {
      super(NAME, "storage write duration (median task)");
    }
  }

  public static class MaxTaskWriteDuration extends NanoDurationMetric {
    public static final String NAME = "maxTaskWriteDuration";

    public MaxTaskWriteDuration() {
      super(NAME, "storage write duration (max task)");
    }
  }

  public static class WriteCloseDuration extends NanoDurationMetric {
    public static final String NAME = "writeCloseDuration";

    public WriteCloseDuration() {
      super(NAME, "total write close duration");
    }
  }
}
