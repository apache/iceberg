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

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;

/**
 * Task metrics reported by the readers that ran earlier in the same Spark task.
 *
 * <p>When a task reads several input partitions, Spark opens one reader per partition and passes
 * the previous reader's metrics to the next through {@code initMetricsValues}. Spark keeps only the
 * last reader's values, so each reader must add the inherited values to its own.
 */
class InheritedTaskMetrics {
  private final Map<String, Long> values = Maps.newHashMap();

  void inherit(CustomTaskMetric[] metrics) {
    values.clear();
    for (CustomTaskMetric metric : metrics) {
      values.put(metric.name(), metric.value());
    }
  }

  CustomTaskMetric[] addTo(CustomTaskMetric... metrics) {
    CustomTaskMetric[] result = new CustomTaskMetric[metrics.length];
    for (int i = 0; i < metrics.length; i += 1) {
      CustomTaskMetric metric = metrics[i];
      Long inherited = values.get(metric.name());
      result[i] =
          inherited != null
              ? new SummedTaskMetric(metric.name(), inherited + metric.value())
              : metric;
    }

    return result;
  }

  private static class SummedTaskMetric implements CustomTaskMetric {
    private final String name;
    private final long value;

    private SummedTaskMetric(String name, long value) {
      this.name = name;
      this.value = value;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public long value() {
      return value;
    }
  }
}
