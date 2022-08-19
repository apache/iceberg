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
package org.apache.iceberg.flink.source.reader;

import java.util.Map;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

class TestingMetricGroup extends UnregisteredMetricsGroup implements SourceReaderMetricGroup {
  private final Map<String, Counter> counters;

  TestingMetricGroup() {
    this.counters = Maps.newHashMap();
  }

  /** Pass along the reference to share the map for child metric groups. */
  private TestingMetricGroup(Map<String, Counter> counters) {
    this.counters = counters;
  }

  Map<String, Counter> counters() {
    return counters;
  }

  @Override
  public Counter counter(String name) {
    Counter counter = new SimpleCounter();
    counters.put(name, counter);
    return counter;
  }

  @Override
  public MetricGroup addGroup(String name) {
    return new TestingMetricGroup(counters);
  }

  @Override
  public MetricGroup addGroup(String key, String value) {
    return new TestingMetricGroup(counters);
  }

  @Override
  public OperatorIOMetricGroup getIOMetricGroup() {
    return new TestingOperatorIOMetricGroup();
  }

  @Override
  public Counter getNumRecordsInErrorsCounter() {
    return new SimpleCounter();
  }

  @Override
  public void setPendingBytesGauge(Gauge<Long> pendingBytesGauge) {}

  @Override
  public void setPendingRecordsGauge(Gauge<Long> pendingRecordsGauge) {}

  private static class TestingOperatorIOMetricGroup extends UnregisteredMetricsGroup
      implements OperatorIOMetricGroup {
    @Override
    public Counter getNumRecordsInCounter() {
      return new SimpleCounter();
    }

    @Override
    public Counter getNumRecordsOutCounter() {
      return new SimpleCounter();
    }

    @Override
    public Counter getNumBytesInCounter() {
      return new SimpleCounter();
    }

    @Override
    public Counter getNumBytesOutCounter() {
      return new SimpleCounter();
    }
  }
}
