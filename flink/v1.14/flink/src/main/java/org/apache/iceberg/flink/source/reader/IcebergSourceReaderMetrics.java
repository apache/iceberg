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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

@Internal
public class IcebergSourceReaderMetrics {
  private final AtomicLong assignedSplits;
  private final AtomicLong assignedBytes;
  private final AtomicLong finishedSplits;
  private final AtomicLong finishedBytes;
  private final Counter splitReaderFetchCalls;

  public IcebergSourceReaderMetrics(MetricGroup metricGroup) {
    MetricGroup readerMetricGroup = metricGroup.addGroup("IcebergSourceReader");

    this.assignedSplits = new AtomicLong();
    this.assignedBytes = new AtomicLong();
    this.finishedSplits = new AtomicLong();
    this.finishedBytes = new AtomicLong();
    readerMetricGroup.gauge("assignedSplits", assignedSplits::get);
    readerMetricGroup.gauge("assignedBytes", assignedBytes::get);
    readerMetricGroup.gauge("finishedSplits", finishedSplits::get);
    readerMetricGroup.gauge("finishedBytes", finishedBytes::get);
    this.splitReaderFetchCalls = readerMetricGroup.counter("splitReaderFetchCalls");
  }

  public void incrementAssignedSplits(long delta) {
    assignedSplits.addAndGet(delta);
  }

  public void incrementAssignedBytes(long delta) {
    assignedBytes.addAndGet(delta);
  }

  public void incrementFinishedSplits(long delta) {
    finishedSplits.addAndGet(delta);
  }

  public void incrementFinishedBytes(long delta) {
    finishedBytes.addAndGet(delta);
  }

  public void incrementSplitReaderFetchCalls() {
    splitReaderFetchCalls.inc();
  }
}
