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
  private final AtomicLong numRecordsOut;
  private final AtomicLong assignedSplits;
  private final AtomicLong finishedSplits;
  private final Counter splitReaderFetches;

  public IcebergSourceReaderMetrics(MetricGroup metricGroup) {
    final MetricGroup readerMetricGroup = metricGroup.addGroup("IcebergSourceReader");

    this.numRecordsOut = new AtomicLong();
    this.assignedSplits = new AtomicLong();
    this.finishedSplits = new AtomicLong();
    readerMetricGroup.gauge("numRecordsOut", numRecordsOut::get);
    readerMetricGroup.gauge("assignedSplits", assignedSplits::get);
    readerMetricGroup.gauge("finishedSplits", finishedSplits::get);
    this.splitReaderFetches = readerMetricGroup.counter("splitReaderFetches");
  }

  public void incrementNumRecordsOut(long delta) {
    numRecordsOut.addAndGet(delta);
  }

  public void incrementAssignedSplits(long delta) {
    assignedSplits.addAndGet(delta);
  }

  public void incrementFinishedSplits(long delta) {
    finishedSplits.addAndGet(delta);
  }

  public void recordSplitReaderFetches() {
    splitReaderFetches.inc();
  }
}
