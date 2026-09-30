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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

public class IcebergSourceReaderMetrics {
  private final Counter assignedSplits;
  private final Counter assignedBytes;
  private final Counter finishedSplits;
  private final Counter finishedBytes;
  private final Counter splitReaderFetchCalls;

  public IcebergSourceReaderMetrics(MetricGroup metrics, String fullTableName) {
    MetricGroup readerMetrics =
        metrics.addGroup("IcebergSourceReader").addGroup("table", fullTableName);

    this.assignedSplits = readerMetrics.counter("assignedSplits");
    this.assignedBytes = readerMetrics.counter("assignedBytes");
    this.finishedSplits = readerMetrics.counter("finishedSplits");
    this.finishedBytes = readerMetrics.counter("finishedBytes");
    this.splitReaderFetchCalls = readerMetrics.counter("splitReaderFetchCalls");
  }

  public void incrementAssignedSplits(long count) {
    assignedSplits.inc(count);
  }

  public void incrementAssignedBytes(long count) {
    assignedBytes.inc(count);
  }

  public void incrementFinishedSplits(long count) {
    finishedSplits.inc(count);
  }

  public void incrementFinishedBytes(long count) {
    finishedBytes.inc(count);
  }

  public void incrementSplitReaderFetchCalls(long count) {
    splitReaderFetchCalls.inc(count);
  }
}
