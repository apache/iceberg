/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink.sink;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.iceberg.io.WriteResult;

class IcebergStreamWriterMetrics {

  private final Counter processedRecords;
  private final AtomicLong flushedDataFiles = new AtomicLong();
  private final AtomicLong flushedDeleteFiles = new AtomicLong();
  private final AtomicLong flushedReferencedDataFiles = new AtomicLong();
  private final AtomicLong lastFlushDurationMs = new AtomicLong();

  IcebergStreamWriterMetrics(MetricGroup metrics, String fullTableName) {
    MetricGroup writerMetrics = metrics.addGroup("IcebergStreamWriter", fullTableName);
    processedRecords = writerMetrics.counter("processedRecords");
    writerMetrics.gauge("flushedDataFiles", flushedDataFiles::get);
    writerMetrics.gauge("flushedDeleteFiles", flushedDeleteFiles::get);
    writerMetrics.gauge("flushedReferencedDataFiles", flushedReferencedDataFiles::get);
    writerMetrics.gauge("lastFlushDurationMs", lastFlushDurationMs::get);
  }

  void incrementProcessedCount() {
    processedRecords.inc();
  }

  void updateFlushResult(WriteResult result) {
    flushedDataFiles.addAndGet(result.dataFiles().length);
    flushedDeleteFiles.addAndGet(result.deleteFiles().length);
    flushedReferencedDataFiles.addAndGet(result.referencedDataFiles().length);
  }

  void flushDuration(long flushDurationMs) {
    lastFlushDurationMs.set(flushDurationMs);
  }
}
