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

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accumulates the time a write task spends in storage calls and closing its writers.
 *
 * <p>Storage time is reported by the streams of a {@link TimedFileIO}, so it covers every file
 * format without reading the clock per row.
 */
class WriteTimer {
  private static final Logger LOG = LoggerFactory.getLogger(WriteTimer.class);

  private final int partitionId;
  private final long taskId;
  private long storageWriteNanos = 0L;
  private long storageWriteBytes = 0L;
  private long closeNanos = 0L;

  WriteTimer(int partitionId, long taskId) {
    this.partitionId = partitionId;
    this.taskId = taskId;
  }

  void recordStorageWrite(long nanos, long bytes) {
    this.storageWriteNanos += nanos;
    this.storageWriteBytes += bytes;
  }

  /** Runs the close, adding its duration to the close time. */
  <E extends Exception> void timeClose(ThrowingRunnable<E> closeOp) throws E {
    long start = System.nanoTime();
    try {
      closeOp.run();
    } finally {
      this.closeNanos += System.nanoTime() - start;
    }
  }

  /** Logs this task's totals and returns them for the commit message. */
  WriteDurations finish() {
    LOG.info(
        "Task {} (partition {}) spent {} ms writing {} bytes to storage and {} ms closing writers",
        taskId,
        partitionId,
        TimeUnit.NANOSECONDS.toMillis(storageWriteNanos),
        storageWriteBytes,
        TimeUnit.NANOSECONDS.toMillis(closeNanos));
    return WriteDurations.of(storageWriteNanos, closeNanos);
  }

  long storageWriteNanos() {
    return storageWriteNanos;
  }

  long storageWriteBytes() {
    return storageWriteBytes;
  }

  long closeNanos() {
    return closeNanos;
  }

  @FunctionalInterface
  interface ThrowingRunnable<E extends Exception> {
    void run() throws E;
  }
}
