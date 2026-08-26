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

import org.apache.iceberg.spark.WriteDurations;

/**
 * Accumulates the wall time a write task spends producing files.
 *
 * <p>Times are split into the row-level write calls and the final close, which flushes buffered
 * rows and finishes the files. Both are timed per call rather than per row: a nanoTime pair per row
 * would cost more than the work it measures.
 */
class WriteTimer {
  private long writeNanos = 0L;
  private long closeNanos = 0L;

  /** Runs the row-level write, adding its duration to the write time. */
  <E extends Exception> void timeWrite(ThrowingRunnable<E> writeOp) throws E {
    long start = System.nanoTime();
    try {
      writeOp.run();
    } finally {
      this.writeNanos += System.nanoTime() - start;
    }
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

  WriteDurations writeDurations() {
    return WriteDurations.of(writeNanos, closeNanos);
  }

  long writeNanos() {
    return writeNanos;
  }

  long closeNanos() {
    return closeNanos;
  }

  @FunctionalInterface
  interface ThrowingRunnable<E extends Exception> {
    void run() throws E;
  }
}
