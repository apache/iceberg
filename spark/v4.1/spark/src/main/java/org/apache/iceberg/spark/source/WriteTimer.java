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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.metrics.InMemoryMetricsReporter;
import org.apache.iceberg.relocated.com.google.common.collect.ObjectArrays;
import org.apache.iceberg.spark.SparkWriteUtil;
import org.apache.iceberg.spark.source.metrics.NanoDurationMetric.MaxTaskWriteDuration;
import org.apache.iceberg.spark.source.metrics.NanoDurationMetric.WriteCloseDuration;
import org.apache.iceberg.spark.source.metrics.NanoDurationMetric.WriteDuration;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DeltaWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accumulates the time a write task spends in storage calls, reported by the streams of a {@link
 * TimedFileIO}.
 *
 * <p>Spark collects task metrics before it calls {@code DataWriter.commit()}, where files are
 * closed and most storage time is spent. So a wrapped writer sends the task's times to the driver
 * in its commit message, and the driver reports them.
 */
class WriteTimer {
  private static final Logger LOG = LoggerFactory.getLogger(WriteTimer.class);

  private final int partitionId;
  private final long taskId;
  private long storageWriteNanos = 0L;
  private long storageWriteBytes = 0L;

  WriteTimer(int partitionId, long taskId) {
    this.partitionId = partitionId;
    this.taskId = taskId;
  }

  void recordStorageWrite(long nanos, long bytes) {
    this.storageWriteNanos += nanos;
    this.storageWriteBytes += bytes;
  }

  long storageWriteNanos() {
    return storageWriteNanos;
  }

  long storageWriteBytes() {
    return storageWriteBytes;
  }

  <T> DataWriter<T> wrap(DataWriter<T> writer) {
    return new TimedDataWriter<>(writer);
  }

  <T> DeltaWriter<T> wrap(DeltaWriter<T> writer) {
    return new TimedDeltaWriter<>(writer);
  }

  private class TimedDataWriter<T> implements DataWriter<T> {
    private final DataWriter<T> delegate;

    private TimedDataWriter(DataWriter<T> delegate) {
      this.delegate = delegate;
    }

    @Override
    public void write(T record) throws IOException {
      delegate.write(record);
    }

    @Override
    public void write(T metadata, T record) throws IOException {
      delegate.write(metadata, record);
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      // the wrapped writers close their files in commit()
      long start = System.nanoTime();
      WriterCommitMessage message = delegate.commit();
      long closeNanos = System.nanoTime() - start;
      LOG.info(
          "Task {} (partition {}) spent {} ms writing {} bytes to storage and {} ms closing writers",
          taskId,
          partitionId,
          TimeUnit.NANOSECONDS.toMillis(storageWriteNanos),
          storageWriteBytes,
          TimeUnit.NANOSECONDS.toMillis(closeNanos));
      return new TimedCommit(message, storageWriteNanos, closeNanos);
    }

    @Override
    public void abort() throws IOException {
      delegate.abort();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    @Override
    public CustomTaskMetric[] currentMetricsValues() {
      return delegate.currentMetricsValues();
    }
  }

  private class TimedDeltaWriter<T> extends TimedDataWriter<T> implements DeltaWriter<T> {
    private final DeltaWriter<T> delegate;

    private TimedDeltaWriter(DeltaWriter<T> delegate) {
      super(delegate);
      this.delegate = delegate;
    }

    @Override
    public void delete(T metadata, T id) throws IOException {
      delegate.delete(metadata, id);
    }

    @Override
    public void update(T metadata, T id, T row) throws IOException {
      delegate.update(metadata, id, row);
    }

    @Override
    public void reinsert(T metadata, T row) throws IOException {
      delegate.reinsert(metadata, row);
    }

    @Override
    public void insert(T row) throws IOException {
      delegate.insert(row);
    }
  }

  /** Returns the commit message of a wrapped writer. */
  static WriterCommitMessage unwrap(WriterCommitMessage message) {
    return ((TimedCommit) message).message;
  }

  /** A task's commit message along with its write times. */
  private static class TimedCommit implements WriterCommitMessage {
    private final WriterCommitMessage message;
    private final long writeNanos;
    private final long closeNanos;

    TimedCommit(WriterCommitMessage message, long writeNanos, long closeNanos) {
      this.message = message;
      this.writeNanos = writeNanos;
      this.closeNanos = closeNanos;
    }
  }

  /** Write times summed over the committed tasks of a write, reported by the driver. */
  static class Totals {
    private long writeNanos = 0L;
    private long maxTaskWriteNanos = 0L;
    private long closeNanos = 0L;

    /** Adds the times of a task and returns the commit message of its wrapped writer. */
    WriterCommitMessage unwrap(WriterCommitMessage message) {
      TimedCommit commit = (TimedCommit) message;
      this.writeNanos += commit.writeNanos;
      this.maxTaskWriteNanos = Math.max(maxTaskWriteNanos, commit.writeNanos);
      this.closeNanos += commit.closeNanos;
      return WriteTimer.unwrap(message);
    }

    CustomTaskMetric[] driverMetrics(InMemoryMetricsReporter metricsReporter) {
      CustomTaskMetric[] durationMetrics = {
        taskMetric(WriteDuration.NAME, writeNanos),
        taskMetric(MaxTaskWriteDuration.NAME, maxTaskWriteNanos),
        taskMetric(WriteCloseDuration.NAME, closeNanos)
      };

      return ObjectArrays.concat(
          durationMetrics,
          SparkWriteUtil.customTaskMetrics(metricsReporter),
          CustomTaskMetric.class);
    }

    private static CustomTaskMetric taskMetric(String name, long value) {
      return new CustomTaskMetric() {
        @Override
        public String name() {
          return name;
        }

        @Override
        public long value() {
          return value;
        }
      };
    }
  }
}
