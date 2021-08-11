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

package org.apache.iceberg.flink.sink;

import com.codahale.metrics.SlidingWindowReservoir;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.iceberg.io.WriteResult;

class IcebergFilesCommitterMetrics {

  private final AtomicLong lastCheckpointDurationMs;
  private final AtomicLong lastCommitDurationMs;
  private final AtomicLong committedDataFilesCount;
  private final AtomicLong committedDataFilesRecordCount;
  private final AtomicLong committedDataFilesByteCount;
  private final AtomicLong committedDeleteFilesCount;
  private final AtomicLong committedDeleteFilesRecordCount;
  private final AtomicLong committedDeleteFilesByteCount;

  private final Histogram dataFilesSizeHistogram;
  private final Histogram deleteFilesSizeHistogram;

  IcebergFilesCommitterMetrics(MetricGroup metrics, String fullTableName,
                               int slidingWindowReservoirSize) {
    MetricGroup committerMetrics = metrics.addGroup("IcebergFilesCommitter", fullTableName);

    this.lastCheckpointDurationMs = new AtomicLong();
    this.lastCommitDurationMs = new AtomicLong();
    this.committedDataFilesCount = new AtomicLong();
    this.committedDataFilesRecordCount = new AtomicLong();
    this.committedDataFilesByteCount = new AtomicLong();
    this.committedDeleteFilesCount = new AtomicLong();
    this.committedDeleteFilesRecordCount = new AtomicLong();
    this.committedDeleteFilesByteCount = new AtomicLong();

    committerMetrics.gauge("lastCheckpointDurationMs", lastCheckpointDurationMs::get);
    committerMetrics.gauge("lastCommitDurationMs", lastCommitDurationMs::get);
    committerMetrics.gauge("committedDataFilesCount", committedDataFilesCount::get);
    committerMetrics.gauge("committedDataFilesRecordCount", committedDataFilesRecordCount::get);
    committerMetrics.gauge("committedDataFilesByteCount", committedDataFilesByteCount::get);
    committerMetrics.gauge("committedDeleteFilesCount", committedDeleteFilesCount::get);
    committerMetrics.gauge("committedDeleteFilesRecordCount", committedDeleteFilesRecordCount::get);
    committerMetrics.gauge("committedDeleteFilesByteCount", committedDeleteFilesByteCount::get);

    com.codahale.metrics.Histogram dropwizardDataFilesSizeHistogram =
        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(slidingWindowReservoirSize));
    this.dataFilesSizeHistogram = committerMetrics.histogram(
        "dataFilesSizeHistogram", new DropwizardHistogramWrapper(dropwizardDataFilesSizeHistogram));

    com.codahale.metrics.Histogram dropwizardDeleteFilesSizeHistogram =
        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(slidingWindowReservoirSize));
    this.deleteFilesSizeHistogram = committerMetrics.histogram(
        "deleteFilesSizeHistogram", new DropwizardHistogramWrapper(dropwizardDeleteFilesSizeHistogram));
  }

  void checkpointDuration(long checkpointDurationMs) {
    lastCheckpointDurationMs.set(checkpointDurationMs);
  }

  void commitDuration(long commitDurationMs) {
    lastCommitDurationMs.set(commitDurationMs);
  }

  void updateCommitStats(CommitStats stats) {
    committedDataFilesCount.addAndGet(stats.dataFilesCount());
    committedDataFilesRecordCount.addAndGet(stats.dataFilesRecordCount());
    committedDataFilesByteCount.addAndGet(stats.dataFilesByteCount());
    committedDeleteFilesCount.addAndGet(stats.deleteFilesCount());
    committedDeleteFilesRecordCount.addAndGet(stats.deleteFilesRecordCount());
    committedDeleteFilesByteCount.addAndGet(stats.deleteFilesByteCount());
  }

  void updateFileSizeHistogram(WriteResult writeResult) {
    Arrays.stream(writeResult.dataFiles()).forEach(dataFile -> {
      dataFilesSizeHistogram.update(dataFile.fileSizeInBytes());
    });
    Arrays.stream(writeResult.deleteFiles()).forEach(deleteFile -> {
      deleteFilesSizeHistogram.update(deleteFile.fileSizeInBytes());
    });
  }
}
