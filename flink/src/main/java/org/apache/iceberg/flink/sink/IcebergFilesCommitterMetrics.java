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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.metrics.MetricGroup;

class IcebergFilesCommitterMetrics {
  private final AtomicLong lastCheckpointDurationMs = new AtomicLong();
  private final AtomicLong lastCommitDurationMs = new AtomicLong();
  private final AtomicLong committedDataFilesCount = new AtomicLong();
  private final AtomicLong committedDataFilesRecordCount = new AtomicLong();
  private final AtomicLong committedDataFilesByteCount = new AtomicLong();
  private final AtomicLong committedDeleteFilesCount = new AtomicLong();
  private final AtomicLong committedDeleteFilesRecordCount = new AtomicLong();
  private final AtomicLong committedDeleteFilesByteCount = new AtomicLong();

  IcebergFilesCommitterMetrics(MetricGroup metrics, String fullTableName) {
    MetricGroup committerMetrics = metrics.addGroup("IcebergFilesCommitter", fullTableName);

    committerMetrics.gauge("lastCheckpointDurationMs", lastCheckpointDurationMs::get);
    committerMetrics.gauge("lastCommitDurationMs", lastCommitDurationMs::get);
    committerMetrics.gauge("committedDataFilesCount", committedDataFilesCount::get);
    committerMetrics.gauge("committedDataFilesRecordCount", committedDataFilesRecordCount::get);
    committerMetrics.gauge("committedDataFilesByteCount", committedDataFilesByteCount::get);
    committerMetrics.gauge("committedDeleteFilesCount", committedDeleteFilesCount::get);
    committerMetrics.gauge("committedDeleteFilesRecordCount", committedDeleteFilesRecordCount::get);
    committerMetrics.gauge("committedDeleteFilesByteCount", committedDeleteFilesByteCount::get);
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
}
