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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

class IcebergFilesCommitterMetrics {
  private final AtomicLong lastCheckpointDurationMs = new AtomicLong();
  private final AtomicLong lastCommitDurationMs = new AtomicLong();
  private final Counter committedDataFilesCount;
  private final Counter committedDataFilesRecordCount;
  private final Counter committedDataFilesByteCount;
  private final Counter committedDeleteFilesCount;
  private final Counter committedDeleteFilesRecordCount;
  private final Counter committedDeleteFilesByteCount;

  IcebergFilesCommitterMetrics(MetricGroup metrics, String fullTableName) {
    MetricGroup committerMetrics =
        metrics.addGroup("IcebergFilesCommitter").addGroup("table", fullTableName);
    committerMetrics.gauge("lastCheckpointDurationMs", lastCheckpointDurationMs::get);
    committerMetrics.gauge("lastCommitDurationMs", lastCommitDurationMs::get);
    this.committedDataFilesCount = committerMetrics.counter("committedDataFilesCount");
    this.committedDataFilesRecordCount = committerMetrics.counter("committedDataFilesRecordCount");
    this.committedDataFilesByteCount = committerMetrics.counter("committedDataFilesByteCount");
    this.committedDeleteFilesCount = committerMetrics.counter("committedDeleteFilesCount");
    this.committedDeleteFilesRecordCount =
        committerMetrics.counter("committedDeleteFilesRecordCount");
    this.committedDeleteFilesByteCount = committerMetrics.counter("committedDeleteFilesByteCount");
  }

  void checkpointDuration(long checkpointDurationMs) {
    lastCheckpointDurationMs.set(checkpointDurationMs);
  }

  void commitDuration(long commitDurationMs) {
    lastCommitDurationMs.set(commitDurationMs);
  }

  void updateCommitSummary(CommitSummary stats) {
    committedDataFilesCount.inc(stats.dataFilesCount());
    committedDataFilesRecordCount.inc(stats.dataFilesRecordCount());
    committedDataFilesByteCount.inc(stats.dataFilesByteCount());
    committedDeleteFilesCount.inc(stats.deleteFilesCount());
    committedDeleteFilesRecordCount.inc(stats.deleteFilesRecordCount());
    committedDeleteFilesByteCount.inc(stats.deleteFilesByteCount());
  }
}
