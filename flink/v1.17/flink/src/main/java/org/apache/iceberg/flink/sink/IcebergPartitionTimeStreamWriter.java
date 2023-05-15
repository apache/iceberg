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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.flink.data.PartitionedWriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Each partition has a writer, which will be closed when the watermark exceeds the partition time,
 * and the result will be sent to the committer. The unclosed writers will remain active until the
 * watermark exceeds their partition time.
 */
class IcebergPartitionTimeStreamWriter<T> extends IcebergStreamWriter<T> {
  private transient long currentWatermark;

  IcebergPartitionTimeStreamWriter(String fullTableName, TaskWriterFactory<T> taskWriterFactory) {
    super(fullTableName, taskWriterFactory);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    flush();
    if (writer() == null) {
      writer(taskWriterFactory().create());
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    // todo store the unclosed writers to the state
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    super.processWatermark(mark);
    this.currentWatermark = mark.getTimestamp();
  }

  @Override
  public void endInput() throws IOException {
    // For bounded stream, it may don't enable the checkpoint mechanism so we'd better to emit the
    // remaining completed files to downstream before closing the writer so that we won't miss any
    // of them.
    // Note that if the task is not closed after calling endInput, checkpoint may be triggered again
    // causing files to be sent repeatedly, the writer is marked as null after the last file is sent
    // to guard against duplicated writes.

    // Set the watermark to the maximum value to ensure that all bounded data are flushed.
    this.currentWatermark = Watermark.MAX_WATERMARK.getTimestamp();
    flush();
  }

  @Override
  protected void flush() throws IOException {
    if (writer() == null) {
      return;
    }

    long startNano = System.nanoTime();
    List<PartitionedWriteResult> results = Lists.newArrayList();
    PartitionedWriteResult newResult =
        ((PartitionCommitWriter) writer()).complete(currentWatermark);
    while (newResult != null) {
      results.add(newResult);
      newResult = ((PartitionCommitWriter) writer()).complete(currentWatermark);
    }

    for (PartitionedWriteResult result : results) {
      writerMetrics().updateFlushResult(result);
      output.collect(new StreamRecord<>(result));
      writerMetrics().flushDuration(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano));
    }
  }
}
