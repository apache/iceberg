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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.spark.source.metrics.NanoDurationMetric.MaxTaskWriteDuration;
import org.apache.iceberg.spark.source.metrics.NanoDurationMetric.MedianTaskWriteDuration;
import org.apache.iceberg.spark.source.metrics.NanoDurationMetric.MinTaskWriteDuration;
import org.apache.iceberg.spark.source.metrics.NanoDurationMetric.WriteDuration;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.junit.jupiter.api.Test;

class TestWriteTimer {
  private static final WriterCommitMessage TASK_MESSAGE = new WriterCommitMessage() {};

  @Test
  void driverReportsTaskWriteDurationSpread() throws Exception {
    WriteTimer.Totals totals = new WriteTimer.Totals();
    for (long writeNanos : new long[] {40L, 10L, 900L, 20L, 30L}) {
      WriteTimer timer = new WriteTimer(0, 0L);
      timer.recordStorageWrite(writeNanos, 0L);
      WriterCommitMessage message = timer.wrap(new CommitOnlyWriter()).commit();
      assertThat(totals.unwrap(message)).isSameAs(TASK_MESSAGE);
    }

    assertThat(values(totals.driverMetrics(null)))
        .containsEntry(WriteDuration.NAME, 1_000L)
        .containsEntry(MinTaskWriteDuration.NAME, 10L)
        .containsEntry(MedianTaskWriteDuration.NAME, 30L)
        .containsEntry(MaxTaskWriteDuration.NAME, 900L);
  }

  private static Map<String, Long> values(CustomTaskMetric[] metrics) {
    return Arrays.stream(metrics)
        .collect(Collectors.toMap(CustomTaskMetric::name, CustomTaskMetric::value));
  }

  private static class CommitOnlyWriter implements DataWriter<Object> {
    @Override
    public void write(Object record) {}

    @Override
    public WriterCommitMessage commit() {
      return TASK_MESSAGE;
    }

    @Override
    public void abort() {}

    @Override
    public void close() {}
  }
}
