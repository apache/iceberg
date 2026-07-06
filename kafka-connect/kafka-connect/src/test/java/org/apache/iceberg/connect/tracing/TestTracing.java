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
package org.apache.iceberg.connect.tracing;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class TestTracing {

  private static IcebergSinkConfig config(boolean tracingEnabled) {
    return new IcebergSinkConfig(
        ImmutableMap.of(
            "iceberg.catalog.type",
            "rest",
            "topics",
            "source-topic",
            "iceberg.tables",
            "db.landing",
            IcebergSinkConfig.TRACING_ENABLED_PROP,
            Boolean.toString(tracingEnabled)));
  }

  @Test
  public void testIsActiveWhenTracingDisabled() {
    assertThat(Tracing.isActive(config(false))).isFalse();
  }

  @Test
  public void testIsActiveWhenTracingEnabled() {
    assertThat(Tracing.isActive(config(true))).isTrue();
  }

  @Test
  public void testTraceRecordIngestSkipsWhenDisabled() {
    AtomicBoolean ingested = new AtomicBoolean(false);
    SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 1L);

    Tracing.traceRecordIngest(config(false), record, "db.table", () -> ingested.set(true));

    assertThat(ingested).isTrue();
  }

  @Test
  public void testTraceCommitSkipsWhenDisabled() {
    AtomicBoolean committed = new AtomicBoolean(false);

    Tracing.traceCommit(config(false), "commit-1", false, 1, "group", () -> committed.set(true));

    assertThat(committed).isTrue();
  }
}
