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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class TestTracingUtils {

  @Test
  public void testOpenTelemetryAvailableOnTestClasspath() {
    assertThat(TracingUtils.isOpenTelemetryAvailable()).isTrue();
  }

  @Test
  public void testTraceRecordIngestCompletesWithNoOpTracer() {
    AtomicBoolean ingested = new AtomicBoolean(false);
    Schema valueSchema =
        SchemaBuilder.struct().field("op", Schema.STRING_SCHEMA).field("ts_ms", Schema.INT64_SCHEMA);
    Struct value = new Struct(valueSchema).put("op", "c").put("ts_ms", 12345L);
    SinkRecord record = new SinkRecord("topic", 0, null, null, valueSchema, value, 42L);

    TracingUtils.traceRecordIngest(record, "db.table", () -> ingested.set(true));

    assertThat(ingested).isTrue();
  }

  @Test
  public void testTraceRecordIngestPropagatesException() {
    SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 42L);

    assertThatThrownBy(
            () ->
                TracingUtils.traceRecordIngest(
                    record, "db.table", () -> { throw new DataException("ingest failed"); }))
        .isInstanceOf(DataException.class)
        .hasMessage("ingest failed");
  }

  @Test
  public void testTraceCommitCompletesWithNoOpTracer() {
    AtomicBoolean committed = new AtomicBoolean(false);

    TracingUtils.traceCommit("commit-1", false, 2, "connect-group", () -> committed.set(true));

    assertThat(committed).isTrue();
  }

  @Test
  public void testTraceCommitPropagatesException() {
    assertThatThrownBy(
            () ->
                TracingUtils.traceCommit(
                    "commit-1",
                    true,
                    1,
                    "connect-group",
                    () -> { throw new RuntimeException("commit failed"); }))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("commit failed");
  }
}
