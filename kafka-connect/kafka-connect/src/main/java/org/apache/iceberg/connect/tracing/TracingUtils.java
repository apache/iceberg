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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

public final class TracingUtils {

  private static final String TRACING_COMPONENT = "iceberg-kafka-connect";
  private static final String SINK_INGEST = "iceberg-sink-ingest";
  private static final String SINK_COMMIT = "iceberg-sink-commit";
  private static final String STAGE_BUFFER = "buffer";
  private static final String STAGE_DURABILITY = "durability";

  private static final boolean OPEN_TELEMETRY_AVAILABLE = resolveOpenTelemetryApiAvailable();
  private static final OpenTelemetry OPEN_TELEMETRY =
      OPEN_TELEMETRY_AVAILABLE ? GlobalOpenTelemetry.get() : null;
  private static final Tracer TRACER =
      OPEN_TELEMETRY_AVAILABLE ? OPEN_TELEMETRY.getTracer(TRACING_COMPONENT) : null;

  private TracingUtils() {}

  public static boolean isOpenTelemetryAvailable() {
    return OPEN_TELEMETRY_AVAILABLE;
  }

  /**
   * Creates a child span for ingesting a record into connector file buffers. Extracts the upstream
   * trace context from Kafka Connect record headers (e.g. {@code traceparent} injected by
   * Debezium).
   *
   * <p>This span covers buffering into Parquet/ORC files via {@code RecordWriter.write()}, not the
   * later Iceberg catalog commit.
   */
  public static void traceRecordIngest(SinkRecord record, String tableName, Runnable ingestAction) {
    TextMapPropagator propagator = OPEN_TELEMETRY.getPropagators().getTextMapPropagator();
    Context parent =
        propagator.extract(Context.root(), record.headers(), KafkaConnectHeadersGetter.INSTANCE);

    Span span =
        TRACER
            .spanBuilder(SINK_INGEST)
            .setParent(parent)
            .setSpanKind(SpanKind.INTERNAL)
            .startSpan();
    try (Scope ignored = span.makeCurrent()) {
      span.setAttribute("iceberg.stage", STAGE_BUFFER);
      span.setAttribute("db.table", tableName);
      span.setAttribute("kafka.topic", record.topic());
      span.setAttribute("kafka.partition", record.kafkaPartition());
      span.setAttribute("kafka.offset", record.kafkaOffset());
      setDebeziumAttributes(span, record);
      runAction(span, ingestAction);
    } finally {
      span.end();
    }
  }

  /** Creates a root span for an Iceberg catalog commit cycle (durability). */
  public static void traceCommit(
      String commitId,
      boolean partialCommit,
      int tableCount,
      String connectGroupId,
      Runnable commitAction) {
    Span span = TRACER.spanBuilder(SINK_COMMIT).setSpanKind(SpanKind.INTERNAL).startSpan();
    try (Scope ignored = span.makeCurrent()) {
      span.setAttribute("iceberg.stage", STAGE_DURABILITY);
      span.setAttribute("iceberg.commit-id", commitId);
      span.setAttribute("iceberg.tables", tableCount);
      span.setAttribute("iceberg.partial-commit", partialCommit);
      span.setAttribute("kafka.connect.group-id", connectGroupId);
      runAction(span, commitAction);
    } finally {
      span.end();
    }
  }

  private static void runAction(Span span, Runnable action) {
    try {
      action.run();
    } catch (Throwable t) {
      span.recordException(t);
      String message = t.getMessage();
      span.setStatus(StatusCode.ERROR, message != null ? message : t.getClass().getName());
      throw t;
    }
  }

  private static void setDebeziumAttributes(Span span, SinkRecord record) {
    Object value = record.value();
    if (!(value instanceof Struct)) {
      return;
    }
    Struct struct = (Struct) value;
    if (struct.schema().field("op") != null) {
      Object op = struct.get("op");
      if (op != null) {
        span.setAttribute("op", op.toString());
      }
    }
    if (struct.schema().field("ts_ms") != null) {
      Object tsMs = struct.get("ts_ms");
      if (tsMs instanceof Number) {
        span.setAttribute("ts_ms", ((Number) tsMs).longValue());
      }
    }
  }

  private static boolean resolveOpenTelemetryApiAvailable() {
    try {
      GlobalOpenTelemetry.get();
      return true;
    } catch (NoClassDefFoundError e) {
      // ignored
    }
    return false;
  }
}
