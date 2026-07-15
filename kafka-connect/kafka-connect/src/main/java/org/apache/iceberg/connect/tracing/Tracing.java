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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Central entry point for opt-in OpenTelemetry tracing in the Kafka Connect sink. */
public final class Tracing {

  private static final Logger LOG = LoggerFactory.getLogger(Tracing.class);
  private static final AtomicBoolean WARNED_MISSING_API = new AtomicBoolean(false);

  private Tracing() {}

  public static boolean isActive(IcebergSinkConfig config) {
    return config.tracingEnabled() && TracingUtils.isOpenTelemetryAvailable();
  }

  /**
   * Logs a one-time warning when tracing is enabled in config but the OpenTelemetry API is not on
   * the classpath.
   */
  public static void ensureConfigured(IcebergSinkConfig config) {
    if (config.tracingEnabled()
        && !TracingUtils.isOpenTelemetryAvailable()
        && WARNED_MISSING_API.compareAndSet(false, true)) {
      LOG.warn(
          "iceberg.tracing.enabled=true but OpenTelemetry API is not on the classpath. "
              + "Tracing will be skipped.");
    }
  }

  public static void traceRecordIngest(
      IcebergSinkConfig config, SinkRecord record, String tableName, Runnable ingestAction) {
    if (isActive(config)) {
      TracingUtils.traceRecordIngest(record, tableName, ingestAction);
    } else {
      ingestAction.run();
    }
  }

  public static void traceCommit(
      IcebergSinkConfig config,
      String commitId,
      boolean partialCommit,
      int tableCount,
      String connectGroupId,
      Runnable commitAction) {
    if (isActive(config)) {
      TracingUtils.traceCommit(commitId, partialCommit, tableCount, connectGroupId, commitAction);
    } else {
      commitAction.run();
    }
  }
}
