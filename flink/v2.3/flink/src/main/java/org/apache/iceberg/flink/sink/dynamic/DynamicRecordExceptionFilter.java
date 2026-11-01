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
package org.apache.iceberg.flink.sink.dynamic;

import java.io.Serializable;
import org.apache.flink.annotation.Experimental;

/**
 * Classifies a failure raised while processing a {@link DynamicRecord} as divertable or not. Keeps
 * error handling fail-safe: only failures this filter whitelists are diverted to the error stream
 * set via {@link DynamicIcebergSink.Builder#errorStreamConsumer}; every other failure propagates
 * and fails the job so Flink's restart strategy can retry it.
 *
 * <p>Whitelist only deterministic, record-scoped failures (bad data, for example a value that
 * cannot be encoded for its table, or an upsert record whose partition source is not an equality
 * field). Return {@code false} for transient or systemic failures (an unreachable catalog,
 * object-store errors, an interrupt) so a sustained outage does not silently drain the whole
 * stream.
 */
@Experimental
@FunctionalInterface
public interface DynamicRecordExceptionFilter extends Serializable {

  /**
   * Classifies a failure as divertable or not.
   *
   * @param error the cause thrown while processing a record
   * @return {@code true} to divert the record, {@code false} to propagate the failure and fail the
   *     job
   */
  boolean shouldDivert(Exception error);
}
