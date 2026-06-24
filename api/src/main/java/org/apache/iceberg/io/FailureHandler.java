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
package org.apache.iceberg.io;

import org.slf4j.Logger;

/**
 * Callback invoked by {@link SupportsBulkOperations} for each per-file failure during a bulk
 * operation.
 *
 * <p>FileIO implementations invoke this once per failed object with a {@link FileFailure} carrying
 * the path, a best-effort {@link FailureCategory}, and any available raw error code / cause.
 * Implementations of this interface decide what to do with that information (log, buffer, build a
 * retry list, etc.) — they should not throw; any exception thrown from the handler will be
 * swallowed and logged by the FileIO so a misbehaving handler cannot poison the bulk operation.
 *
 * <p><b>Thread safety:</b> implementations must be thread-safe. FileIO implementations delete
 * objects concurrently (e.g. S3 and ADLS report failures from worker threads), so {@link
 * #onFailure(FileFailure)} may be invoked concurrently from multiple threads for the same bulk
 * operation. Handlers that accumulate state must guard it accordingly (e.g. with a {@code
 * java.util.concurrent.CopyOnWriteArrayList} or other synchronization).
 */
@FunctionalInterface
public interface FailureHandler {
  /** Handler that drops every failure. */
  FailureHandler NOOP = failure -> {};

  void onFailure(FileFailure failure);

  /**
   * Invokes {@code handler} for a single failure, swallowing and logging any exception it throws.
   *
   * <p>This enforces the {@link FailureHandler} contract that a misbehaving handler cannot poison a
   * bulk operation. FileIO implementations should route every per-file failure through this helper
   * rather than calling {@link #onFailure(FileFailure)} directly.
   *
   * @param logger the FileIO's logger, used to report a handler that threw
   * @param handler the handler to invoke
   * @param failure the failure to report
   */
  static void safeNotify(Logger logger, FailureHandler handler, FileFailure failure) {
    try {
      handler.onFailure(failure);
    } catch (RuntimeException handlerError) {
      logger.warn(
          "FailureHandler threw while reporting failure for {}", failure.path(), handlerError);
    }
  }
}
