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

import java.io.Closeable;
import java.io.IOException;
import java.util.Deque;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class acts as a helper for handling the closure of multiple resource. It can be used for
 * either inheritance or composition. To use it, register resources to be closed via the add()
 * calls, and call the corresponding close method when needed.
 *
 * <p>It can take both closeable and autocloseable objects, and handle closeable as autocloseable
 * and guarantee close idempotency by ensuring that each resource will be closed once even with
 * concurrent close calls. It will also wrap checked non-IO exceptions into runtime exceptions.
 *
 * <p>Users can choose to suppress close failure with this class. By default such failures are not
 * suppressed.
 */
public class CloseableGroup implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CloseableGroup.class);

  private final Deque<AutoCloseable> closeables = Lists.newLinkedList();
  private boolean suppressCloseFailure = false;

  /** Register a closeable to be managed by this class. */
  public void addCloseable(Closeable closeable) {
    closeables.add(closeable);
  }

  /**
   * Register an autocloseables to be managed by this class. It will be handled as a closeable
   * object.
   */
  public void addCloseable(AutoCloseable autoCloseable) {
    closeables.add(autoCloseable);
  }

  /**
   * Whether to suppress failure when any of the closeable this class tracks throws exception during
   * closing. This could be helpful to ensure the close method of all resources to be called.
   *
   * @param shouldSuppress true if user wants to suppress close failures
   */
  public void setSuppressCloseFailure(boolean shouldSuppress) {
    this.suppressCloseFailure = shouldSuppress;
  }

  /**
   * Close all the registered resources. Close method of each resource will only be called once.
   * Checked exception from AutoCloseable will be wrapped to runtime exception.
   */
  @Override
  public void close() throws IOException {
    while (!closeables.isEmpty()) {
      AutoCloseable toClose = closeables.pollFirst();
      if (toClose != null) {
        try {
          toClose.close();
        } catch (Exception e) {
          if (suppressCloseFailure) {
            LOG.error("Exception suppressed when attempting to close resources", e);
          } else {
            ExceptionUtil.castAndThrow(e, IOException.class);
          }
        }
      }
    }
  }
}
