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


import java.io.IOException;
import java.util.Arrays;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class acts as a helper for handling the closure of multiple resource.
 * To use it. register resources to be closed in constructor, and call the corresponding close method when needed.
 * <p>
 * It can take both closeable and autocloseable objects, and handle closeable as autocloseable and guarantee close
 * idempotency by ensuring that each resource will be closed once even with concurrent close calls. It will also
 * wrap checked exceptions into IOException to follow Closeable method signature.
 */
public class CloseGroupHelper {
  private static final Logger LOG = LoggerFactory.getLogger(CloseGroupHelper.class);

  private final Deque<AutoCloseable> closeables;

  /**
   * Register closeables and/or autocloseables to be managed by this class.
   */
  public CloseGroupHelper(AutoCloseable... autoCloseables) {
    this.closeables = Arrays.stream(autoCloseables)
        .filter(Objects::nonNull)
        .collect(Collectors.toCollection(ConcurrentLinkedDeque::new));
  }

  /**
   * Close all the registered resources. Close method of each resource will only be called once.
   * Checked exception from AutoCloseable will be wrapped to IOException. Users can also configure to
   * suppress the failure during closing to ensure the close method of all resources to be called.
   *
   * @param suppressCloseFailure true if user wants to suppress close failures
   * @throws IOException exception thrown if close encounters errors and suppress is false
   */
  public void closeAsCloseable(boolean suppressCloseFailure) throws IOException {
    while (!closeables.isEmpty()) {
      AutoCloseable toClose = closeables.pollFirst();
      if (toClose != null) {
        try {
          toClose.close();
        } catch (Exception e) {
          if (suppressCloseFailure) {
            LOG.error("Exception suppressed when attempting to close resources", e);
          } else if (e instanceof IOException) {
            throw (IOException) e;
          } else if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
          } else {
            throw new IOException("Exception occurs when closing AutoCloseable", e);
          }
        }
      }
    }
  }
}
