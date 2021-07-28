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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CloseableGroup implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CloseableGroup.class);

  private final Deque<AutoCloseable> closeables = Lists.newLinkedList();
  private boolean suppressCloseFailure = false;

  protected void addCloseable(AutoCloseable closeable) {
    closeables.add(closeable);
  }

  protected void setSuppressCloseFailure(boolean shouldSuppress) {
    this.suppressCloseFailure = shouldSuppress;
  }

  @Override
  public void close() throws IOException {
    while (!closeables.isEmpty()) {
      AutoCloseable toClose = closeables.removeFirst();
      if (toClose != null) {
        if (suppressCloseFailure) {
          try {
            toClose.close();
          } catch (Exception e) {
            LOG.error("Exception suppressed when attempting to close resources", e);
          }
        } else {
          try {
            toClose.close();
          } catch (Exception e) {
            if (e instanceof IOException) {
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
}
