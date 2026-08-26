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
package org.apache.iceberg.spark;

import java.io.Serializable;

/**
 * Write times summed across the tasks of a write.
 *
 * <p>Tasks report their own times in their commit messages. Spark collects task metrics before it
 * calls {@code DataWriter.commit()}, so the close time, where buffered rows are flushed, is not yet
 * known at that point and cannot be reported as a task metric. Both times therefore travel back to
 * the driver with the commit messages and are aggregated here.
 */
public class WriteDurations implements Serializable {

  public static final WriteDurations EMPTY = new WriteDurations(0L, 0L);

  private final long writeNanos;
  private final long closeNanos;

  private WriteDurations(long writeNanos, long closeNanos) {
    this.writeNanos = writeNanos;
    this.closeNanos = closeNanos;
  }

  public static WriteDurations of(long writeNanos, long closeNanos) {
    return new WriteDurations(writeNanos, closeNanos);
  }

  public WriteDurations combine(WriteDurations other) {
    return new WriteDurations(writeNanos + other.writeNanos, closeNanos + other.closeNanos);
  }

  public long writeNanos() {
    return writeNanos;
  }

  public long closeNanos() {
    return closeNanos;
  }
}
