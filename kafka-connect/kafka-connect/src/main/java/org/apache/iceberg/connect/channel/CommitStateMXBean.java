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
package org.apache.iceberg.connect.channel;

/**
 * JMX MXBean interface for monitoring the commit buffer state. Exposes metrics for stale event
 * eviction, buffer size, and stale group count. Registered under {@code
 * org.apache.iceberg.connect:type=CommitState,connector=<name>}.
 */
public interface CommitStateMXBean {
  /** Cumulative count of stale events evicted by TTL. */
  long getEvictedStaleEventCount();

  /** Number of distinct stale commitId groups currently in the buffer. */
  int getStaleGroupCount();

  /** Total number of envelopes currently in the commit buffer. */
  int getBufferSize();
}
