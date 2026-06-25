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
package org.apache.iceberg.flink.sink;

import java.io.Serializable;

/**
 * Gate that controls whether a checkpoint's committables should be emitted downstream.
 *
 * <p>When {@link #isCommitAllowed} returns {@code false}, the {@link IcebergWriteAggregator}
 * buffers committables in Flink operator state and retries on the next checkpoint. When the gate
 * transitions from closed to open, all buffered committables are flushed.
 *
 * <p>Use cases include:
 *
 * <ul>
 *   <li>Honoring catalog maintenance windows
 *   <li>External pause signals or rate limiting
 * </ul>
 */
@FunctionalInterface
public interface CommitGate extends Serializable {
  boolean isCommitAllowed(long checkpointId);
}
