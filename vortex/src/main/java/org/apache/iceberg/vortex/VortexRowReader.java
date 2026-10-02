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
package org.apache.iceberg.vortex;

import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Reads rows of type {@link T} from Arrow {@link VectorSchemaRoot} batches. Callers must invoke
 * {@link #newBatch(VectorSchemaRoot)} each time they advance to a new batch, then read rows by
 * their position within that batch.
 */
public interface VortexRowReader<T> {
  /**
   * Binds this reader to {@code batch} for subsequent {@link #read} calls, hoisting per-batch work
   * (column resolution, buffer and null-count binding) out of the per-row path.
   */
  void newBatch(VectorSchemaRoot batch);

  /** Reads the row at {@code row} within the current batch. */
  T read(int row);

  /**
   * Requests that {@link #read} refill and return the same top-level container instance on every
   * call instead of allocating a new one per row. Callers that retain rows across calls must copy
   * them. Readers that do not support reuse ignore the request.
   */
  default void reuseContainers(boolean reuse) {}
}
