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

import org.apache.arrow.vector.FieldVector;

/**
 * Reads a {@link D} from a bound Arrow {@link FieldVector} at a row.
 *
 * <p>Readers are bound to a vector once per batch via {@link #bind(FieldVector)} and then read rows
 * from it, letting them hoist per-batch work (null counts, buffers, child vector resolution) out of
 * the per-row path. {@link #bind(FieldVector)} must be called before rows are read and again
 * whenever a new batch arrives.
 */
public interface VortexValueReader<D> {
  /**
   * Binds this reader to the vector it will read from until the next call. The default is a no-op
   * for readers that do not read from a vector (constants).
   */
  default void bind(FieldVector vector) {}

  /** Reads the value at {@code row}, or null when the slot is null. */
  D read(int row);

  /** Reads the value at {@code row}, which must be a non-null slot. */
  D readNonNull(int row);
}
