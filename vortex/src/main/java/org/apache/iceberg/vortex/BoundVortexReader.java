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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;

/**
 * Base {@link VortexValueReader} that captures per-batch state on {@link #bind(FieldVector)}. Null
 * checks read the bound validity buffer directly, skipping per-row {@code isNull} calls entirely
 * for all-valid vectors. {@link #bind(FieldVector)} must be called before any row is read.
 */
public abstract class BoundVortexReader<D> implements VortexValueReader<D> {
  // Validity bitmap of the bound vector, or null when every slot is valid.
  private ArrowBuf validity = null;

  @Override
  public final void bind(FieldVector vector) {
    this.validity = vector.getNullCount() > 0 ? vector.getValidityBuffer() : null;
    bindVector(vector);
  }

  /** Captures reader-specific state (typed vectors, buffers, children) for the bound vector. */
  protected abstract void bindVector(FieldVector vector);

  protected final boolean isNullAt(int row) {
    return validity != null && (validity.getByte(row >>> 3) & (1 << (row & 7))) == 0;
  }

  @Override
  public final D read(int row) {
    if (isNullAt(row)) {
      return null;
    }

    return readNonNull(row);
  }
}
