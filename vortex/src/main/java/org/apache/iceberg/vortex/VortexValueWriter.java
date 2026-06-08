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

import java.util.stream.Stream;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.FieldMetrics;

/**
 * Interface for writing a datum of type {@code D} into Arrow vectors for Vortex file output.
 *
 * <p>Implementations are engine-specific: the generic data path writes {@code Record} objects,
 * while Spark writes {@code InternalRow} objects.
 *
 * @param <D> the type of data to write
 */
public interface VortexValueWriter<D> {
  /**
   * Write a single datum into the Arrow {@link VectorSchemaRoot} at the given row index.
   *
   * <p>The caller manages the {@link VectorSchemaRoot} lifecycle and ensures vectors have been
   * allocated with sufficient capacity.
   */
  void write(D datum, VectorSchemaRoot root, int rowIndex);

  /**
   * Returns per-field metrics collected during writes.
   *
   * <p>Implementations that track metrics should return one {@link FieldMetrics} per column,
   * containing value counts, null counts, NaN counts, and lower/upper bounds.
   */
  default Stream<FieldMetrics<?>> metrics() {
    return Stream.empty();
  }
}
