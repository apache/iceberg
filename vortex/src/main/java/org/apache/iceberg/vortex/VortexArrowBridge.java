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

import dev.vortex.arrow.ArrowAllocation;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

final class VortexArrowBridge {
  private static final RootAllocator ARROW_ALLOCATOR = new RootAllocator(Long.MAX_VALUE);

  private VortexArrowBridge() {}

  static BufferAllocator arrowAllocator() {
    return ARROW_ALLOCATOR;
  }

  static dev.vortex.relocated.org.apache.arrow.memory.BufferAllocator vortexAllocator() {
    return ArrowAllocation.rootAllocator();
  }

  static VectorSchemaRoot importVortexRoot(
      dev.vortex.relocated.org.apache.arrow.vector.VectorSchemaRoot vortexRoot,
      dev.vortex.relocated.org.apache.arrow.memory.BufferAllocator vortexAllocator,
      BufferAllocator arrowAllocator) {
    try (dev.vortex.relocated.org.apache.arrow.c.ArrowArray vortexArray =
            dev.vortex.relocated.org.apache.arrow.c.ArrowArray.allocateNew(vortexAllocator);
        dev.vortex.relocated.org.apache.arrow.c.ArrowSchema vortexSchema =
            dev.vortex.relocated.org.apache.arrow.c.ArrowSchema.allocateNew(vortexAllocator)) {
      dev.vortex.relocated.org.apache.arrow.c.Data.exportVectorSchemaRoot(
          vortexAllocator, vortexRoot, null, vortexArray, vortexSchema);

      try (ArrowArray arrowArray = ArrowArray.wrap(vortexArray.memoryAddress())) {
        ArrowSchema arrowSchema = ArrowSchema.wrap(vortexSchema.memoryAddress());
        VectorSchemaRoot imported =
            Data.importVectorSchemaRoot(arrowAllocator, arrowArray, arrowSchema, null);
        return normalizeUnsignedLongs(imported, arrowAllocator);
      }
    }
  }

  /**
   * Rewrites unsigned 64-bit integer columns as signed ones. Vortex's {@code row_idx} expression
   * (used to materialize the {@code _pos} metadata column) produces an unsigned int64 column, which
   * Spark's {@code ArrowColumnVector} cannot read ({@code Int(64, false)} is unsupported). Row
   * positions always fit in a signed long, so the values are copied verbatim into a signed vector.
   * The original imported root is left untouched unless an unsigned column is present.
   */
  private static VectorSchemaRoot normalizeUnsignedLongs(
      VectorSchemaRoot imported, BufferAllocator allocator) {
    boolean hasUnsigned =
        imported.getFieldVectors().stream().anyMatch(vector -> vector instanceof UInt8Vector);
    if (!hasUnsigned) {
      return imported;
    }

    List<FieldVector> vectors = Lists.newArrayListWithCapacity(imported.getFieldVectors().size());
    for (FieldVector vector : imported.getFieldVectors()) {
      if (vector instanceof UInt8Vector) {
        vectors.add(copyAsSigned((UInt8Vector) vector, allocator));
        // The unsigned source is replaced by the signed copy and is not part of the returned root,
        // so release its buffers now. Remaining vectors are moved into the new root unchanged.
        vector.close();
      } else {
        vectors.add(vector);
      }
    }

    VectorSchemaRoot result = new VectorSchemaRoot(vectors);
    result.setRowCount(imported.getRowCount());
    return result;
  }

  private static BigIntVector copyAsSigned(UInt8Vector source, BufferAllocator allocator) {
    int count = source.getValueCount();
    BigIntVector signed = new BigIntVector(source.getField().getName(), allocator);
    signed.allocateNew(count);
    for (int i = 0; i < count; i++) {
      if (source.isNull(i)) {
        signed.setNull(i);
      } else {
        signed.set(i, source.getValueAsLong(i));
      }
    }
    signed.setValueCount(count);
    return signed;
  }
}
