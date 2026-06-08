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
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

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
        return Data.importVectorSchemaRoot(arrowAllocator, arrowArray, arrowSchema, null);
      }
    }
  }
}
