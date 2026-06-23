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
package org.apache.iceberg.data.vortex;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.vortex.VortexValueWriter;

/**
 * Writes {@link PositionDelete} objects to Arrow vectors for Vortex position delete file output.
 *
 * <p>The output schema is [file_path: string, pos: long].
 */
public class PositionDeleteVortexWriter<D> implements VortexValueWriter<PositionDelete<D>> {
  @Override
  public void write(PositionDelete<D> datum, VectorSchemaRoot root, int rowIndex) {
    VarCharVector pathVector = (VarCharVector) root.getVector(0);
    byte[] pathBytes = datum.path().toString().getBytes(StandardCharsets.UTF_8);
    pathVector.setSafe(rowIndex, pathBytes);

    BigIntVector posVector = (BigIntVector) root.getVector(1);
    posVector.setSafe(rowIndex, datum.pos());
  }

  @Override
  public Stream<FieldMetrics<?>> metrics() {
    return Stream.empty();
  }
}
