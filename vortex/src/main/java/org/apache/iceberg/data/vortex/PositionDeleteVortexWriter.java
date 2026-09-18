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
import java.util.Comparator;
import java.util.Map;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.vortex.VortexExactBounds;
import org.apache.iceberg.vortex.VortexValueWriter;

/**
 * Writes {@link PositionDelete} objects to Arrow vectors for Vortex position delete file output.
 *
 * <p>The output schema is [file_path: string, pos: long].
 *
 * <p>The paths are tracked as they are written because Vortex reports string bounds as a truncated
 * prefix range. Iceberg reads a delete file as covering a single data file when {@code file_path}'s
 * bounds are equal, and only rewrites deletes it can attribute to one data file, so a truncated
 * bound would leave every delete file looking partition scoped.
 */
public class PositionDeleteVortexWriter<D>
    implements VortexValueWriter<PositionDelete<D>>, VortexExactBounds {
  private static final Comparator<CharSequence> PATHS = Comparators.charSequences();

  private String lowerPath = null;
  private String upperPath = null;

  @Override
  public void write(PositionDelete<D> datum, VectorSchemaRoot root, int rowIndex) {
    VarCharVector pathVector = (VarCharVector) root.getVector(0);
    // Copied rather than referenced: callers reuse a single PositionDelete across rows, so a path
    // this writer retains has to be independent of it.
    String path = datum.path().toString();
    pathVector.setSafe(rowIndex, path.getBytes(StandardCharsets.UTF_8));

    if (lowerPath == null || PATHS.compare(path, lowerPath) < 0) {
      this.lowerPath = path;
    }

    if (upperPath == null || PATHS.compare(path, upperPath) > 0) {
      this.upperPath = path;
    }

    BigIntVector posVector = (BigIntVector) root.getVector(1);
    posVector.setSafe(rowIndex, datum.pos());
  }

  /**
   * {@inheritDoc}
   *
   * <p>Only {@code file_path} is tracked. Vortex reports exact bounds for the numeric {@code pos}
   * column, so there is nothing to correct there.
   */
  @Override
  public Map<Integer, Pair<Object, Object>> exactBounds() {
    if (lowerPath == null) {
      return ImmutableMap.of();
    }

    return ImmutableMap.of(
        MetadataColumns.DELETE_FILE_PATH.fieldId(), Pair.of(lowerPath, upperPath));
  }
}
