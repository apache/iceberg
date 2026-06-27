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
package org.apache.iceberg.formats;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.util.CharSequenceMap;

/**
 * Materializes a position delete file directly into one or more {@link PositionDeleteIndex}
 * instances, bypassing the per-row {@link org.apache.iceberg.formats.ReadBuilder} pipeline.
 *
 * <p>Implementations are registered with {@link FormatModelRegistry} per {@link
 * org.apache.iceberg.FileFormat} and consulted by delete loaders before falling back to the generic
 * record-by-record reader. This provides a fast path for engines (e.g. Spark on Arrow) that can
 * decode the delete file's columnar buffers without materializing intermediate records.
 *
 * <p>The contract is intentionally narrow: the interface returns ready-to-query bitmaps, so
 * implementations are free to use direct buffer access, range coalescing, or any other
 * format-specific optimization.
 */
public interface PositionDeleteIndexReader {

  /**
   * Reads the positions in {@code file} that target {@code dataLocation} and returns them as a
   * single index. Implementations must return an empty (but possibly mutable) index when no deletes
   * target the path.
   *
   * @param file the position delete file to read; must not be {@code null}
   * @param dataLocation the data file path to filter on; must not be {@code null}
   * @param deleteFile delete-file metadata recorded with the returned index, may be {@code null}
   * @return a {@link PositionDeleteIndex} containing all positions for {@code dataLocation}
   */
  PositionDeleteIndex read(InputFile file, CharSequence dataLocation, DeleteFile deleteFile);

  /**
   * Reads every position in {@code file} grouped by the data file it targets. Used by the caching
   * path of delete loaders, which holds onto the entire delete file's content keyed by data file
   * path.
   *
   * @param file the position delete file to read; must not be {@code null}
   * @param deleteFile delete-file metadata recorded with each returned index, may be {@code null}
   * @return a map of data file path to its corresponding {@link PositionDeleteIndex}
   */
  CharSequenceMap<PositionDeleteIndex> readAll(InputFile file, DeleteFile deleteFile);
}
