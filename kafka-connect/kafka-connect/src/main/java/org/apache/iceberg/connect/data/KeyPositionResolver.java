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
package org.apache.iceberg.connect.data;

import java.io.IOException;
import java.util.Collection;
import java.util.function.LongConsumer;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.util.StructLikeSet;

/** Finds the rows of a table snapshot whose identifier values are in a set of deleted keys. */
interface KeyPositionResolver {

  /**
   * Resolves the keys of one group of equality delete files.
   *
   * @param base snapshot whose rows are resolved
   * @param keySchema the identifier columns, a projection of the table schema
   * @param keys deleted keys in the internal representation of {@code keySchema}, as produced by
   *     {@link org.apache.iceberg.data.InternalRecordWrapper}
   * @param deleteSpec partition spec of the equality delete files
   * @param deletePartition partition of the equality delete files. If {@code deleteSpec} is
   *     partitioned, only rows of data files with the same spec ID and partition values are
   *     returned; otherwise rows of every data file are candidates
   * @return the matched rows, grouped by data file
   */
  Resolution resolve(
      Snapshot base,
      Schema keySchema,
      StructLikeSet keys,
      PartitionSpec deleteSpec,
      StructLike deletePartition)
      throws IOException;

  /** Rows matched by one resolution. */
  interface Resolution {
    Collection<FileMatches> matches();

    /**
     * Number of data files planned as candidates, before the spec and partition of each candidate
     * are checked against the deletes.
     */
    int plannedDataFiles();
  }

  /** Matched rows of one data file. */
  interface FileMatches {
    String path();

    PartitionSpec spec();

    StructLike partition();

    /**
     * Position deletes the data file already has in the base snapshot: its deletion vector, or
     * position delete files left from format version 2. Returns null only if the file has none. The
     * converter merges them into the new deletion vector, which replaces them for readers, so
     * returning null for a file that has some leaves it with two vectors or brings back rows the
     * older deletes removed.
     */
    PositionDeleteIndex existingDeletes();

    void forEachPosition(LongConsumer consumer);
  }
}
