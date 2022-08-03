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
package org.apache.iceberg.io;

import java.io.Closeable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;

/**
 * A writer capable of writing data and position deletes that may belong to different specs and
 * partitions.
 *
 * @param <T> the row type
 */
public interface PositionDeltaWriter<T> extends Closeable {

  /**
   * Inserts a row to the provided spec/partition.
   *
   * @param row a data record
   * @param spec a partition spec
   * @param partition a partition or null if the spec is unpartitioned
   */
  void insert(T row, PartitionSpec spec, StructLike partition);

  /**
   * Inserts a new version of an existing row to the provided spec/partition.
   *
   * <p>This method allows writers to distinguish new and updated records. The caller must
   * separately invoke {@link #delete(CharSequence, long, PartitionSpec, StructLike)} for the
   * original row position that is being updated.
   *
   * @param row a new version of an existing row
   * @param spec a new partition spec
   * @param partition a new partition or null if the spec is unpartitioned
   */
  default void update(T row, PartitionSpec spec, StructLike partition) {
    insert(row, spec, partition);
  }

  /**
   * Deletes a position in the provided spec/partition.
   *
   * @param path a data file path
   * @param pos a position
   * @param spec a partition spec
   * @param partition a partition or null if the spec is unpartitioned
   */
  default void delete(CharSequence path, long pos, PartitionSpec spec, StructLike partition) {
    delete(path, pos, null, spec, partition);
  }

  /**
   * Deletes a position in the provided spec/partition and records the deleted row in the delete
   * file.
   *
   * @param path a data file path
   * @param pos a position
   * @param row a deleted row
   * @param spec a partition spec
   * @param partition a partition or null if the spec is unpartitioned
   */
  void delete(CharSequence path, long pos, T row, PartitionSpec spec, StructLike partition);

  /**
   * Returns a result that contains information about written {@link DataFile}s or {@link
   * DeleteFile}s. The result is valid only after the writer is closed.
   *
   * @return the writer result
   */
  WriteResult result();
}
