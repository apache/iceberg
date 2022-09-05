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
 * A writer capable of writing files of a single type (i.e. data/delete) to multiple specs and
 * partitions.
 *
 * <p>As opposed to {@link FileWriter}, this interface should be implemented by writers that are not
 * limited to writing to a single spec/partition. Implementations may internally use {@link
 * FileWriter}s for writing to a single spec/partition.
 *
 * <p>Note that this writer can be used both for partitioned and unpartitioned tables.
 *
 * @param <T> the row type
 * @param <R> the result type
 */
public interface PartitioningWriter<T, R> extends Closeable {

  /**
   * Writes a row to the provided spec/partition.
   *
   * @param row a data or delete record
   * @param spec a partition spec
   * @param partition a partition or null if the spec is unpartitioned
   */
  void write(T row, PartitionSpec spec, StructLike partition);

  /**
   * Returns a result that contains information about written {@link DataFile}s or {@link
   * DeleteFile}s. The result is valid only after the writer is closed.
   *
   * @return the writer result
   */
  R result();
}
