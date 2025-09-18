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
package org.apache.iceberg.deletes;

import java.io.Closeable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.DeleteWriteResult;

/** A deletion vector file writer. */
public interface DVFileWriter extends Closeable {
  /**
   * Marks a position in a given data file as deleted.
   *
   * @param path the data file path
   * @param pos the data file position
   * @param spec the data file partition spec
   * @param partition the data file partition
   */
  void delete(String path, long pos, PartitionSpec spec, StructLike partition);

  /**
   * Returns a result that contains information about written {@link DeleteFile}s. The result is
   * valid only after the writer is closed.
   *
   * @return the writer result
   */
  DeleteWriteResult result();
}
