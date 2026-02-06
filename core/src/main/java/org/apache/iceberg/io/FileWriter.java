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

/**
 * A writer capable of writing files of a single type (i.e. data/delete) to one spec/partition.
 *
 * <p>As opposed to {@link FileAppender}, this interface should be implemented by classes that not
 * only append records to files but actually produce {@link DataFile}s or {@link DeleteFile}s
 * objects with Iceberg metadata. Implementations may wrap {@link FileAppender}s with extra
 * information such as spec, partition, sort order ID needed to construct {@link DataFile}s or
 * {@link DeleteFile}s.
 *
 * @param <T> the row type
 * @param <R> the result type
 */
public interface FileWriter<T, R> extends Closeable {

  /**
   * Writes rows to a predefined spec/partition.
   *
   * @param rows data or delete records
   */
  default void write(Iterable<T> rows) {
    for (T row : rows) {
      write(row);
    }
  }

  /**
   * Writes a row to a predefined spec/partition.
   *
   * @param row a data or delete record
   */
  void write(T row);

  /**
   * Returns the number of bytes that were currently written by this writer.
   *
   * @return the number of written bytes
   */
  long length();

  /**
   * Returns a result that contains information about written {@link DataFile}s or {@link
   * DeleteFile}s. The result is valid only after the writer is closed.
   *
   * @return the file writer result
   */
  R result();
}
