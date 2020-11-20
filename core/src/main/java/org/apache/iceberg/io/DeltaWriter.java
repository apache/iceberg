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

/**
 * The write interface could accept INSERT, POS-DELETION, EQUALITY-DELETION. It usually write those operations
 * in a given partition or bucket.
 */
public interface DeltaWriter<T> extends Closeable {

  /**
   * Write the insert record.
   */
  void writeRow(T row);

  /**
   * Write the equality delete record.
   */
  void writeEqualityDelete(T equalityDelete);

  /**
   * Write the deletion with file path and position into underlying system.
   */
  default void writePosDelete(CharSequence path, long offset) {
    writePosDelete(path, offset, null);
  }

  /**
   * Write the deletion with file path, position and original row into underlying system.
   */
  void writePosDelete(CharSequence path, long offset, T row);

  /**
   * Abort the writer to clean all generated files.
   */
  void abort();

  /**
   * Close the writer and get all the completed data files and delete files.
   */
  WriterResult complete();
}
