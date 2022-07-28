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
import java.io.IOException;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * The writer interface could accept records and provide the generated data files.
 *
 * @param <T> to indicate the record data type.
 */
public interface TaskWriter<T> extends Closeable {

  /** Write the row into the data files. */
  void write(T row) throws IOException;

  /**
   * Close the writer and delete the completed files if possible when aborting.
   *
   * @throws IOException if any IO error happen.
   */
  void abort() throws IOException;

  /**
   * Close the writer and get the completed data files, it requires that the task writer would
   * produce data files only.
   *
   * @return the completed data files of this task writer.
   */
  default DataFile[] dataFiles() throws IOException {
    WriteResult result = complete();
    Preconditions.checkArgument(
        result.deleteFiles() == null || result.deleteFiles().length == 0,
        "Should have no delete files in this write result.");

    return result.dataFiles();
  }

  /**
   * Close the writer and get the completed data and delete files.
   *
   * @return the completed data and delete files of this task writer.
   */
  WriteResult complete() throws IOException;
}
