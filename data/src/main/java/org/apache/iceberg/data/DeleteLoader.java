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
package org.apache.iceberg.data;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.util.StructLikeSet;

/** An API for loading delete file content into in-memory data structures. */
public interface DeleteLoader {
  /**
   * Loads the content of equality delete files into a set.
   *
   * @param deleteFiles equality delete files
   * @param projection a projection of columns to load
   * @return a set of equality deletes
   */
  StructLikeSet loadEqualityDeletes(Iterable<DeleteFile> deleteFiles, Schema projection);

  /**
   * Loads the content of position delete files for a given data file path into a position index.
   *
   * @param deleteFiles position delete files
   * @param filePath the data file path for which to load deletes
   * @return a position delete index for the provided data file path
   */
  PositionDeleteIndex loadPositionDeletes(Iterable<DeleteFile> deleteFiles, CharSequence filePath);
}
