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
package org.apache.iceberg;

import java.util.List;

/** Interface for delete files listed in a table delete manifest. */
public interface DeleteFile extends ContentFile<DeleteFile> {
  /**
   * @return List of recommended split locations, if applicable, null otherwise. When available,
   *     this information is used for planning scan tasks whose boundaries are determined by these
   *     offsets. The returned list must be sorted in ascending order.
   */
  @Override
  default List<Long> splitOffsets() {
    return null;
  }

  /**
   * Returns the location of a data file that all deletes reference.
   *
   * <p>The referenced data file is required for deletion vectors and may be optionally captured for
   * position delete files that apply to only one data file. This method always returns null for
   * equality delete files.
   */
  default String referencedDataFile() {
    return null;
  }

  /**
   * Returns the offset in the file where the content starts.
   *
   * <p>The content offset is required for deletion vectors and points to the start of the deletion
   * vector blob in the Puffin file, enabling direct access. This method always returns null for
   * equality and position delete files.
   */
  default Long contentOffset() {
    return null;
  }

  /**
   * Returns the length of referenced content stored in the file.
   *
   * <p>The content size is required for deletion vectors and indicates the size of the deletion
   * vector blob in the Puffin file, enabling direct access. This method always returns null for
   * equality and position delete files.
   */
  default Long contentSizeInBytes() {
    return null;
  }
}
