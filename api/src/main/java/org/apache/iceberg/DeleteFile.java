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

public interface DeleteFile extends DataFile {
  /**
   * @return List of recommended split locations, if applicable, null otherwise.
   * When available, this information is used for planning scan tasks whose boundaries
   * are determined by these offsets. The returned list must be sorted in ascending order.
   */
  default List<Long> splitOffsets() {
    return null;
  }

  /**
   * Copies this {@link DataFile data file}. Manifest readers can reuse data file instances; use
   * this method to copy data when collecting files from tasks.
   *
   * @return a copy of this data file
   */
  DeleteFile copy();

  /**
   * Copies this {@link DataFile data file} without file stats. Manifest readers can reuse data file instances; use
   * this method to copy data without stats when collecting files.
   *
   * @return a copy of this data file, without lower bounds, upper bounds, value counts, or null value counts
   */
  DeleteFile copyWithoutStats();
}
