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

import java.io.Serializable;

/**
 * A scan task.
 */
public interface ScanTask extends Serializable {
  /**
   * @return true if this is a {@link FileScanTask}, false otherwise.
   */
  default boolean isFileScanTask() {
    return false;
  }

  /**
   * @return this cast to {@link FileScanTask} if it is one
   * @throws IllegalStateException if this is not a {@link FileScanTask}
   */
  default FileScanTask asFileScanTask() {
    throw new IllegalStateException("Not a FileScanTask: " + this);
  }

  /**
   * @return true if this is a {@link DataTask}, false otherwise.
   */
  default boolean isDataTask() {
    return false;
  }

  /**
   * @return this cast to {@link DataTask} if it is one
   * @throws IllegalStateException if this is not a {@link DataTask}
   */
  default DataTask asDataTask() {
    throw new IllegalStateException("Not a DataTask: " + this);
  }

  /**
   * @return this cast to {@link CombinedScanTask} if it is one
   * @throws IllegalStateException if this is not a {@link CombinedScanTask}
   */
  default CombinedScanTask asCombinedScanTask() {
    throw new IllegalStateException("Not a CombinedScanTask: " + this);
  }
}
