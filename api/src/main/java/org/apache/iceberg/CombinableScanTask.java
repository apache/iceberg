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

/**
 * A scan task that can be potentially combined with another scan task.
 *
 * @param <ThisT> the child Java API class
 */
public interface CombinableScanTask<ThisT> extends ScanTask {
  /**
   * Checks if this task can be combined with a given task.
   *
   * @param other another task
   * @return whether the tasks can be combined
   */
  boolean isCombinableWith(ScanTask other);

  /**
   * Combines this task with a given task.
   * <p>
   * Note this method will be called only if {@link #isCombinableWith(ScanTask)} returned true.
   *
   * @param other another task
   * @return a new combined task
   */
  ThisT combineWith(ScanTask other);
}
