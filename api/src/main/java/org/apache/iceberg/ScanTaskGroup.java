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

import java.util.Collection;

/**
 * A scan task that may include partial input files, multiple input files or both.
 *
 * @param <T> the type of scan tasks
 */
public interface ScanTaskGroup<T extends ScanTask> extends ScanTask {
  /** Returns scan tasks in this group. */
  Collection<T> tasks();

  @Override
  default long sizeBytes() {
    return tasks().stream().mapToLong(ScanTask::sizeBytes).sum();
  }

  @Override
  default long rowsCount() {
    return tasks().stream().mapToLong(ScanTask::rowsCount).sum();
  }

  @Override
  default int filesCount() {
    return tasks().stream().mapToInt(ScanTask::filesCount).sum();
  }
}
