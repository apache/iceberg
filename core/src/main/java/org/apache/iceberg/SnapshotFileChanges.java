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

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * A snapshot's data file and standalone delete file changes.
 *
 * <p>Returned views are repeatable and require no caller-managed resources.
 */
interface SnapshotFileChanges {
  /** Returns the data files added by this snapshot. */
  Iterable<DataFile> addedDataFiles();

  /** Returns data files removed from the dataset, excluding metadata replacements. */
  Iterable<DataFile> removedDataFiles();

  /** Returns the new metadata for data files modified by this snapshot. */
  default Iterable<DataFile> modifiedDataFiles() {
    return ImmutableList.of();
  }

  /** Returns the previous metadata for data files modified by this snapshot. */
  default Iterable<DataFile> replacedDataFiles() {
    return ImmutableList.of();
  }

  /** Returns standalone delete files added by this snapshot. */
  Iterable<DeleteFile> addedDeleteFiles();

  /** Returns standalone delete files removed by this snapshot. */
  Iterable<DeleteFile> removedDeleteFiles();
}
