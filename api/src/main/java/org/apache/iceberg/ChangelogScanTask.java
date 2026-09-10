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

/** A changelog scan task. */
public interface ChangelogScanTask extends ScanTask {
  /** Returns the type of changes produced by this task (i.e. insert/delete). */
  ChangelogOperation operation();

  /**
   * Returns the ordinal of changes produced by this task. This number indicates the order in which
   * changes produced by this scan must be applied. Operations with a lower ordinal must be applied
   * first.
   */
  int changeOrdinal();

  /** Returns the snapshot ID in which the changes were committed. */
  long commitSnapshotId();

  /**
   * Returns the first row ID assigned to rows in the data file associated with this task, or null
   * if row lineage is not available (i.e. format version &lt; 3 or the file was written before row
   * lineage was enabled).
   *
   * <p>Row IDs are stable, table-scoped 64-bit integers assigned monotonically at commit time. For
   * an added-rows task, this is the base row ID for the newly added file. For a deleted-data-file
   * task, this is the base row ID of the file that was removed.
   *
   * @return the first row ID for the file associated with this task, or null if unavailable
   */
  default Long rowId() {
    return null;
  }
}
