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
 * Data operations that produce snapshots.
 *
 * <p>A snapshot can return the operation that summarizes the changes in the snapshot to help other
 * components ignore snapshots that are not needed for some tasks. For example, snapshot expiration
 * does not need to clean up deleted files for appends, which have no deleted files.
 *
 * <p>An operation describes the changes that were committed in a snapshot, not the intent of the
 * write that produced it. A single API may produce different operations depending on the changes
 * that it commits.
 */
public class DataOperations {
  private DataOperations() {}

  /** Only data files were added and no files were removed. */
  public static final String APPEND = "append";

  /**
   * Data and delete files were added and removed without changing table data; i.e., compaction,
   * changing the data file format, or relocating data files.
   */
  public static final String REPLACE = "replace";

  /**
   * Data files were added, and data files were removed and/or delete files were added to delete
   * rows.
   */
  public static final String OVERWRITE = "overwrite";

  /**
   * Data files were removed and their contents logically deleted and/or delete files were added to
   * delete rows; no data files were added.
   */
  public static final String DELETE = "delete";
}
