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
 * <p>A snapshot can return the operation that created the snapshot to help other components ignore
 * snapshots that are not needed for some tasks. For example, snapshot expiration does not need to
 * clean up deleted files for appends, which have no deleted files.
 */
public class DataOperations {
  private DataOperations() {}

  /**
   * New data is appended to the table and no data is removed or deleted.
   *
   * <p>This operation is implemented by {@link AppendFiles}.
   */
  public static final String APPEND = "append";

  /**
   * Files are removed and replaced, without changing the data in the table.
   *
   * <p>This operation is implemented by {@link RewriteFiles}.
   */
  public static final String REPLACE = "replace";

  /**
   * New data is added to overwrite existing data.
   *
   * <p>This operation is implemented by {@link OverwriteFiles} and {@link ReplacePartitions}.
   */
  public static final String OVERWRITE = "overwrite";

  /**
   * Data is deleted from the table and no data is added.
   *
   * <p>This operation is implemented by {@link DeleteFiles}.
   */
  public static final String DELETE = "delete";
}
