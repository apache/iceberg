/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg;

import java.util.List;
import java.util.function.Consumer;

/**
 * API for removing old {@link TableMetadataFile table metadata files} from a table.
 * <p>
 * This API accumulates table metadata files deletions. This API does not allow deleting
 * the current table metadata file.
 * <p>
 * {@link #deleteWith(Consumer)} can be used to pass an alternative deletion method.
 *
 * {@link #apply()} returns a list of the table metadata files that will be removed.
 */
public interface ExpireTableMetadata extends PendingUpdate<List<TableMetadataFile>> {

  /**
   * Expires all table metadata files except of last N.
   *
   * @param number number of table metadata files to be left
   * @return this for method chaining
   */
  ExpireTableMetadata expireExcept(int number);

  /**
   * Expires all table metadata files where last modified time is older than the given timestamp.
   *
   * @param timestampMillis a long timestamp, as returned by {@link System#currentTimeMillis()}
   * @return this for method chaining
   */
  ExpireTableMetadata expireOlderThan(long timestampMillis);

  /**
   * Passes an alternative delete implementation that will be used for table metadata files.
   * <p>
   * If this method is not called, requested table metadata files will still be deleted.
   *
   * @param deleteFunc a function that will be called to delete table metadata files
   * @return this for method chaining
   */
  ExpireTableMetadata deleteWith(Consumer<String> deleteFunc);
}
