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
package org.apache.iceberg.index;

import org.apache.iceberg.catalog.IndexIdentifier;

/**
 * A lightweight summary of an index instance.
 *
 * <p>This interface provides the essential information needed for query optimizers to discover and
 * evaluate indexes available for a given table. It contains only the fields required for the
 * optimizer to decide whether the index is applicable to a query or should be skipped.
 *
 * <p>For full index metadata, use the {@link Index} interface obtained via catalog load operations.
 */
public interface IndexSummary {

  /**
   * Returns the unique identifier for this index instance.
   *
   * @return the index identifier
   */
  IndexIdentifier id();

  /**
   * Returns the type of this index instance.
   *
   * @return the index type
   */
  IndexType type();

  /**
   * Returns the IDs of columns which are stored losslessly in the index.
   *
   * @return an array of column IDs
   */
  int[] indexColumnIds();

  /**
   * Returns the IDs of columns that the index is optimized for retrieval.
   *
   * @return an array of column IDs
   */
  int[] optimizedColumnIds();

  /**
   * Returns the table snapshot IDs for which this index has valid snapshots.
   *
   * @return an array of table snapshot IDs
   */
  long[] availableTableSnapshots();
}
