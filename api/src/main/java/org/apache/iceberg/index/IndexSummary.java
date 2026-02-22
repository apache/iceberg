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

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.IndexCatalog;
import org.apache.iceberg.catalog.IndexIdentifier;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * A compact representation of {@link Index} metadata for discovery and evaluation purposes.
 *
 * <p>This interface exposes only the essential attributes required by query optimizers to determine
 * whether an index is applicable to a given query. Available indexes for a {@link Table} can be
 * retrieved via {@link IndexCatalog#listIndexes(TableIdentifier, IndexType...)}.
 *
 * <p>To access complete index metadata, load the full {@link Index} instance using {@link
 * IndexCatalog#loadIndex(IndexIdentifier)}.
 */
public interface IndexSummary {

  /** Returns the unique identifier for this index instance. */
  IndexIdentifier id();

  /** Returns the type of this index instance. */
  IndexType type();

  /** Returns the IDs of columns which are stored losslessly in the index. */
  int[] indexColumnIds();

  /** Returns the IDs of columns that the index is optimized for retrieval. */
  int[] optimizedColumnIds();

  /** Returns the table snapshot IDs for that have corresponding index snapshots. */
  long[] availableTableSnapshots();
}
