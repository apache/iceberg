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

import java.util.List;
import org.apache.iceberg.catalog.IndexCatalog;
import org.apache.iceberg.catalog.IndexIdentifier;

/**
 * A builder used to create or replace an {@link Index}.
 *
 * <p>Call {@link IndexCatalog#buildIndex(IndexIdentifier)} to create a new builder.
 */
public interface IndexBuilder
    extends VersionBuilder<IndexBuilder>,
        SnapshotBuilder<IndexBuilder>,
        RemoveSnapshotsBuilder<IndexBuilder> {

  /**
   * Set the index type.
   *
   * @param type the type of the index (e.g., BTREE, TERM, IVF)
   * @return this for method chaining
   */
  IndexBuilder withType(IndexType type);

  /**
   * Set the column IDs to be stored losslessly in the index.
   *
   * @param columnIds the IDs of columns contained by the index
   * @return this for method chaining
   */
  IndexBuilder withIndexColumnIds(List<Integer> columnIds);

  /**
   * Set the column IDs to be stored losslessly in the index.
   *
   * @param columnIds the IDs of columns contained by the index
   * @return this for method chaining
   */
  IndexBuilder withIndexColumnIds(int... columnIds);

  /**
   * Set the column IDs that this index is optimized for.
   *
   * @param columnIds the IDs of columns the index is optimized for retrieval
   * @return this for method chaining
   */
  IndexBuilder withOptimizedColumnIds(List<Integer> columnIds);

  /**
   * Set the column IDs that this index is optimized for.
   *
   * @param columnIds the IDs of columns the index is optimized for retrieval
   * @return this for method chaining
   */
  IndexBuilder withOptimizedColumnIds(int... columnIds);

  /**
   * Sets a location for the index.
   *
   * @param location the base location to set for the index; used to create index file locations
   * @return this for method chaining
   */
  default IndexBuilder withLocation(String location) {
    throw new UnsupportedOperationException("Setting an index's location is not supported");
  }

  /**
   * Create the index.
   *
   * @return the index created
   */
  Index create();

  /**
   * Replace the index.
   *
   * @return the {@link Index} replaced
   */
  Index replace();

  /**
   * Create or replace the index.
   *
   * @return the {@link Index} created or replaced
   */
  Index createOrReplace();
}
