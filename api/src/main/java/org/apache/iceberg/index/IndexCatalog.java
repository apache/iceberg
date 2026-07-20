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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * Catalog interface for managing secondary indexes on Iceberg tables.
 *
 * <p>An index catalog stores a pointer to the current index metadata file for each registered
 * index. Commits are atomic: {@link #updateIndex} replaces the metadata pointer only if the
 * current pointer still matches the base that the caller read.
 */
public interface IndexCatalog {

  /**
   * Register a new index in the catalog.
   *
   * @param identifier the index identifier
   * @param metadata the initial index metadata (must have a metadataFileLocation set)
   * @throws AlreadyExistsException if an index with the same identifier already exists
   */
  void createIndex(IndexIdentifier identifier, IndexMetadata metadata);

  /**
   * Load the current metadata for an index.
   *
   * @param identifier the index identifier
   * @return the current index metadata
   * @throws NoSuchTableException if the index does not exist
   */
  IndexMetadata loadIndex(IndexIdentifier identifier);

  /**
   * Atomically replace the current index metadata.
   *
   * <p>The update succeeds only if the current metadata file location matches
   * {@code base.metadataFileLocation()}. If another writer has already committed a newer version,
   * this call throws {@link java.util.ConcurrentModificationException}.
   *
   * @param identifier the index identifier
   * @param base the metadata the caller read (used for optimistic concurrency check)
   * @param updated the new metadata to commit (must have a new metadataFileLocation)
   */
  void updateIndex(IndexIdentifier identifier, IndexMetadata base, IndexMetadata updated);

  /**
   * Remove an index from the catalog.
   *
   * <p>Does not delete the underlying index files — callers are responsible for cleanup.
   *
   * @param identifier the index identifier
   * @throws NoSuchTableException if the index does not exist
   */
  void dropIndex(IndexIdentifier identifier);

  /**
   * Check if an index exists.
   *
   * @param identifier the index identifier
   * @return true if the index exists
   */
  boolean indexExists(IndexIdentifier identifier);

  /**
   * List all indexes registered for a table.
   *
   * @param tableIdentifier the table identifier
   * @return list of index metadata for all indexes on the table, may be empty
   */
  List<IndexMetadata> listIndexes(TableIdentifier tableIdentifier);
}
