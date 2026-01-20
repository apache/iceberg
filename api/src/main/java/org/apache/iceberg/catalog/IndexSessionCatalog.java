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
package org.apache.iceberg.catalog;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchIndexException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.index.Index;
import org.apache.iceberg.index.IndexBuilder;
import org.apache.iceberg.index.IndexSummary;
import org.apache.iceberg.index.IndexType;

/**
 * A session Catalog API for index create, drop, and load operations.
 *
 * <p>Indexes are specialized data structures that improve the speed of data retrieval operations on
 * a database table. An index instance is uniquely identified by its {@link IndexIdentifier}, which
 * is constructed by combining the {@link TableIdentifier} with the index name.
 */
public interface IndexSessionCatalog {

  /**
   * Return the name for this catalog.
   *
   * @return this catalog's name
   */
  String name();

  /**
   * Return a list of index instances for the specified table, filtered to include only those whose
   * type matches one of the provided types.
   *
   * <p>This enables query optimizers to discover the indexes available for a given table. The
   * returned list is already filtered to include only index types supported by the engine.
   *
   * @param context a session context
   * @param tableIdentifier the identifier of the table to list indexes for
   * @param types the index types to filter by; if empty, returns all indexes
   * @return a list of index summaries matching the criteria
   * @throws NoSuchTableException if the table does not exist
   */
  List<IndexSummary> listIndexes(
      SessionCatalog.SessionContext context, TableIdentifier tableIdentifier, IndexType... types);

  /**
   * Load an index.
   *
   * @param context a session context
   * @param identifier an index identifier
   * @return instance of {@link Index} implementation referred by the identifier
   * @throws NoSuchIndexException if the index does not exist
   */
  Index loadIndex(SessionCatalog.SessionContext context, IndexIdentifier identifier);

  /**
   * Check whether an index exists.
   *
   * @param context a session context
   * @param identifier an index identifier
   * @return true if the index exists, false otherwise
   */
  default boolean indexExists(SessionCatalog.SessionContext context, IndexIdentifier identifier) {
    try {
      loadIndex(context, identifier);
      return true;
    } catch (NoSuchIndexException e) {
      return false;
    }
  }

  /**
   * Instantiate a builder to create or update an index.
   *
   * @param context a session context
   * @param identifier an index identifier
   * @return an index builder
   */
  IndexBuilder buildIndex(SessionCatalog.SessionContext context, IndexIdentifier identifier);

  /**
   * Drop an index.
   *
   * @param context a session context
   * @param identifier an index identifier
   * @return true if the index was dropped, false if the index did not exist
   */
  boolean dropIndex(SessionCatalog.SessionContext context, IndexIdentifier identifier);

  /**
   * Invalidate cached index metadata from current catalog.
   *
   * <p>If the index is already loaded or cached, drop cached data. If the index does not exist or
   * is not cached, do nothing.
   *
   * @param context a session context
   * @param identifier an index identifier
   */
  default void invalidateIndex(SessionCatalog.SessionContext context, IndexIdentifier identifier) {}

  /**
   * Register an index with the catalog if it does not exist.
   *
   * @param context a session context
   * @param identifier an index identifier
   * @param metadataFileLocation the location of a metadata file
   * @return an Index instance
   * @throws AlreadyExistsException if an index with the same identifier already exists in the
   *     catalog.
   */
  default Index registerIndex(
      SessionCatalog.SessionContext context,
      IndexIdentifier identifier,
      String metadataFileLocation) {
    throw new UnsupportedOperationException("Registering index is not supported");
  }

  /**
   * Initialize an index catalog given a custom name and a map of catalog properties.
   *
   * <p>A custom index catalog implementation must have a no-arg constructor. A compute engine like
   * Spark or Flink will first initialize the catalog without any arguments, and then call this
   * method to complete catalog initialization with properties passed into the engine.
   *
   * @param name a custom name for the catalog
   * @param properties catalog properties
   */
  void initialize(String name, Map<String, String> properties);
}
