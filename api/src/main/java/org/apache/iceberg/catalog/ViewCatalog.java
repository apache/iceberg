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
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;

/** A Catalog API for view create, drop, and load operations. */
public interface ViewCatalog {

  /**
   * Return the name for this catalog.
   *
   * @return this catalog's name
   */
  String name();

  /**
   * Return all the identifiers under this namespace.
   *
   * @param namespace a namespace
   * @return a list of identifiers for views
   * @throws NotFoundException if the namespace is not found
   */
  List<TableIdentifier> listViews(Namespace namespace);

  /**
   * Load a view.
   *
   * @param identifier a view identifier
   * @return instance of {@link View} implementation referred by the identifier
   * @throws NoSuchViewException if the view does not exist
   */
  View loadView(TableIdentifier identifier);

  /**
   * Check whether view exists.
   *
   * @param identifier a view identifier
   * @return true if the view exists, false otherwise
   */
  default boolean viewExists(TableIdentifier identifier) {
    try {
      loadView(identifier);
      return true;
    } catch (NoSuchViewException e) {
      return false;
    }
  }

  /**
   * Instantiate a builder to create or replace a SQL view.
   *
   * @param identifier a view identifier
   * @return a view builder
   */
  ViewBuilder buildView(TableIdentifier identifier);

  /**
   * Drop a view.
   *
   * @param identifier a view identifier
   * @return true if the view was dropped, false if the view did not exist
   */
  boolean dropView(TableIdentifier identifier);

  /**
   * Rename a view.
   *
   * @param from identifier of the view to rename
   * @param to new view identifier
   * @throws NoSuchViewException if the "from" view does not exist
   * @throws AlreadyExistsException if the "to" view already exists
   */
  void renameView(TableIdentifier from, TableIdentifier to);

  /**
   * Invalidate cached view metadata from current catalog.
   *
   * <p>If the view is already loaded or cached, drop cached data. If the view does not exist or is
   * not cached, do nothing.
   *
   * @param identifier a view identifier
   */
  default void invalidateView(TableIdentifier identifier) {}

  /**
   * Initialize a view catalog given a custom name and a map of catalog properties.
   *
   * <p>A custom view catalog implementation must have a no-arg constructor. A compute engine like
   * Spark or Flink will first initialize the catalog without any arguments, and then call this
   * method to complete catalog initialization with properties passed into the engine.
   *
   * @param name a custom name for the catalog
   * @param properties catalog properties
   */
  default void initialize(String name, Map<String, String> properties) {}
}
