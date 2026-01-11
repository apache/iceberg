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

import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * Extension interface for Catalog that supports context-aware table loading. This enables passing
 * additional context (such as view information) when loading tables.
 */
public interface ContextAwareTableCatalog {

  /**
   * Context key for the view identifier(s) that reference the table being loaded.
   *
   * <p>The value should be a List&lt;TableIdentifier&gt; representing the view(s) that reference
   * the table. For nested views (a view referencing another view which references the table), the
   * list should be ordered from outermost view to the view that directly references the table.
   *
   * <p>This structured format keeps namespace parts and view names separate throughout the catalog
   * layer, only being serialized into the REST API format when making the actual HTTP request
   * according to the spec: namespace parts joined by namespace separator (default 0x1F), followed
   * by dot, followed by view name, with multiple views comma-separated.
   */
  String VIEW_IDENTIFIER_KEY = "view.referenced-by";

  /**
   * Load a table with additional context information.
   *
   * <p>Common context keys:
   *
   * <ul>
   *   <li>{@link #VIEW_IDENTIFIER_KEY}: A List&lt;TableIdentifier&gt; of view(s) referencing this
   *       table
   * </ul>
   *
   * @param identifier the table identifier to load
   * @param loadingContext additional context information as key-value pairs
   * @return the loaded table
   * @throws NoSuchTableException if the table does not exist
   */
  Table loadTable(TableIdentifier identifier, Map<String, Object> loadingContext)
      throws NoSuchTableException;
}
