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
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * Extension interface for Catalog that supports context-aware table loading. This enables passing
 * additional context (such as view information) when loading tables.
 */
public interface ContextAwareTableCatalog {

  /** Context key for the view identifier chain that references a table. */
  String VIEW_IDENTIFIER_KEY = "view.referenced-by";

  /**
   * Property key used to propagate the pre-encoded referenced-by query parameter value through
   * table/view configuration properties to credential providers. The value is the URL-encoded
   * referenced-by string (e.g., "ns%1FviewName" or "ns1%1Fv1,ns2%1Fv2" for chains).
   */
  String REFERENCED_BY_PROPERTY = "referenced-by";

  /**
   * Load a table with additional context information.
   *
   * <p>Common context keys:
   *
   * <ul>
   *   <li>{@link #VIEW_IDENTIFIER_KEY}: A {@link List} of {@link TableIdentifier} representing the
   *       chain of views that reference this table. The list is ordered from outermost view first
   *       to innermost view last. For a single view reference, the list contains one element.
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
