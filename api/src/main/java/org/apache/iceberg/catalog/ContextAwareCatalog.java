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
import org.apache.iceberg.view.View;

/** A Catalog API extension for context-aware table and view loading. */
public interface ContextAwareCatalog {

  /**
   * Context key whose value is a {@link List} of {@link TableIdentifier} representing the chain of
   * views that reference a table or view. The list is ordered from outermost to innermost view.
   */
  String VIEW_IDENTIFIER_KEY = "view.referenced-by";

  /**
   * Load a table with additional loading context.
   *
   * @param identifier a table identifier
   * @param loadingContext additional context as key-value pairs
   * @return instance of {@link Table} referred by the identifier
   * @throws UnsupportedOperationException if context-aware loading is not supported
   */
  default Table loadTable(TableIdentifier identifier, Map<String, Object> loadingContext) {
    throw new UnsupportedOperationException(
        "Context-aware table loading is not supported by this catalog");
  }

  /**
   * Load a view with additional loading context.
   *
   * @param identifier a view identifier
   * @param loadingContext additional context as key-value pairs
   * @return instance of {@link View} referred by the identifier
   * @throws UnsupportedOperationException if context-aware loading is not supported
   */
  default View loadView(TableIdentifier identifier, Map<String, Object> loadingContext) {
    throw new UnsupportedOperationException(
        "Context-aware view loading is not supported by this catalog");
  }
}
